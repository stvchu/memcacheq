/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  MemcacheQ - Simple Queue Service over Memcache
 *
 *      http://memcacheq.googlecode.com
 *
 *  The source code of MemcacheQ is most based on MemcachDB:
 *
 *      http://memcachedb.googlecode.com
 *
 *  Copyright 2008 Steve Chu.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Steve Chu <stvchu@gmail.com>
 *
 */

#include "memcacheq.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>
#include <db.h>
#include <assert.h>

#define CHECK_DB_RET(ret) \
  if (0!=ret) \
      goto dberr

static void open_exsited_queue_db(DB_TXN *txn, char *qn, qstats_t *qs);
static void dump_qstats(void);
static void *bdb_checkpoint_thread __P((void *));
static void *bdb_mempool_trickle_thread __P((void *));
static void *bdb_deadlock_detect_thread __P((void *));
static void *bdb_qstats_dump_thread __P((void *));
static void bdb_err_callback(const DB_ENV *dbenv, const char *errpfx, const char *msg);
static void bdb_msg_callback(const DB_ENV *dbenv, const char *msg);

static unsigned int
hashfromkey(void *ky)
{
    char *k = (char *)ky;
    unsigned int hash;
    hash = murmurhash2(k, strlen(k), 0);
    return hash;
}

static int
equalkeys(void *k1, void *k2)
{
    return (0 == strcmp(k1,k2));
}

static struct hashtable *qlist_htp;
static pthread_rwlock_t qlist_ht_lock;
static DB *qlist_dbp = NULL;

void qlist_ht_init(void) {
    pthread_rwlock_init(&qlist_ht_lock, NULL);
    qlist_htp = create_hashtable(64, hashfromkey, equalkeys);
    if (NULL == qlist_htp) {
        fprintf(stderr, "create_hashtable fail\n");
        exit(EXIT_FAILURE);
    }
}

void qlist_ht_close(void) {
    hashtable_destroy(qlist_htp, 1);
    pthread_rwlock_destroy(&qlist_ht_lock);
}

void bdb_settings_init(void){
    bdb_settings.env_home = DBHOME;
    bdb_settings.cache_size = 64 * 1024 * 1024; /* default is 64MB */ 
    bdb_settings.txn_lg_bsize = 32 * 1024; /* default is 32KB */ 
    
    bdb_settings.re_len = 1024;
    bdb_settings.q_extentsize = 16 * 1024; // 64MB extent file each
    
    bdb_settings.page_size = 4096;  /* default is 4K */
    bdb_settings.txn_nosync = 0; /* default DB_TXN_NOSYNC is off */
    bdb_settings.deadlock_detect_val = 100 * 1000; /* default is 100 millisecond */
    bdb_settings.checkpoint_val = 60 * 5; /* seconds */
    bdb_settings.mempool_trickle_val = 30; /* seconds */
    bdb_settings.mempool_trickle_percent = 60; 
    bdb_settings.qstats_dump_val = 30; /* seconds */                              
}

void bdb_env_init(void){
    int ret;
    u_int32_t env_flags = DB_CREATE
                        | DB_INIT_LOCK 
                        | DB_THREAD 
                        | DB_INIT_MPOOL 
                        | DB_INIT_LOG 
                        | DB_INIT_TXN
                        | DB_RECOVER;
    /* db env init */
    if ((ret = db_env_create(&envp, 0)) != 0) {
        fprintf(stderr, "db_env_create: %s\n", db_strerror(ret));
        exit(EXIT_FAILURE);
    }

    /* set err&msg display */
    envp->set_errpfx(envp, PACKAGE);
	envp->set_errcall(envp, bdb_err_callback);
  	envp->set_msgcall(envp, bdb_msg_callback);
  	
    /* set MPOOL size */
    envp->set_cachesize(envp, 0, bdb_settings.cache_size, 0);

    /* set DB_TXN_NOSYNC flag */
    if (bdb_settings.txn_nosync){
        envp->set_flags(envp, DB_TXN_NOSYNC, 1);
    }

    /* set locking */
    envp->set_lk_max_lockers(envp, 40000);
    envp->set_lk_max_locks(envp, 40000);
    envp->set_lk_max_objects(envp, 40000);
    
    /* at least max active transactions */
  	envp->set_tx_max(envp, 40000);
  	
    /* set transaction log buffer */
    envp->set_lg_bsize(envp, bdb_settings.txn_lg_bsize);
    
    /* if no home dir existed, we create it */
    if (0 != access(bdb_settings.env_home, F_OK)) {
        if (0 != mkdir(bdb_settings.env_home, 0750)) {
            fprintf(stderr, "mkdir env_home error:[%s]\n", bdb_settings.env_home);
            exit(EXIT_FAILURE);
        }
    }
    
    if ((ret = envp->open(envp, bdb_settings.env_home, env_flags, 0)) != 0) {
        fprintf(stderr, "envp->open: %s\n", db_strerror(ret));
        exit(EXIT_FAILURE);
    }
}

/* for atexit cleanup */
void bdb_env_close(void){
    int ret = 0;
    if (envp != NULL) {
        ret = envp->close(envp, 0);
        if (0 != ret){
            fprintf(stderr, "envp->close: %s\n", db_strerror(ret));
        }else{
            envp = NULL;
            fprintf(stderr, "envp->close: OK\n");
        }
    }
}

void bdb_qlist_db_open(void){
    int ret;
    DBC *cursorp = NULL;
    DB_TXN *txnp = NULL;
        
    ret = envp->txn_begin(envp, NULL, &txnp, 0);
    CHECK_DB_RET(ret);
    ret = db_create(&qlist_dbp, envp, 0);
    CHECK_DB_RET(ret);
    ret = qlist_dbp->open(qlist_dbp, txnp, "queue.list", NULL, DB_BTREE, DB_CREATE, 0664);
    CHECK_DB_RET(ret);
    ret = qlist_dbp->cursor(qlist_dbp, txnp, &cursorp, 0); 
    CHECK_DB_RET(ret);
    
    /* Initialize our DBTs. */
    DBT dbkey, dbdata;
    char qname[512];
    qstats_t qs;
    BDB_CLEANUP_DBT();
    memset(qname, 0, 512);
    memset(&qs, 0, sizeof(qs));
    dbkey.data = (void *)qname;
    dbkey.ulen = 512;
    dbkey.flags = DB_DBT_USERMEM;
    dbdata.data = (void *)&qs;
    dbdata.ulen = sizeof(qs);
    dbdata.flags = DB_DBT_USERMEM;
    
    while ((ret = cursorp->get(cursorp, &dbkey, &dbdata, DB_NEXT)) == 0) {
        open_exsited_queue_db(txnp, qname, &qs);
    }
    if (ret != DB_NOTFOUND) {
        goto dberr;
    }
    
    ret = cursorp->close(cursorp);
    CHECK_DB_RET(ret);
    
    ret = txnp->commit(txnp, 0);
    CHECK_DB_RET(ret);
    return;
        
dberr:
    if (cursorp != NULL){
        cursorp->close(cursorp);
    }
    if (txnp != NULL){
        txnp->abort(txnp);
    }
    fprintf(stderr, "bdb_qlist_db_open: %s\n", db_strerror(ret));
    exit(EXIT_FAILURE);
}

static void open_exsited_queue_db(DB_TXN *txnp, char *queue_name, qstats_t *qsp){
    int ret;
    
    char *k = strdup(queue_name);
    assert(k != NULL);
    queue_t *q = (queue_t *)calloc(1, sizeof(queue_t));
    assert(q != NULL);

    /* init hash_key and hash_value */
    q->dbp = NULL;
    q->set_hits = q->old_set_hits = qsp->set_hits;
    q->get_hits = q->old_get_hits = qsp->get_hits;
    pthread_mutex_init(&(q->lock), NULL);
    
    ret = db_create(&(q->dbp), envp, 0);
    CHECK_DB_RET(ret);    
    ret = q->dbp->open(q->dbp, txnp, queue_name, NULL, DB_QUEUE, DB_CREATE, 0664);         
    CHECK_DB_RET(ret);
    
    int result = hashtable_insert(qlist_htp, (void *)k, (void *)q);
    assert(result != 0);
    return;
    
dberr:
    fprintf(stderr, "open_exsited_queue_db: %s\n", db_strerror(ret));
    exit(EXIT_FAILURE);
}

/* for atexit cleanup */
void bdb_qlist_db_close(void){
    dump_qstats();
    
    struct hashtable_itr *itr = NULL;
    queue_t *q;
    itr = hashtable_iterator(qlist_htp);
    assert(itr != NULL);
    if (hashtable_count(qlist_htp) > 0)
    {
        do {
            q = hashtable_iterator_value(itr);
            q->dbp->close(q->dbp, 0);
            pthread_mutex_destroy(&(q->lock));
        } while (hashtable_iterator_advance(itr));
    }
    free(itr);    
    qlist_dbp->close(qlist_dbp, 0);
    fprintf(stderr, "qlist_dbp->close: OK\n");
}

int bdb_create_queue(char *queue_name) {
    pthread_rwlock_wrlock(&qlist_ht_lock);

    char *k = strdup(queue_name);
    assert(k != NULL);
    queue_t *q = (queue_t *)calloc(1, sizeof(queue_t));
    assert(q != NULL);

    q->dbp = NULL;
    q->set_hits = q->old_set_hits = 0;
    q->get_hits = q->old_get_hits = 0;
    pthread_mutex_init(&(q->lock), NULL);
    
    int ret;
    DB_TXN *txnp = NULL;
    ret = db_create(&(q->dbp), envp, 0);
    CHECK_DB_RET(ret);

    if (bdb_settings.q_extentsize != 0){
        ret = q->dbp->set_q_extentsize(q->dbp, bdb_settings.q_extentsize);
        CHECK_DB_RET(ret);
    }
    ret = q->dbp->set_re_len(q->dbp, bdb_settings.re_len);
    CHECK_DB_RET(ret);
    ret = q->dbp->set_pagesize(q->dbp, bdb_settings.page_size);
    CHECK_DB_RET(ret);

    ret = envp->txn_begin(envp, NULL, &txnp, 0);
    ret = q->dbp->open(q->dbp, txnp, queue_name, NULL, DB_QUEUE, DB_CREATE, 0664); 
    CHECK_DB_RET(ret);
    
    DBT dbkey,dbdata;
    qstats_t qs;
    memset(&qs, 0, sizeof(qs));
    BDB_CLEANUP_DBT();
    dbkey.data = (void *)queue_name;
    dbkey.size = strlen(queue_name)+1;
    dbdata.data = (void *)&qs;
    dbdata.size = sizeof(qstats_t);
    CHECK_DB_RET(ret);
    ret = qlist_dbp->put(qlist_dbp, txnp, &dbkey, &dbdata, 0);
    CHECK_DB_RET(ret);
    ret = txnp->commit(txnp, 0);
    CHECK_DB_RET(ret);
    int result = hashtable_insert(qlist_htp, (void *)k, (void *)q);
    assert(result != 0);
    pthread_rwlock_unlock(&qlist_ht_lock);
    return 0;
dberr:
    if (txnp != NULL){
        txnp->abort(txnp);
    }
    fprintf(stderr, "bdb_create_queue: %s %s\n", queue_name, db_strerror(ret));
    pthread_rwlock_unlock(&qlist_ht_lock);
    return -1;
}

int bdb_delete_queue(char *queue_name){
    pthread_rwlock_wrlock(&qlist_ht_lock);
    queue_t *q = hashtable_search(qlist_htp, (void *)queue_name);
    /* NOT FOUND */
    if (q == NULL) {
        pthread_rwlock_unlock(&qlist_ht_lock);
        return 1;
    } 
    /* Found, just close and remove it. */
    q->dbp->close(q->dbp, 0);
    pthread_mutex_destroy(&(q->lock));
    q = hashtable_remove(qlist_htp, (void *)queue_name);
    assert(NULL != q);
    free(q);
    
    int ret;
    DB_TXN *txnp = NULL;
    ret = envp->txn_begin(envp, NULL, &txnp, 0);
    CHECK_DB_RET(ret);
    ret = envp->dbremove(envp, txnp, queue_name, NULL, 0);
    CHECK_DB_RET(ret);
    ret = txnp->commit(txnp, 0);
    CHECK_DB_RET(ret);
    pthread_rwlock_unlock(&qlist_ht_lock);
    return 0;
dberr:
    if (txnp != NULL){
        txnp->abort(txnp);
    }
    fprintf(stderr, "bdb_delete_queue: %s %s\n", queue_name, db_strerror(ret));
    pthread_rwlock_unlock(&qlist_ht_lock);
    return -1;
}

static void dump_qstats(void) {
    struct hashtable_itr *itr = NULL;

    /* qstats hashtable */
    char *kk;
    qstats_t *s;
    struct hashtable *qstats_htp = NULL;    
    qstats_htp = create_hashtable(64, hashfromkey, equalkeys);
    assert(qstats_htp != NULL);
    
    /* cp hashtable to stats table, this is very fast in-memory */
    pthread_rwlock_rdlock(&qlist_ht_lock);
    char *k;
    queue_t *q;
    itr = hashtable_iterator(qlist_htp);
    assert(itr != NULL);
    if (hashtable_count(qlist_htp) > 0)
    {
        do {
            k = hashtable_iterator_key(itr);
            q = hashtable_iterator_value(itr);
            pthread_mutex_lock(&(q->lock));
            if (q->old_set_hits == q->set_hits &&
                q->old_get_hits == q->get_hits) {
                pthread_mutex_unlock(&(q->lock));
                continue;
            }
            q->old_set_hits = q->set_hits;
            q->old_get_hits = q->get_hits;
            pthread_mutex_unlock(&(q->lock));
            kk = strdup(k);
            assert(kk);
            s = calloc(1, sizeof(qstats_t));
            assert(s);
            s->set_hits = q->old_set_hits;
            s->get_hits = q->old_get_hits;
            hashtable_insert(qstats_htp, (void *)kk, (void *)s);
        } while (hashtable_iterator_advance(itr));
    }
    free(itr);
    itr = NULL;
    pthread_rwlock_unlock(&qlist_ht_lock);
    
    /* dump stats hashtable to db */
    DBT dbkey, dbdata;
    int ret;
    DB_TXN *txnp = NULL;
    ret = envp->txn_begin(envp, NULL, &txnp, 0);
    CHECK_DB_RET(ret);
    itr = hashtable_iterator(qstats_htp);
    assert(itr != NULL);
    if (hashtable_count(qstats_htp) > 0)
    {
        do {
            kk = hashtable_iterator_key(itr);
            s = hashtable_iterator_value(itr);
            dbkey.data = kk;
            dbkey.size = strlen(kk) + 1;
            dbdata.data = s;
            dbdata.size = sizeof(qstats_t);
            ret = qlist_dbp->put(qlist_dbp, txnp, &dbkey, &dbdata, 0);
            CHECK_DB_RET(ret);
            fprintf(stderr, "dump stats[%s], set_hits: %lld, get_hits: %lld \n",
                    kk, s->set_hits, s->get_hits);
        } while (hashtable_iterator_advance(itr));
    }
    free(itr);
    itr = NULL;
    ret = txnp->commit(txnp, 0);
    CHECK_DB_RET(ret);

    hashtable_destroy(qstats_htp, 1);
    qstats_htp = NULL;
    return;
dberr:
    if (txnp != NULL){
        txnp->abort(txnp);
    }
    if (settings.verbose > 1) {
        fprintf(stderr, "dump_qstats: %s\n", db_strerror(ret));
    }
}


/* if return item is not NULL, free by caller */
item *bdb_get(char *key){
    pthread_rwlock_rdlock(&qlist_ht_lock);
    item *it = NULL;
    DB_TXN *txnp = NULL;
    int ret;
    
    queue_t *q = (queue_t *)hashtable_search(qlist_htp, (void *)key);
    /* queue not exsited */
    if (q == NULL) {
        pthread_rwlock_unlock(&qlist_ht_lock);            
        return NULL;
    } else {
        DBT dbkey, dbdata;
        db_recno_t recno;

        /* first, alloc a fixed size */
        it = item_alloc2();
        if (it == 0) {
            pthread_rwlock_unlock(&qlist_ht_lock);            
            return NULL;
        }

        BDB_CLEANUP_DBT();
        dbkey.data = &recno;
        dbkey.ulen = sizeof(recno);
        dbkey.flags = DB_DBT_USERMEM;
        dbdata.ulen = bdb_settings.re_len;
        dbdata.data = it;
        dbdata.flags = DB_DBT_USERMEM;

        ret = envp->txn_begin(envp, NULL, &txnp, 0);
        CHECK_DB_RET(ret);
        ret = q->dbp->get(q->dbp, txnp, &dbkey, &dbdata, DB_CONSUME);
        CHECK_DB_RET(ret);
        ret = txnp->commit(txnp, 0);
        CHECK_DB_RET(ret);
    }
    pthread_rwlock_unlock(&qlist_ht_lock);    
    return it;
dberr:
    item_free(it);
    it = NULL;
    if (txnp != NULL){
        txnp->abort(txnp);
    }
    if (settings.verbose > 1) {
        fprintf(stderr, "bdb_get: %s\n", db_strerror(ret));
    }
    pthread_rwlock_unlock(&qlist_ht_lock);
    return NULL;
}

/* 0 for Success
   -1 for SERVER_ERROR
*/
int bdb_set(char *key, item *it){
    pthread_rwlock_rdlock(&qlist_ht_lock);
    queue_t *q = (queue_t *)hashtable_search(qlist_htp, (void *)key);
    DB_TXN *txnp = NULL;
    int ret;

    if (NULL == q) {
        pthread_rwlock_unlock(&qlist_ht_lock);
        ret = bdb_create_queue(key);
        if (0 != ret){
            return -1;
        }
        /* search again */
        pthread_rwlock_rdlock(&qlist_ht_lock);
        q = (queue_t *)hashtable_search(qlist_htp, (void *)key);        
    }
    
    if (NULL != q) {
        db_recno_t recno;
        DBT dbkey, dbdata;    
        BDB_CLEANUP_DBT();
        dbkey.data = &recno;
        dbkey.ulen = sizeof(recno);
        dbkey.flags = DB_DBT_USERMEM;
        dbdata.data = it;
        dbdata.size = ITEM_ntotal(it);
        ret = envp->txn_begin(envp, NULL, &txnp, 0);
        CHECK_DB_RET(ret);
        ret = q->dbp->put(q->dbp, txnp, &dbkey, &dbdata, DB_APPEND);
        CHECK_DB_RET(ret);
        ret = txnp->commit(txnp, 0);
        CHECK_DB_RET(ret);
    }
    pthread_rwlock_unlock(&qlist_ht_lock);    
    return 0;
dberr:
    if (txnp != NULL){
        txnp->abort(txnp);
    }
    if (settings.verbose > 1) {
        fprintf(stderr, "bdb_set: %s\n", db_strerror(ret));
    }
    pthread_rwlock_unlock(&qlist_ht_lock);
    return -1;
}

void start_checkpoint_thread(void){
    pthread_t tid;
    if (bdb_settings.checkpoint_val > 0){
        /* Start a checkpoint thread. */
        if ((errno = pthread_create(
            &tid, NULL, bdb_checkpoint_thread, (void *)envp)) != 0) {
            fprintf(stderr,
                "failed spawning checkpoint thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

void start_mempool_trickle_thread(void){
    pthread_t tid;
    if (bdb_settings.mempool_trickle_val > 0){
        /* Start a memp_trickle thread. */
        if ((errno = pthread_create(
            &tid, NULL, bdb_mempool_trickle_thread, (void *)envp)) != 0) {
            fprintf(stderr,
                "failed spawning memp_trickle thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

void start_deadlock_detect_thread(void){
    pthread_t tid;
    if (bdb_settings.deadlock_detect_val > 0){
        /* Start a deadlock detecting thread. */
        if ((errno = pthread_create(
            &tid, NULL, bdb_deadlock_detect_thread, (void *)envp)) != 0) {
            fprintf(stderr,
                "failed spawning deadlock thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}
/* TODO: */
void start_qstats_dump_thread(void){
    pthread_t tid;
    if (bdb_settings.qstats_dump_val > 0){
        /* Start a queue stats dump thread. */
        if ((errno = pthread_create(
            &tid, NULL, bdb_qstats_dump_thread, (void *)envp)) != 0) {
            fprintf(stderr,
                "failed spawning qstats dump thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

static void *bdb_checkpoint_thread(void *arg){
    DB_ENV *dbenv;
    int ret;
    dbenv = arg;
    for (;; sleep(bdb_settings.checkpoint_val)) {
        if ((ret = dbenv->txn_checkpoint(dbenv, 0, 0, 0)) != 0) {
            dbenv->err(dbenv, ret, "checkpoint thread");
        }
        dbenv->errx(dbenv, "checkpoint thread: a txn_checkpoint is done");
    }
    return (NULL);
}

static void *bdb_mempool_trickle_thread(void *arg){
    DB_ENV *dbenv;
    int ret, nwrotep;
    dbenv = arg;
    for (;; sleep(bdb_settings.mempool_trickle_val)) {
        if ((ret = dbenv->memp_trickle(dbenv, bdb_settings.mempool_trickle_percent, &nwrotep)) != 0) {
            dbenv->err(dbenv, ret, "mempool_trickle thread");
        }
        dbenv->errx(dbenv, "mempool_trickle thread: writing %d dirty pages", nwrotep);
    }
    return (NULL);
}

static void *bdb_deadlock_detect_thread(void *arg){
    DB_ENV *dbenv;
    struct timeval t;
    dbenv = arg;
    while (!daemon_quit) {
        t.tv_sec = 0;
        t.tv_usec = bdb_settings.deadlock_detect_val;
        (void)dbenv->lock_detect(dbenv, 0, DB_LOCK_YOUNGEST, NULL);
        /* select is a more accurate sleep timer */
        (void)select(0, NULL, NULL, NULL, &t);
    }
    return (NULL);
}

static void *bdb_qstats_dump_thread(void *arg){
    DB_ENV *dbenv;
    int ret;
    dbenv = arg;
    for (;; sleep(bdb_settings.qstats_dump_val)) {
        dump_qstats();
        dbenv->errx(dbenv, "qstats dump thread: a qstats is dump.");
    }
    return (NULL);
}

static void bdb_err_callback(const DB_ENV *dbenv, const char *errpfx, const char *msg){
	time_t curr_time = time(NULL);
	char time_str[32];
	strftime(time_str, 32, "%c", localtime(&curr_time));
	fprintf(stderr, "[%s] [%s] \"%s\"\n", errpfx, time_str, msg);
}

static void bdb_msg_callback(const DB_ENV *dbenv, const char *msg){
	time_t curr_time = time(NULL);
	char time_str[32];
	strftime(time_str, 32, "%c", localtime(&curr_time));
	fprintf(stderr, "[%s] [%s] \"%s\"\n", PACKAGE, time_str, msg);
}

/* for atexit cleanup */
void bdb_chkpoint(void){
    int ret = 0;
    if (envp != NULL){
        ret = envp->txn_checkpoint(envp, 0, 0, 0); 
        if (0 != ret){
            fprintf(stderr, "envp->txn_checkpoint: %s\n", db_strerror(ret));
        }else{
            fprintf(stderr, "envp->txn_checkpoint: OK\n");
        }
    }
}


