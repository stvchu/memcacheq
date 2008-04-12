/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  Copyright 2008 Steve Chu.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Steve Chu <stvchu@gmail.com>
 *
 *  $Id: bdb.c 2008-01-22 17:27:13Z steve $
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

static pthread_t chk_ptid;
static pthread_t dld_ptid;

void bdb_settings_init(void)
{
    bdb_settings.db_file = DBFILE;
    bdb_settings.env_home = DBHOME;
    bdb_settings.cache_size = 64 * 1024 * 1024; /* default is 64MB */ 
    bdb_settings.page_size = 4096;  /* default is 4K */
    bdb_settings.txn_lg_bsize = 32 * 1024; /* default is 32KB */ 
    bdb_settings.txn_nosync = 0; /* default DB_TXN_NOSYNC is off */
    bdb_settings.dldetect_val = 100 * 1000; /* default is 100 millisecond */
    bdb_settings.chkpoint_val = 60; /* default is 60 second */
    bdb_settings.db_flags = DB_CREATE | DB_AUTO_COMMIT;
    bdb_settings.env_flags = DB_CREATE
                          | DB_INIT_LOCK 
                          | DB_THREAD 
                          | DB_INIT_MPOOL 
                          | DB_INIT_LOG 
                          | DB_INIT_TXN
                          | DB_RECOVER;
                              
    bdb_settings.is_replicated = 0;
    bdb_settings.rep_localhost = "127.0.0.1"; /* local host in replication */
    bdb_settings.rep_localport = 31311;  /* local port in replication */
    bdb_settings.rep_remotehost = NULL; /* local host in replication */
    bdb_settings.rep_remoteport = 0;  /* local port in replication */
    bdb_settings.rep_is_master = -1; /* 1 on YES, 0 on NO, -1 on UNKNOWN, for two sites replication */
    bdb_settings.rep_master_eid = DB_EID_INVALID;
    bdb_settings.rep_start_policy = DB_REP_ELECTION;
    bdb_settings.rep_priority = 100;
    bdb_settings.rep_ack_timeout = 20000;
    bdb_settings.rep_ack_policy = DB_REPMGR_ACKS_ONE_PEER;
    bdb_settings.rep_bulk = 1;
    bdb_settings.rep_req_min = 2;
    bdb_settings.rep_req_max = 8;
    bdb_settings.q_extentsize = 262144;
}

void bdb_env_init(void){
    int ret;
    /* db env init */
    if ((ret = db_env_create(&env, 0)) != 0) {
        fprintf(stderr, "db_env_create: %s\n", db_strerror(ret));
        exit(EXIT_FAILURE);
    }

    /* set err&msg display */
    env->set_errfile(env, stderr);
    env->set_errpfx(env, "Memcacheq");
    env->set_msgfile(env, stderr);

    /* set MPOOL size */
    env->set_cachesize(env, 0, bdb_settings.cache_size, 0);

    /* set DB_TXN_NOSYNC flag */
    if (bdb_settings.txn_nosync){
        env->set_flags(env, DB_TXN_NOSYNC, 1);
    }

    /* set locking */
    env->set_lk_max_lockers(env, 20000);
    env->set_lk_max_locks(env, 20000);
    env->set_lk_max_objects(env, 20000);

    /* set transaction log buffer */
    env->set_lg_bsize(env, bdb_settings.txn_lg_bsize);
    
    /* if no home dir existed, we create it */
    if (0 != access(bdb_settings.env_home, F_OK)) {
        if (0 != mkdir(bdb_settings.env_home, 0750)) {
            fprintf(stderr, "mkdir env_home error:[%s]\n", bdb_settings.env_home);
            exit(EXIT_FAILURE);
        }
    }
    
    if(bdb_settings.is_replicated) {
        bdb_settings.env_flags |= DB_INIT_REP;
        if ((ret = env->set_verbose(env, DB_VERB_REPLICATION, 1)) != 0) {
            fprintf(stderr, "env->set_verbose[DB_VERB_REPLICATION]: %s\n",
                    db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        env->set_event_notify(env, bdb_event_callback);
        env->repmgr_set_ack_policy(env, bdb_settings.rep_ack_policy);
        env->rep_set_timeout(env, DB_REP_ACK_TIMEOUT, bdb_settings.rep_ack_timeout);
        env->rep_set_timeout(env, DB_REP_CHECKPOINT_DELAY, 0);
        env->rep_set_config(env, DB_REP_CONF_BULK, bdb_settings.rep_bulk);
        env->rep_set_priority(env, bdb_settings.rep_priority);
        env->set_rep_request(env, bdb_settings.rep_req_min, bdb_settings.rep_req_max);

        if ((ret = env->repmgr_set_local_site(env, bdb_settings.rep_localhost, bdb_settings.rep_localport, 0)) != 0) {
            fprintf(stderr, "repmgr_set_local_site[%s:%d]: %s\n", 
                    bdb_settings.rep_localhost, bdb_settings.rep_localport, db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        if(NULL != bdb_settings.rep_remotehost) {
            if ((ret = env->repmgr_add_remote_site(env, bdb_settings.rep_remotehost, bdb_settings.rep_remoteport, NULL, 0)) != 0) {
                fprintf(stderr, "repmgr_add_remote_site[%s:%d]: %s\n", 
                        bdb_settings.rep_remotehost, bdb_settings.rep_remoteport, db_strerror(ret));
                exit(EXIT_FAILURE);
            }
        }
        if ((ret = env->rep_set_nsites(env, 2)) != 0) {
            fprintf(stderr, "rep_set_nsites: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
    }

    if ((ret = env->open(env, bdb_settings.env_home, bdb_settings.env_flags, 0)) != 0) {
        fprintf(stderr, "db_env_open: %s\n", db_strerror(ret));
        exit(EXIT_FAILURE);
    }


    if(bdb_settings.is_replicated) {
        /* repmgr_start must run after daemon !!!*/
        if ((ret = env->repmgr_start(env, 3, bdb_settings.rep_start_policy)) != 0) {
            fprintf(stderr, "env->repmgr_start: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        /* sleep 5 second for electing */
        if (bdb_settings.rep_start_policy == DB_REP_ELECTION) {
            sleep(5);
        }
    }
}


void bdb_db_open(void){
    int ret;
    int db_opened = 0;
    /* for replicas to get a full master copy, then open db */
    while(0 == db_opened) {
        if (0 == bdb_settings.rep_is_master) {
           bdb_settings.db_flags = DB_AUTO_COMMIT;
        }else if (1 == bdb_settings.rep_is_master) {
            bdb_settings.db_flags = DB_CREATE | DB_AUTO_COMMIT;
        }else{
            /* do nothing */
        }

        bdb_db_close();

        if ((ret = db_create(&dbp, env, 0)) != 0) {
            fprintf(stderr, "db_create: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        /* set page size */
        if ((ret = dbp->set_pagesize(dbp, bdb_settings.page_size)) != 0) {
            fprintf(stderr, "dbp->set_pagesize: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }

        /* set record len */
        if ((ret = dbp->set_re_len(dbp, settings.item_buf_size)) != 0) {
            fprintf(stderr, "dbp->set_re_len: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }

        /* set how many pages in a single Queue database file */
        if ((ret = dbp->set_q_extentsize(dbp, bdb_settings.q_extentsize)) != 0) {
            fprintf(stderr, "dbp->set_q_extentsize: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }

        if ((ret = dbp->open(dbp, NULL, bdb_settings.db_file, NULL, DB_QUEUE, bdb_settings.db_flags, 0664)) != 0) {
            if ((ret == ENOENT) || (ret == DB_LOCK_DEADLOCK) || (ret = DB_REP_LOCKOUT)) {
                fprintf(stderr, "No [%s] database yet available.\n", bdb_settings.db_file);
                fprintf(stderr, "db_ismaster: [%d], we are syncing with master..\n", bdb_settings.rep_is_master);
                db_opened = 0;
                sleep(3);
                continue;
            }
            fprintf(stderr, "db_open: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }else{
            db_opened = 1;
        }
    }

}

void start_chkpoint_thread(void){
    if (bdb_settings.chkpoint_val > 0){
        /* Start a checkpoint thread. */
        if ((errno = pthread_create(
            &chk_ptid, NULL, bdb_chkpoint_thread, (void *)env)) != 0) {
            fprintf(stderr,
                "failed spawning checkpoint thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

void start_dl_detect_thread(void){
    if (bdb_settings.dldetect_val > 0){
        /* Start a deadlock detecting thread. */
        if ((errno = pthread_create(
            &dld_ptid, NULL, bdb_dl_detect_thread, (void *)env)) != 0) {
            fprintf(stderr,
                "failed spawning deadlock thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

void *bdb_chkpoint_thread(void *arg)
{
    DB_ENV *dbenv;
    int ret;
    dbenv = arg;
    dbenv->errx(dbenv, "checkpoint thread created: %lu", (u_long)pthread_self());
    for (;; sleep(bdb_settings.chkpoint_val)) {
        if ((ret = dbenv->txn_checkpoint(dbenv, 0, 0, 0)) != 0) {
            dbenv->err(dbenv, ret, "checkpoint thread");
        }
    }
    return (NULL);
}

void *bdb_dl_detect_thread(void *arg)
{
    DB_ENV *dbenv;
    struct timeval t;
    dbenv = arg;
    dbenv->errx(dbenv, "deadlock detecting thread created: %lu", (u_long)pthread_self());
    while (!daemon_quit) {
        t.tv_sec = 0;
        t.tv_usec = bdb_settings.dldetect_val;
        (void)dbenv->lock_detect(dbenv, 0, DB_LOCK_YOUNGEST, NULL);
        /* select is a more accurate sleep timer */
        (void)select(0, NULL, NULL, NULL, &t);
    }
    return (NULL);
}

void bdb_event_callback(DB_ENV *env, u_int32_t which, void *info)
{
    switch (which) {
    case DB_EVENT_REP_CLIENT:
        fprintf(stderr, "Msg: DB_EVENT_REP_CLIENT %s:%d\n", 
                bdb_settings.rep_localhost, bdb_settings.rep_localport);
        bdb_settings.rep_is_master = 0;
        break;
    case DB_EVENT_REP_MASTER:
        fprintf(stderr, "Msg: DB_EVENT_REP_MASTER %s:%d\n", 
                bdb_settings.rep_localhost, bdb_settings.rep_localport);
        bdb_settings.rep_is_master = 1;
        bdb_settings.rep_master_eid = BDB_EID_SELF;
        break;
    case DB_EVENT_REP_ELECTED:
        fprintf(stderr, "Msg: DB_EVENT_REP_ELECTED %s:%d\n", 
                bdb_settings.rep_localhost, bdb_settings.rep_localport);
        break;
    case DB_EVENT_REP_PERM_FAILED:
        fprintf(stderr, "Msg: insufficient acks\n");
        break;
    case DB_EVENT_REP_STARTUPDONE: /* FALLTHROUGH */
        break;
    case DB_EVENT_REP_NEWMASTER:
        fprintf(stderr, "Msg: DB_EVENT_REP_NEWMASTER %s:%d\n", 
                bdb_settings.rep_localhost, bdb_settings.rep_localport);
        bdb_settings.rep_master_eid = *(int*)info;
        break;
    default:
        env->errx(env, "ignoring event %d", which);
    }
}

/* for atexit cleanup */
void bdb_db_close(void){
    int ret = 0;
    if (dbp != NULL) {
        ret = dbp->close(dbp, 0);
        if (0 != ret){
            fprintf(stderr, "dbp->close: %s\n", db_strerror(ret));
        }else{
            dbp = NULL;
            fprintf(stderr, "dbp->close: OK\n");
        }
    }
}

/* for atexit cleanup */
void bdb_env_close(void){
    int ret = 0;
    if (env != NULL) {
        ret = env->close(env, 0);
        if (0 != ret){
            fprintf(stderr, "env->close: %s\n", db_strerror(ret));
        }else{
            env = NULL;
            fprintf(stderr, "env->close: OK\n");
        }
    }
}

/* for atexit cleanup */
void bdb_chkpoint(void)
{
    int ret = 0;
    ret = env->txn_checkpoint(env, 0, 0, 0); 
    if (0 != ret){
        fprintf(stderr, "env->txn_checkpoint: %s\n", db_strerror(ret));
    }else{
        fprintf(stderr, "env->txn_checkpoint: OK\n");
    }
}
