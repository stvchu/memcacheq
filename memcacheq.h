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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <event.h>
#include <netdb.h>
#include <db.h>
#include "hashtable.h"
#include "hash.h"

#define DATA_BUFFER_SIZE 2048
#define UDP_READ_BUFFER_SIZE 65536
#define UDP_MAX_PAYLOAD_SIZE 1400
#define UDP_HEADER_SIZE 8
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)
/* I'm told the max legnth of a 64-bit num converted to string is 20 bytes.
 * Plus a few for spaces, \r\n, \0 */
#define SUFFIX_SIZE 24

/** Initial size of list of items being returned by "get". */
#define ITEM_LIST_INITIAL 200

/** Initial size of the sendmsg() scatter/gather array. */
#define IOV_LIST_INITIAL 400

/** Initial number of sendmsg() argument structures to allocate. */
#define MSG_LIST_INITIAL 10

/** High water marks for buffer shrinking */
#define READ_BUFFER_HIGHWAT 8192
#define ITEM_LIST_HIGHWAT 400
#define IOV_LIST_HIGHWAT 600
#define MSG_LIST_HIGHWAT 100

/* Get a consistent bool type */
#if HAVE_STDBOOL_H
# include <stdbool.h>
#else
  typedef enum {false = 0, true = 1} bool;
#endif

#if HAVE_STDINT_H
# include <stdint.h>
#else
 typedef unsigned char             uint8_t;
#endif

/* unistd.h is here */
#if HAVE_UNISTD_H
# include <unistd.h>
#endif

struct stats {
    unsigned int  curr_conns;
    unsigned int  total_conns;
    unsigned int  conn_structs;
    uint64_t      get_cmds;
    uint64_t      get_hits;
    uint64_t      set_cmds;
    uint64_t      set_hits;
    time_t        started;          /* when the process was started */
    uint64_t      bytes_read;
    uint64_t      bytes_written;
};

#define MAX_VERBOSITY_LEVEL 2

struct settings {
    int maxconns;
    int port;
    int udpport;
    char *inter;
    int verbose;
    char *socketpath;   /* path to unix socket if using local socket */
    int access;  /* access mask (a la chmod) for unix domain socket */
    int num_threads;        /* number of libevent threads to run */
};

extern struct stats stats;
extern struct settings settings;

typedef struct _stritem {
    int             nbytes;     /* size of data */
    uint8_t         nsuffix;    /* length of flags-and-length string */
    uint8_t         nkey;       /* key length, w/terminating null and padding */
    void * end[];
    /* then null-terminated key */
    /* then " flags length\r\n" (no terminating null) */
    /* then data with terminating \r\n (no terminating null; it's binary!) */
} item;

#define ITEM_key(item) ((char*)&((item)->end[0]))

/* warning: don't use these macros with a function, as it evals its arg twice */
#define ITEM_suffix(item) ((char*) &((item)->end[0]) + (item)->nkey + 1)
#define ITEM_data(item) ((char*) &((item)->end[0]) + (item)->nkey + 1 + (item)->nsuffix)
#define ITEM_ntotal(item) (sizeof(struct _stritem) + (item)->nkey + 1 + (item)->nsuffix + (item)->nbytes)

enum conn_states {
    conn_listening,  /** the socket which listens for connections */
    conn_read,       /** reading in a command line */
    conn_write,      /** writing out a simple response */
    conn_nread,      /** reading in a fixed number of bytes */
    conn_swallow,    /** swallowing unnecessary bytes w/o storing */
    conn_closing,    /** closing this connection */
    conn_mwrite,     /** writing out many items sequentially */
};

typedef struct conn conn;
struct conn {
    int    sfd;
    int    state;
    struct event event;
    short  ev_flags;
    short  which;   /** which events were just triggered */

    char   *rbuf;   /** buffer to read commands into */
    char   *rcurr;  /** but if we parsed some already, this is where we stopped */
    int    rsize;   /** total allocated size of rbuf */
    int    rbytes;  /** how much data, starting from rcur, do we have unparsed */

    char   *wbuf;
    char   *wcurr;
    int    wsize;
    int    wbytes;
    int    write_and_go; /** which state to go into after finishing current write */
    void   *write_and_free; /** free this memory after finishing writing */

    char   *ritem;  /** when we read in an item's value, it goes here */
    int    rlbytes;

    /* data for the nread state */

    /**
     * item is used to hold an item structure created after reading the command
     * line of set/add/replace commands, but before we finished reading the actual
     * data. The data is read into ITEM_data(item) to avoid extra copying.
     */

    void   *item;     /* for commands set/add/replace  */
    int    item_comm; /* which one is it: set/add/replace */

    /* data for the swallow state */
    int    sbytes;    /* how many bytes to swallow */

    /* data for the mwrite state */
    struct iovec *iov;
    int    iovsize;   /* number of elements allocated in iov[] */
    int    iovused;   /* number of elements used in iov[] */

    struct msghdr *msglist;
    int    msgsize;   /* number of elements allocated in msglist[] */
    int    msgused;   /* number of elements used in msglist[] */
    int    msgcurr;   /* element in msglist[] being transmitted now */
    int    msgbytes;  /* number of bytes in current msg */

    item   **ilist;   /* list of items to write out */
    int    isize;
    item   **icurr;
    int    ileft;

    /* data for UDP clients */
    bool   udp;       /* is this is a UDP "connection" */
    int    request_id; /* Incoming UDP request ID, if this is a UDP "connection" */
    struct sockaddr request_addr; /* Who sent the most recent request */
    socklen_t request_addr_size;
    unsigned char *hdrbuf; /* udp packet headers */
    int    hdrsize;   /* number of headers' worth of space is allocated */
    conn   *next;     /* Used for generating a list of conn structures */
};

/*
 * Functions
 */

/* item buffer management */
void item_init(void);
item *do_item_from_freelist(void);
int do_item_add_to_freelist(item *it);
item *item_alloc1(char *key, const size_t nkey, const int flags, const int nbytes);
item *item_alloc2(void);
int item_free(item *it);

/* conn management */
conn *do_conn_from_freelist();
bool do_conn_add_to_freelist(conn *c);
conn *conn_new(const int sfd, const int init_state, const int event_flags, const int read_buffer_size, const bool is_udp, struct event_base *base);

/* bdb */
#define DBHOME "/data1/memcacheq"

#define BDB_CLEANUP_DBT() \
    memset(&dbkey, 0, sizeof(dbkey)); \
    memset(&dbdata, 0, sizeof(dbdata))

struct bdb_settings {
    char *env_home;
    u_int32_t cache_size;
    u_int32_t txn_lg_bsize;
    u_int32_t page_size;
    int txn_nosync;
    int deadlock_detect_val;
    int checkpoint_val;
    int mempool_trickle_val;
    int mempool_trickle_percent;
    int qstats_dump_val;
    u_int32_t re_len;
    u_int32_t q_extentsize;
};

typedef struct _qstats {
  int64_t set_hits;
  int64_t get_hits;
} qstats_t;

typedef struct _queue {
  DB* dbp;
  int64_t set_hits;
  int64_t get_hits;
  int64_t old_set_hits;
  int64_t old_get_hits;
  pthread_mutex_t lock;
} queue_t;

extern struct bdb_settings bdb_settings;
extern DB_ENV *envp;

void  qlist_ht_init(void);
void  qlist_ht_close(void);
void  bdb_settings_init(void);
void  bdb_env_init(void);
void  bdb_env_close(void);
void  bdb_qlist_db_open(void);
void  bdb_qlist_db_close(void);
int   bdb_create_queue(char *queue_name);
int   bdb_delete_queue(char *queue_name);
int   bdb_set(char *key, item *it);
item* bdb_get(char *key);

void start_checkpoint_thread(void);
void start_mempool_trickle_thread(void);
void start_deadlock_detect_thread(void);
void start_qstats_dump_thread(void);
void bdb_chkpoint(void);

/*
 * In multithreaded mode, we wrap certain functions with lock management and
 * replace the logic of some other functions. All wrapped functions have
 * "mt_" and "do_" variants. In multithreaded mode, the plain version of a
 * function is #define-d to the "mt_" variant, which often just grabs a
 * lock and calls the "do_" function. In singlethreaded mode, the "do_"
 * function is called directly.
 *
 * Functions such as the libevent-related calls that need to do cross-thread
 * communication in multithreaded mode (rather than actually doing the work
 * in the current thread) are called via "dispatch_" frontends, which are
 * also #define-d to directly call the underlying code in singlethreaded mode.
 */
#ifdef USE_THREADS

void thread_init(int nthreads, struct event_base *main_base);
int  dispatch_event_add(int thread, conn *c);
void dispatch_conn_new(int sfd, int init_state, int event_flags, int read_buffer_size, int is_udp);

/* Lock wrappers for cache functions that are called from main loop. */
conn *mt_conn_from_freelist(void);
bool  mt_conn_add_to_freelist(conn *c);
int   mt_is_listen_thread(void);
item *mt_item_from_freelist(void);
int mt_item_add_to_freelist(item *it);
void  mt_stats_lock(void);
void  mt_stats_unlock(void);

# define conn_from_freelist()        mt_conn_from_freelist()
# define conn_add_to_freelist(x)     mt_conn_add_to_freelist(x)
# define is_listen_thread()          mt_is_listen_thread()
# define item_from_freelist()        mt_item_from_freelist()
# define item_add_to_freelist(x)     mt_item_add_to_freelist(x)

# define STATS_LOCK()                mt_stats_lock()
# define STATS_UNLOCK()              mt_stats_unlock()

#else /* !USE_THREADS */

# define conn_from_freelist()         do_conn_from_freelist()
# define conn_add_to_freelist(x)      do_conn_add_to_freelist(x)
# define dispatch_conn_new(x,y,z,a,b) conn_new(x,y,z,a,b,main_base)
# define dispatch_event_add(t,c)      event_add(&(c)->event, 0)
# define is_listen_thread()           1
# define item_from_freelist()         do_item_from_freelist()
# define item_add_to_freelist(x)      do_item_add_to_freelist(x)
# define thread_init(x,y)             0

# define STATS_LOCK()                /**/
# define STATS_UNLOCK()              /**/

#endif /* !USE_THREADS */

extern int daemon_quit;
