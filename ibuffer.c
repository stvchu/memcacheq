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
 *  $Id: item.c 2008-01-23 22:33:13Z steve $
 */

/*
 * Free list management for item buffers.
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
#include <stdlib.h>

#define MAX_ITEM_FREELIST_LENGTH 5000
#define INIT_ITEM_FREELIST_LENGTH 500

static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes, char *suffix, uint8_t *nsuffix);
static item *item_from_freelist(size_t ntotal);

static item **freeitem;
static int freeitemtotal;
static int freeitemcurr;

/**
 * Generates the variable-sized part of the header for an object.
 *
 * key     - The key
 * nkey    - The length of the key
 * flags   - key flags
 * nbytes  - Number of bytes to hold value and addition CRLF terminator
 * suffix  - Buffer for the "VALUE" line suffix (flags, size).
 * nsuffix - The length of the suffix is stored here.
 *
 * Returns the total size of the header.
 */
static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes,
                     char *suffix, uint8_t *nsuffix) {
    /* suffix is defined at 40 chars elsewhere.. */
    *nsuffix = (uint8_t) snprintf(suffix, 40, " %d %d\r\n", flags, nbytes - 2);
    return sizeof(item) + nkey + *nsuffix + nbytes;
}

/*
 * Returns a item buffer from the freelist, if any. 
 * */
static item *item_from_freelist(size_t ntotal) {
    item *s;
    if (ntotal > settings.item_buf_size){
        return NULL;
    }

    if (freeitemcurr > 0) {
        s = freeitem[--freeitemcurr];
    } else {
        /* If malloc fails, let the logic fall through without spamming
         * STDERR on the server. */
        s = (item *)malloc( settings.item_buf_size );
    }

    return s;
}

void item_init(void) {
    freeitemtotal = INIT_ITEM_FREELIST_LENGTH;
    freeitemcurr  = 0;

    freeitem = (item **)malloc( sizeof(item *) * freeitemtotal );
    if (freeitem == NULL) {
        perror("malloc()");
    }
    return;
}

/*
 * alloc a item buffer from the freelist. Should call this using
 * item_alloc() for thread safety.
 */
item *do_item_alloc(char *key, const size_t nkey, const int flags, const int nbytes) {
    uint8_t nsuffix;
    item *it;
    char suffix[40];
    size_t ntotal = item_make_header(nkey + 1, flags, nbytes, suffix, &nsuffix);

    it = item_from_freelist(ntotal);
    if (it == NULL){
        return NULL;
    }

    it->nkey = nkey;
    it->nbytes = nbytes;
    strcpy(ITEM_key(it), key);
    memcpy(ITEM_suffix(it), suffix, (size_t)nsuffix);
    it->nsuffix = nsuffix;
    return it;
}

/*
 * Adds a item to the freelist. 0 = success. Should call this using
 * item_add_to_freelist() for thread safety.
 */
int do_item_add_to_freelist(item *it) {
    if (freeitemcurr < freeitemtotal) {
        freeitem[freeitemcurr++] = it;
        return 0;
    } else {
        if (freeitemtotal >= MAX_ITEM_FREELIST_LENGTH){
            return 1;
        }
        /* try to enlarge free item buffer array */
        item **new_freeitem = realloc(freeitem, freeitemtotal * 2);
        if (new_freeitem) {
            freeitemtotal *= 2;
            freeitem = new_freeitem;
            freeitem[freeitemcurr++] = it;
            return 0;
        }
    }
    return 1;
}

