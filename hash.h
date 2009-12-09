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

#ifndef QLIST_H__
#define QLIST_H__

unsigned int murmurhash2( const void * key, int len, unsigned int seed );

#endif /* QLIST_H__ */