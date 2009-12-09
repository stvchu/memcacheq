#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2008 Steve Chu.  All rights reserved.
# 
# Use and distribution licensed under the BSD license.  See
# the LICENSE file for full text.
# 
# Authors:
#     Steve Chu <stvchu@gmail.com>

import memcache

mc = memcache.Client(['127.0.0.1:22201'], debug=1)
for i in range(100000):
  mc.set("test1", "1234567890")
print mc.get_stats("queue")
mc.disconnect_all()
