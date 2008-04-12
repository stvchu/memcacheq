#! /usr/bin/env python
# -*- coding: utf-8 -*-

import memcache

mc = memcache.Client(['127.0.0.1:21201'], debug=1)
print mc.db_archive()
print mc.db_checkpoint()
print mc.rep_ismaster()
print mc.rep_whoismaster()
print mc.rep_set_priority(100)
print mc.rep_set_ack_policy(5)
print mc.rep_set_ack_timeout(20000)
print mc.rep_set_request(4, 16)
print mc.set('q', 'a' * 500)
mc.disconnect_all()
