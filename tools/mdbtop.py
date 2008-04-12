#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
a Monitor for Memcachedb

Overview
========

See U{the Memcachedb homepage<http://memcachedb.googlecode.com>} for more about memcachedb.

Usage summary
=============

Command::

  python mdbtop.py <config file>

Monitor config file example::

  # here a bunch of servers you wanna monitor
  [Server]  
  server1 = 127.0.0.1:21201
  server2 = 127.0.0.1:21202
  [View]    
  interval = 2  # display interval for seconds

"""

import sys
import time
import signal
import ConfigParser

import memcache

__author__    = "Steve Chu <stvchu@gmail.com>"
__version__ = "0.0.2"
__copyright__ = "Copyright (C) 2008 Steve Chu"
__license__   = "Python"

class Monitor:
  def __init__(self, cfg_file):
    self.mdb_servers = []
    self.mdb_clients = []
    self.cfg = ConfigParser.ConfigParser()
    self.cfg.readfp(open(cfg_file))
    self.interval = int(self.cfg.get('View', 'interval'))
    for (key,value) in self.cfg.items('Server'):
      self.mdb_servers.append([value])
    for mdb_server in self.mdb_servers:
      self.mdb_clients.append(memcache.Client(mdb_server, debug = 0))

  def top(self):
    print 
    print 'mdbtop.py %s, %s' % (__version__, __author__)
    print 
    while True:
      for mdb_client in self.mdb_clients:
        # deamon stats
        data = []
        stats = {}
        stats_bdb = {}
        stats_rep = {}
        data = mdb_client.get_stats()
        if data != [] and data[0][1] != {}:
          stats = data[0][1]
          localhp = data[0][0]
        # bdb stats
        data = mdb_client.get_stats('bdb')
        if data != [] and data[0][1] != {}:
          stats_bdb = data[0][1]
        # rep stats
        data = mdb_client.get_stats('rep')
        if data != [] and data[0][1] != {}:
          stats_rep = data[0][1]

        if stats == {} and stats_bdb == {} and stats_rep =={}:
          continue

        print "=============================================================================="

        if stats != {}:
          print "[SRV] localhp: %s    pid: %s    threads: %s" % (localhp, stats['pid'], stats['threads'])
          print "      ibuffer: %sB    ver: %s" % (stats['ibuffer_size'], stats['version'])

        if stats_bdb != {}:
          print "[BDB] cache_size: %sMB    txn_lg_bsize: %sKB    txn_nosync: %s" % (int(stats_bdb['cache_size'])/(1024*1024), int(stats_bdb['txn_lg_bsize'])/1024, stats_bdb['txn_nosync'])
          print "      dldetect_val: %sms    chkpoint_val: %ss" % (int(stats_bdb['dldetect_val'])/1000, stats_bdb['chkpoint_val'])

        if stats_rep != {}:
          print "[REP] localhp:%s    priority: %s    bulk: %s" % (stats_rep['rep_localhp'], stats_rep['rep_priority'], stats_rep['rep_bulk'])
          print "      ack_policy: %s    ack_timeout: %sms    request: %s" % (stats_rep['rep_ack_policy'], int(stats_rep['rep_ack_timeout'])/1000, stats_rep['rep_request'])

        print

        if stats != {}:
          print "%-12s %10s  %10s  %13s  %10s  %10s" % ('uptime', 'curr_conns', 'bytes_read', 'bytes_written', 'cmd_get', 'cmd_set' )
          print "%-12s %10s  %10s  %13s  %10s  %10s" % (stats['uptime'], stats['curr_connections'], stats['bytes_read'], stats['bytes_written'], stats['cmd_get'], stats['cmd_set'] )
          print 

        if stats_rep != {}:
          print "%-9s  %21s %16s" % ('ismaster', 'whoismaster', 'next_lsn')
          print "%-9s  %21s %16s" % (stats_rep['rep_ismaster'], stats_rep['rep_whoismaster'], stats_rep['rep_next_lsn'])

      if self.interval == 0:
        break
      else:
        time.sleep(self.interval)

  def top_rep(self):
    print 
    print 'mdbtop.py %s, %s' % (__version__, __author__)
    print 
    while True:
      print "=============================================================================="
      print "%-21s %9s  %21s %16s" % ('server','ismaster', 'whoismaster', 'next_lsn')
      for mdb_client in self.mdb_clients:
        # deamon stats
        data = []
        stats_rep = {}
        data = mdb_client.get_stats('rep')
        if data != [] and data[0][1] != {}:
          localhp = data[0][0]
          stats_rep = data[0][1]

        if stats_rep =={}:
          continue

        if stats_rep != {}:
          print "%-21s %9s  %21s %16s" % (localhp, stats_rep['rep_ismaster'], stats_rep['rep_whoismaster'], stats_rep['rep_next_lsn'])

      if self.interval == 0:
        break
      else:
        time.sleep(self.interval)

  def close(self):
    for mdb_client in self.mdb_clients:
      mdb_client.disconnect_all()

def handler(signum, frame):
  sys.exit(0)

if __name__ == "__main__":
  signal.signal(signal.SIGTERM, handler)
  signal.signal(signal.SIGQUIT, handler)
  signal.signal(signal.SIGINT, handler)
  cfg_file = sys.argv[1]
  mon = Monitor(cfg_file)
  if len(sys.argv) == 2:
    mon.top()
  elif len(sys.argv) == 3 and sys.argv[2] == 'rep':
    mon.top_rep()
  else:
    print '\nUsage:'
    print 'python mdbtop.py <config file> <option>'
    sys.exit(1)
  mon.close()
