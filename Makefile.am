bin_PROGRAMS = memcacheq
memcacheq_SOURCES = memcacheq.c ibuffer.c memcacheq.h thread.c bdb.c

SUBDIRS = doc tools
EXTRA_DIST = doc tools CREDITS AUTHORS LICENSE

dist-hook:
	rm -rf $(distdir)/doc/.svn/
	rm -rf $(distdir)/tools/.svn/
