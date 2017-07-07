FROM ubuntu:14.04

COPY start.sh /start.sh

RUN buildDeps='make gcc curl' \
        && apt-get update \
        && apt-get install -y  $buildDeps libevent-1.4.2 libdb5.3 libevent-dev libdb-dev \
        && curl -L -o memcacheq.tar.gz https://codeload.github.com/stvchu/memcacheq/tar.gz/v0.2.1 \
        && tar -xvf memcacheq.tar.gz -C /tmp --strip-components=1 \
        && cd /tmp \
        && ./configure --enable-threads \
        && make \
        && make install \
        && cd / && rm -rf /tmp/* \
        && apt-get purge -y --auto-remove $buildDeps

EXPOSE 22201

CMD ["/start.sh"]
