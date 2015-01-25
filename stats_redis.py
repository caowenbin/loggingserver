#!/usr/bin/env python
#coding=utf-8

import json

class RedisStats(object):
    def __init__(self, redis, logger):
        self.redis = redis
        self.logger = logger

    def record(self, stats_log):
        if (not stats_log) or (not stats_log.startswith('::STATS::')):
            return
        stats_str = stats_log[len('::STATS::'):]
        if not stats_str:
            return 
        self.stats_decode(stats_str)

    def stats_decode(self, stats_str):
        try:
            r = True
            msg_d = json.loads(stats_str)
            groupname = msg_d['__group__']
            nodename = msg_d['__name__']
            skip_sections = ['__group__', '__name__']
            support_types = ['update', 'set']
            for section in msg_d.keys():
                if section in skip_sections:
                    continue
                options = msg_d[section]
                type = options['__type__']
                if type == 'update': # 直接更新以前记录
                    if not options:
                        self.logger.warn('%s is None' % section)
                        continue
                    redis_key = 'monitor::%s::%s::%s' % (groupname, nodename, section)
                    r = self.redis.hmset(redis_key, options)
                    self.logger.info('hmset %s %s %s' % (redis_key, options, r)) 
                elif type == 'set':  # 先清空之前记录，再设置
                    redis_key = 'monitor::%s::%s::%s' % (groupname, nodename, section)
                    r = self.redis.delete(redis_key)  # 先删除
                    self.logger.info('hdel %s %s' % (redis_key, r)) 
                    if not options:
                        self.logger.warn('%s is None' % section)
                        continue
                    r = self.redis.hmset(redis_key, options)
                    self.logger.info('hmset %s %s %s' % (redis_key, options, r)) 
        except Exception, e:
            self.logger.exception(e)

    def list(self, groupname=None, nodename=None, section=None):
        ret = {}
        key1 = groupname if groupname else '*'
        key2 = nodename if nodename else '*'
        key3 = section if section else '*'
        redis_key = 'monitor::%s::%s::%s' % (key1, key2, key3)
        stats_keys = self.redis.keys(redis_key)
        for stats_key in stats_keys:
            r = self.redis.hgetall(stats_key)

            #_groupname_ = r['__groupname__'] if '__groupname__' in r else '-'
            #_nodename_ = r['__nodename__'] if '__nodename__' in r else '-'
            #_name_ = r['__name__'] if '__name__' in r else '-'

            _groupname_ = '-'
            if '__groupname__' in r:
                _groupname_ = r['__groupname__']
                r.pop('__groupname__')
            _nodename_ = '-'
            if '__nodename__' in r:
                _nodename_ = r['__nodename__']
                r.pop('__nodename__')
            _name_ = '-'
            if '__name__' in r:
                _name_ = r['__name__']
                r.pop('__name__')
            if '__type__' in r:
                r.pop('__type__')

            if _groupname_ not in ret:
                ret[_groupname_] = {}
            if _nodename_ not in ret[_groupname_]:
                ret[_groupname_][_nodename_] = {}
            if _name_ not in ret[_groupname_][_nodename_]:
                ret[_groupname_][_nodename_][_name_] = {}
            ret[_groupname_][_nodename_][_name_].update(r)
        return ret

    def list_nodes(self, groupname=None):
        ret = {}
        key1 = groupname if groupname else '*'
        redis_key = 'monitor::%s::*' % key1
        stats_keys = self.redis.keys(redis_key)
        for stats_key in stats_keys:
            l = stats_key.split('::')
            if len(l) != 4:
                self.logger.warn('invalid redis key:%s'%stats_key)
                continue
            groupname = l[1]
            nodename = l[2]
            #section = l[3]
            if groupname not in ret:
                ret[groupname] = []
            if nodename not in ret[groupname]:
                ret[groupname].append(nodename)
        return ret

    def list_sections(self, groupname=None, nodename=None):
        ret = {}
        key1 = groupname if groupname else '*'
        key2 = nodename if nodename else '*'
        redis_key = 'monitor::%s::%s::*' % (key1, key2)
        stats_keys = self.redis.keys(redis_key)
        for stats_key in stats_keys:
            l = stats_key.split('::')
            if len(l) != 4:
                self.logger.warn('invalid redis key:%s'%stats_key)
                continue
            groupname = l[1]
            nodename = l[2]
            section = l[3]
            if groupname not in ret:
                ret[groupname] = {}
            if nodename not in ret[groupname]:
                ret[groupname][nodename] = []
            if section not in ret[groupname][nodename]:
                ret[groupname][nodename].append(section)
        return ret

if __name__ == "__main__":

    from mylogger import Logger
    logger = Logger.getLogger('debug', None, 'DEBUG', True)
    Logger.addLoggingServer(logger, '127.0.0.1', 9900)

    from stats_client import AppStats
    stats = AppStats('cluster1', 'selector', logger)
    stats2 = AppStats('cluster2', 'master', logger)
    stats3 = AppStats('cluster3', 'parser', logger)

    import redis
    redis_ = redis.Redis()
    redisStats = RedisStats(redis_, logger)

    section = 'process'
    options = {'mem': 1000, 'cpu':0.01}

    stats_str = stats.stats_encode('p1', options, 'set')
    redisStats.stats_decode(stats_str)

    stats_str = stats2.stats_encode('p2', options, 'set')
    redisStats.stats_decode(stats_str)

    stats_str = stats.stats_encode('p3', options, 'set')
    redisStats.stats_decode(stats_str)

    stats_str = stats2.stats_encode('p4', options, 'set')
    redisStats.stats_decode(stats_str)

    stats_str = stats.stats_encode('p5', options, 'set')
    redisStats.stats_decode(stats_str)

    stats_str = stats2.stats_encode('p6', options, 'set')
    redisStats.stats_decode(stats_str)

    options = {'带宽': 1000, '速度':0.01, '解析数量': 102392, '存储数量': 1029394}
    stats_str = stats3.stats_encode('Stats', options, 'update')
    redisStats.stats_decode(stats_str)

    stats_str = stats3.stats_encode('AStats', options, 'update')
    redisStats.stats_decode(stats_str)

    stats_str = stats3.stats_encode('BStats', options, 'update')
    redisStats.stats_decode(stats_str)

    stats_str = stats3.stats_encode('CStats', options, 'update')
    redisStats.stats_decode(stats_str)

    stats_str = stats3.stats_encode('Process', options, 'set')
    redisStats.stats_decode(stats_str)

    print 'AAAAAAAAAAAAAAAAAAAAAAAAAAA'
    print redisStats.list(groupname=None, nodename=None, section=None)
    print redisStats.list(groupname=None, nodename='master', section=None)
    print redisStats.list(groupname='cluster2', nodename='master', section=None)
    print redisStats.list(groupname=None, nodename=None, section='p1')

    print 'BBBBBBBBBBBBBBBBBBBBBBBBBBB'
    print redisStats.list_nodes(groupname=None)
    print redisStats.list_nodes(groupname='cluster2')
    print redisStats.list_sections(groupname=None, nodename=None)
