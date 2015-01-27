#!/usr/bin/env python
#coding=utf-8

import json

class AppStats(object):
    def __init__(self, groupname, nodename, logger=None):
        self.groupname = groupname
        self.nodename = nodename
        self.logger = logger

    def log(self, section, options, type):
        stats_str = self.stats_encode(section, options, type)
        if stats_str:
            self.stats_log(stats_str)

    def stats_log(self, stats_str):
        self.logger.error('::STATS::%s' % stats_str)

    def stats_encode(self, section, options, type):
        support_type = ['update', 'set']
        assert type in support_type

        options['__type__'] = type
        options['__name__'] = section
        options['__groupname__'] = self.groupname
        options['__nodename__'] = self.nodename

        stats_d = {   '__group__': self.groupname, 
                      '__name__': self.nodename,
                      section: options,
         }
        jsonstr = json.dumps(stats_d, ensure_ascii=True)
        return jsonstr

StatsClient = AppStats

if __name__ == "__main__":

    from mylogger import Logger
    logger = Logger.getLogger('test1', None, 'DEBUG', True)
    Logger.addLoggingServer(logger, '127.0.0.1', 9900)

    logger2 = Logger.getLogger('test2', None, 'DEBUG', True)
    Logger.addLoggingServer(logger2, '127.0.0.1', 9900)

    stats = AppStats('cluster1', 'master', logger)
    stats2 = AppStats('cluster2', 'spider', logger2)

    section = 'process'
    options = {'mem': 1000, 'cpu':0.01}
    stats.log('p1', options, 'set')
    stats.log('p2', options, 'set')
    stats.log('p3', options, 'set')
    stats.log('p4', options, 'set')
    stats.log('p5', options, 'set')
    stats.log('p6', options, 'set')

    stats2.log('p1', options, 'set')
    stats2.log('p2', options, 'set')
    stats2.log('p3', options, 'set')
    stats2.log('p4', options, 'set')
    stats2.log('p5', options, 'set')
    stats2.log('p6', options, 'set')
 
