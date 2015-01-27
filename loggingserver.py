#!/usr/bin/env python
#coding=utf-8

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from tornado.options import define, options
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

import os
import sys
import time
import json
import redis
import logging
import requests
import signal
import pymongo
from mylogger import Loggers
from stats_redis import RedisStats

define("http_port", default=9900, help="run on the given port", type=int)
define("redis_addr", default='192.168.1.65', help="redis host", type=str)
define("redis_port", default=6379, help="redis port", type=int)

define("logdir", default='log', help="log directory", type=str)
define("logname", default='monitor', help="log module name", type=str)
define("logfile", default='monitor.log', help="log file", type=str)
define("logdebug", default=False, help="log debug mode", type=bool)
define("loglevel", default='DEBUG', help="log level", type=str)

define("loggingserver_host", default=None, help="log server addr", type=str)
define("loggingserver_port", default=9900, help="log server port", type=int)
define("loggingserver_path", default='/log', help="log server uri", type=str)
define("loggingserver_level", default='ERROR', help="log server level", type=str)
define("loggingserver_method", default='GET', help="log server http method", type=str)
 
class Application(tornado.web.Application):
    def __init__(self, redis_, logname, loggers):
        handlers = [
            (r"/logging", LoggingApiHandler),
            (r"/log", LoggingApiHandler),

            (r"/stats/list/([^/]+)/?([^/]*)/?([^/]*)?", StatsApiHandler),
            (r"/", StatsHomeHandler),
            (r"/stats/?", StatsHomeHandler),
            (r"/stats/manager/?", StatsManagerHandler),
            (r"/stats/node/([^/]+)/([^/]+)/?", StatsNodeHandler),
        ]
        settings = dict(
            title=u"logging stats",
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            ui_modules={"Entry": EntryModule},
            #debug=True,
        )
        tornado.web.Application.__init__(self, handlers, **settings)

        self.redis_ = redis_
        self.loggers = loggers
        self.logger = self.loggers[logname]
        self.stats = RedisStats(self.redis_, self.logger)

        #tornado.ioloop.PeriodicCallback(self.timer, 10000).start()
        
        #这里是绑定信号处理函数，将SIGTERM绑定在函数onsignal_term上面    
        signal.signal(signal.SIGTERM, self.onsignal)  
        signal.signal(signal.SIGINT, self.onsignal)  

    def timer(self):
        #tornado.ioloop.IOLoop.instance().add_timeout(time.time()+10, self.timer)
        self.logger.info('timer')
        # TODO: 定时同步数据到rrd

    def onsignal(self, sig, frame):
        self.logger.info('will to be stopping after 1 second')
        #tornado.ioloop.PeriodicCallback(self.stop_timer, 1000).start()
        self.stop_timer()
    def stop_timer(self):
        tornado.ioloop.IOLoop.instance().stop()
        self.logger.info('stopped')

class BaseHandler(tornado.web.RequestHandler):
    @property
    def redis(self):
        return self.application.redis_
    @property
    def logger(self):
        return self.application.logger
    @property
    def loggers(self):
        return self.application.loggers
    @property
    def stats(self):
        return self.application.stats

class LoggingApiHandler(BaseHandler):
    ''' 日志服务器, 并负责将统计信息存入db '''

    def get(self):
        errcode = 0
        errmsg = 'success'
        try:
            logname = self.get_argument("name", '-')
            loglevel = self.get_argument("levelname", '-')
            filename = self.get_argument("filename", '-')
            lineno = self.get_argument("lineno", '-')
            funcName = self.get_argument("funcName", '-')
            created = self.get_argument("created", '0')
            message = self.get_argument("message", None) # msg
            self.loggers[logname].info('==> %s %s %s +%s %s %s' % (loglevel, created, filename, lineno, funcName, message))
            if message and message.startswith('::STATS::'):
                self.stats.record(message)
        except ValueError, e:
            errcode = -1
            errmsg = e.message
            if e.message == "No JSON object could be decoded":
                errmsg = "post data should be json format"
            self.logger.exception(e)
        except Exception, e:
            errcode = -1
            errmsg = e.message
            self.logger.exception(e)
        self.write({'errno': errcode, 'msg': errmsg})

    def post(self):
        self.get()

class StatsHomeHandler(BaseHandler):

    def get(self):
        monitor_groups = self.stats.list_nodes(groupname=None)
        #monitor_groups_info = self.stats.list(groupname=group, nodename=node, section=None)
        monitor_groups_info = {}
        self.render("stats_index.html",
                    title="logging stats", 
                    monitor_groups=monitor_groups,
                    monitor_groups_info=monitor_groups_info,
                    )
 
class StatsApiHandler(BaseHandler):

    def get(self, groupname, nodename=None, section=None):
        if section:
            infos = self.stats.list(groupname=groupname, nodename=nodename, section=section)
            self.write(infos)
        elif nodename:
            sections = self.stats.list_sections(groupname=groupname, nodename=nodename)
            self.write(sections)
        else:
            nodes = self.stats.list_nodes(groupname=groupname)
            self.write(nodes)
             
class StatsManagerHandler(BaseHandler):

    def get(self):
        monitor_groups = self.stats.list_nodes(groupname=None)
        #monitor_groups_info = self.stats.list(groupname=group, nodename=node, section=None)
        monitor_groups_info = {}
        monitor_group_node_sections = self.stats.list_sections(groupname=None, nodename=None)
        self.render("stats_manager.html",
                    title="logging stats manager", 
                    monitor_groups=monitor_groups,
                    monitor_groups_info=monitor_groups_info,
                    monitor_group_node_sections = monitor_group_node_sections,
                    )

class StatsNodeHandler(BaseHandler):

    def get(self, group, node):
        monitor_groups = self.stats.list_nodes(groupname=None)
        monitor_groups_info = self.stats.list(groupname=group, nodename=node, section=None)
        self.render('stats_node.html',
                    monitor_groups = monitor_groups,
                    monitor_groups_info = monitor_groups_info,
                    )

class EntryModule(tornado.web.UIModule):
    def render(self, key, kvs):
        return self.render_string('modules/entry.html', key=key, kvs=kvs)

def main():
    tornado.options.parse_command_line()

    logdir = options.logdir
    default_level = options.loglevel
    default_debug = options.logdebug

    default_loggingserver_host = options.loggingserver_host # 禁用logging server
    default_loggingserver_port = options.loggingserver_port
    default_loggingserver_path = options.loggingserver_path
    default_loggingserver_level = options.loggingserver_level
    default_loggingserver_method = options.loggingserver_method
    loggers = Loggers(logdir, default_level, default_debug, \
                      default_loggingserver_host, default_loggingserver_port,  \
                      default_loggingserver_path, default_loggingserver_level, \
                      default_loggingserver_method)

    loggers[options.logname].info('connect to redis %s:%s ' % (options.redis_addr, options.redis_port))
    redis_ = redis.Redis(host=options.redis_addr, port=options.redis_port)

    loggers[options.logname].info('listen %s' % options.http_port)
    application = Application(redis_, options.logname, loggers)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.http_port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
