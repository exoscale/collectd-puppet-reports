#!/usr/bin/env python

import collectd
import yaml
import os

NAME = 'puppet_reports'

DEFAULT_REPORTS_DIR = '/var/lib/puppet/reports'

def compute_log_metrics(data):
  return {'log_info': len(filter(lambda x: x['level'] == 'info', data)),
          'log_notice': len(filter(lambda x: x['level'] == 'notice', data)),
          'log_warning': len(filter(lambda x: x['level'] == 'warning', data)),
          'log_error': len(filter(lambda x: x['level'] == 'error', data))}

def tridict(prefix, data):
  dicts = map(lambda x: {(prefix + '_' + x[0]): x[2]}, data)
  return reduce(lambda x,y: dict(x, **y), dicts)

def compute_metrics(data):
  h = {'configuration_version': data['configuration_version']}
  h.update(compute_log_metrics(data['logs']))
  h.update(tridict('changes', data['metrics']['changes']['values']))
  h.update(tridict('events', data['metrics']['events']['values']))
  h.update(tridict('resources', data['metrics']['resources']['values']))
  h.update(tridict('time', data['metrics']['time']['values']))
  return h

def identity(loader, suffix, node):
  return node

def map_value(node):
  if isinstance(node,yaml.nodes.MappingNode):
    dicts = map(lambda x: dict({map_value(x[0]): map_value(x[1])}), node.value)
    h = reduce(lambda e1,e2: dict(e1, **e2), dicts)
    return h
  elif isinstance(node,yaml.nodes.SequenceNode):
    return map(map_value, node.value)
  elif isinstance(node,yaml.nodes.ScalarNode):
    return node.value
  elif isinstance(node,list):
    return map(map_value, node)
  elif isinstance(node,tuple):
    return map(map_value, node)
  else:
    return node


def read_callback():
  yaml.add_multi_constructor("!", identity)
  logger('verb', "starting run")
  for report_dir in os.listdir(REPORTS_DIR):
    logger('verb', "parsing: %s" % report_dir)
    reports_dir = os.listdir(REPORTS_DIR + '/' + report_dir)
    reports_dir.sort
    last_report = reports_dir[-1]
    last_report_file = REPORTS_DIR + '/' + report_dir + '/' + last_report
    with open(last_report_file, "r") as stream:
      data = yaml.load(stream)
      data = map_value(data)
      results = compute_metrics(data)
      logger('verb', "ready to send")
      for k in results:
        logger('verb', ("pushing value for %s => %s = %s" % (report_dir, k, results[k])))
        val = collectd.Values(plugin=NAME, type='counter')
        val.plugin_instance = report_dir
        val.type_instance = k
        val.values = [ float(results[k]) ]
        val.dispatch()

def configure_callback(conf):
  global REPORTS_DIR, VERBOSE_LOGGING

  yaml.add_multi_constructor("!", identity)
  logger('verb', "configuring")

  REPORTS_DIR = DEFAULT_REPORTS_DIR
  VERBOSE_LOGGING = False

  for node in conf.children:
    if node.key == 'ReportsDir':
      REPORTS_DIR = node.values[0]
    else:
      logger('verb', "unknown config key in puppet module: %s" % node.key)
    
# logging function
def logger(t, msg):
    if t == 'err':
        collectd.error('%s: %s' % (NAME, msg))
    elif t == 'warn':
        collectd.warning('%s: %s' % (NAME, msg))
    elif t == 'verb':
        if VERBOSE_LOGGING:
            collectd.info('%s: %s' % (NAME, msg))
    else:
        collectd.notice('%s: %s' % (NAME, msg))

collectd.register_config(configure_callback)
collectd.register_read(read_callback)
