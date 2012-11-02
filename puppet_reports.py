#!/usr/bin/python

import collectd
import yaml
import os

NAME = 'puppet_reports'

DEFAULT_REPORTS_DIR = '/home/pyr/reports'

def compute_log_metrics(data):
  return {'log_info': len(filter(lambda x: x['level'] == 'info', data)),
          'log_notice': len(filter(lambda x: x['level'] == 'notice', data)),
          'log_warning': len(filter(lambda x: x['level'] == 'warning', data)),
          'log_error': len(filter(lambda x: x['level'] == 'error', data))}

def tridict(prefix, data):
  dicts = map(lambda x: {(prefix + '_' + x[0]): x[2]}, data)
  return reduce(lambda x,y: dict(x, **y), dicts)

def compute_metrics(data):
  h= {'configuration_version': data['configuration_version']}
  h.update(compute_log_metrics(data['logs']))
  h.update(tridict('changes', data['metrics']['changes']['values']))
  h.update(tridict('events', data['metrics']['events']['values']))
  h.update(tridict('resources', data['metrics']['resources']['values']))
  h.update(tridict('time', data['metrics']['time']['values']))

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

yaml.add_multi_constructor("!", identity)

def read_callback():
  for report_dir in os.listdir(REPORTS_DIR):
    reports_dir = os.listdir(REPORTS_DIR + '/' + report_dir)
    reports_dir.sort
    last_report = reports_dir[-1]
    last_report_file = REPORTS_DIR + '/' + report_dir + '/' + last_report
    with open(last_report_file, "r") as stream:
      data = yaml.load(stream)
      results = compute_metrics(yaml.load(stream))
      for k,v in compute_metrics(yaml.load(stream)):
        val = collectd.values(plugin=NAME, val_type='counter')
        val.plugin_instance = reports_dir
        val.type_instance = k
        val.values = [ v ]
        val.dispatch()

def configure_callback(conf):
  global REPORTS_DIR, VERBOSE_LOGGING

  REPORTS_DIR = DEFAULT_REPORTS_DIR
  VERBOSE_LOGGING = False

  for node in conf.children:
    if node.key == 'ReportsDir':
      REPORTS_DIR = node.val
    else:
      logger.warn('warn', 'unknown config key in puppet module: %s' % node.key)
    
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
