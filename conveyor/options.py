from __future__ import absolute_import

import optparse
import socket


op = optparse.OptionParser("usage: %prog [options]")

og = optparse.OptionGroup(op, 'Session Options')
og.add_option('--servers',
              dest='servers',
              help="zookeeper connection string (default: %default)")
og.add_option('--timeout',
              dest='timeout',
              type='int',
              help="zookeeper connection timeout (default: %default)")
og.add_option('--host-id',
              dest='host_id',
              help="host id (default: %default)")
og.add_option('--groups',
              dest='groups',
              help="comma-separated list of groups")
op.add_option_group(og)

og = optparse.OptionGroup(op, 'Deployment Options')
og.add_option('--deployment-factor',
              dest='deployment_factor',
              type='int',
              help="deployment factor (default: %default)")
og.add_option('--deployment-tries',
              dest='deployment_tries',
              type='int',
              help="deployment tries (default: %default)")
op.add_option_group(og)

og = optparse.OptionGroup(op, 'Output and Logging Options')
og.add_option('--log-level',
              dest='log_level',
              type='choice',
              choices=['critical', 'error', 'warning', 'info', 'debug'],
              help="critical, error, warning, info, debug (default: %default)")
og.add_option('--log-file-path',
              dest='log_file_path',
              help="path for optional log file")
og.add_option('--log-file-rotate-interval-type',
              dest='log_file_rotate_interval_type',
              type='choice',
              choices=['s', 'm', 'h', 'd', 'w', 'midnight'],
              help="s=seconds, m=minutes, h=hours, d=days, w=week day (0=monday), midnight (default: %default)")
og.add_option('--log-file-rotate-interval',
              dest='log_file_rotate_interval',
              type='int',
              help="log rotation interval (default: %default)")
og.add_option('--log-file-max-backups',
              dest='log_file_max_backups',
              type='int',
              help="number of log files to keep when rotating (default: %default)")
op.add_option_group(og)

op.set_defaults(servers = 'localhost:2181/conveyor',
                timeout = 10,
                host_id = socket.getfqdn(),
                groups = [],
                deployment_factor = '1',
                deployment_tries = '3', # FIXME
                log_level = 'info',
                log_file_rotate_interval_type = 'd',
                log_file_rotate_interval = 7,
                log_file_max_backups = 4)

(options, args) = op.parse_args()

if len(options.groups) > 0:
    groups = []
    for group in options.groups.split(','):
        groups.append(group.strip())
    options.groups = list(set(groups)) # remove duplicate groups
