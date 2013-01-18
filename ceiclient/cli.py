#!/usr/bin/env python

import argparse
import sys

from dashi.exceptions import NotFoundError, WriteConflictError
import json
import yaml

import ceiclient
from ceiclient.exception import CeiClientError
from ceiclient.commands import DASHI_SERVICES, PYON_SERVICES
from ceiclient.connection import DashiCeiConnection, PyonCeiConnection
from ceiclient.common import safe_print

DEFAULT_RABBITMQ_USERNAME = 'guest'
DEFAULT_RABBITMQ_PASSWORD = 'guest'
DEFAULT_RABBITMQ_HOSTNAME = 'localhost'
DEFAULT_RABBITMQ_EXCHANGE = None
DEFAULT_TIMEOUT = 5


def using_pyon():
    """Peek into argv to see if user wants to use pyon or not
    """
    return '-P' in sys.argv or '--pyon' in sys.argv

parser = argparse.ArgumentParser(description='Client to control CEI services')

parser.add_argument('--broker', '-b', action='store', dest='broker')
parser.add_argument('--exchange', '-x', action='store', dest='exchange', default=None)
parser.add_argument('--username', '-u', action='store', dest='username')
parser.add_argument('--password', '-p', action='store', dest='password')
parser.add_argument('--timeout', '-t', action='store', dest='timeout', type=int, default=DEFAULT_TIMEOUT)
parser.add_argument('--yaml', '-Y', action='store_const', const=True)
parser.add_argument('--json', '-J', action='store_const', const=True)
parser.add_argument('--details', '-D', action='store_const', const=True)
parser.add_argument('--run-name', '-n', action='store', dest='run_name')
parser.add_argument('--service-name', '-d', action='store', default=None)
parser.add_argument('--sysname', '-s', action='store', default=None)
parser.add_argument('--caller', '-c', action='store', dest='caller', default=None)
parser.add_argument('--pyon', '-P', action='store_const', const=True)

if using_pyon():
    SERVICES = PYON_SERVICES
else:
    SERVICES = DASHI_SERVICES

subparsers = parser.add_subparsers(dest='service', help='Service to which to send a command')

for service_name, service in SERVICES.items():
    service_parser = subparsers.add_parser(service_name)
    service_subparsers = service_parser.add_subparsers(dest='command', help='Command to send to the service')
    for command_name, command in service.commands.items():
        command(service_subparsers)

opts = parser.parse_args()

def main():
    if opts.service not in SERVICES:
        raise ValueError('Service %s is not supported' % opts.service)

    service = SERVICES[opts.service]

    if opts.command not in service.commands:
        raise ValueError('Command %s is not supported by service %s' % (opts.command, opts.service))

    amqp_settings = {}

    # Set default amqp settings (for localhost testing)
    amqp_settings['rabbitmq_host'] = DEFAULT_RABBITMQ_HOSTNAME
    amqp_settings['rabbitmq_username'] = DEFAULT_RABBITMQ_USERNAME
    amqp_settings['rabbitmq_password'] = DEFAULT_RABBITMQ_PASSWORD
    amqp_settings['rabbitmq_exchange'] = DEFAULT_RABBITMQ_EXCHANGE

    # Read AMQP settings and credentials from the cloudinitd DB if possible
    if opts.run_name:
        amqp_settings = ceiclient.common.load_cloudinitd_db(opts.run_name)

    # Override with command line arguments
    if opts.broker:
        amqp_settings['rabbitmq_host'] = opts.broker
    if opts.username:
        amqp_settings['rabbitmq_username'] = opts.username
    if opts.password:
        amqp_settings['rabbitmq_password'] = opts.password
    if opts.exchange:
        amqp_settings['rabbitmq_exchange'] = opts.exchange
    if opts.sysname:
        amqp_settings['coi_services_system_name'] = opts.sysname
        amqp_settings['dashi_sysname'] = opts.sysname

    if opts.pyon:
        conn = PyonCeiConnection(amqp_settings['rabbitmq_host'],
                amqp_settings['rabbitmq_username'],
                amqp_settings['rabbitmq_password'],
                sysname=amqp_settings.get('coi_services_system_name'),
                timeout=opts.timeout)
        client = service.client(conn, service_name=opts.service_name)
    else:
        conn = DashiCeiConnection(amqp_settings['rabbitmq_host'],
                amqp_settings['rabbitmq_username'],
                amqp_settings['rabbitmq_password'],
                exchange=amqp_settings['rabbitmq_exchange'],
                timeout=opts.timeout,
                sysname=amqp_settings.get('dashi_sysname'))
        client = service.client(conn, dashi_name=opts.service_name)

    command = service.commands[opts.command]
    try:
        result = command.execute(client, opts)
    except (NotFoundError, WriteConflictError) as e:
        raise CeiClientError(e.value)

    if opts.yaml:
        safe_print(yaml.safe_dump(result, default_flow_style=False)),
    elif opts.json:
        safe_print(json.dumps(result, indent=4)),
    elif opts.details:
        command.details(result)
    else:
        command.output(result)

    conn.disconnect()

def start():
    try:
        main()
    except CeiClientError as e:
        sys.exit("Error: " + str(e))
