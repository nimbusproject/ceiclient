import os
import sys
import errno
import pprint

def safe_print(p_str):
    try:
        print(p_str)
    except IOError, e:
        if e.errno == errno.EPIPE:
            sys.exit(0)
        else:
            raise

def safe_pprint(p_str):
    try:
        pprint.pprint(p_str)
    except IOError, e:
        if e.errno == errno.EPIPE:
            sys.exit(0)
        else:
            raise

def load_cloudinitd_db(run_name):

    # doing imports within function because they are not needed elsewhere
    # and they are surprisingly expensive.
    # (and this is generally only called once)
    from cloudinitd.user_api import CloudInitD
    from cloudinitd.exceptions import APIUsageException, ConfigException

    vars = {}
    home = os.environ['HOME']

    try:
        cid = CloudInitD(home + '/.cloudinitd', db_name=run_name, terminate=False, boot=False, ready=False)
    except APIUsageException, e:
        print "Problem loading records from cloudinit.d: %s" % str(e)
        raise

    svc_list = cid.get_all_services()
    services = dict((svc.name, svc) for svc in svc_list)

    rabbitmq = services.get('rabbitmq')
    basenode = services.get('basenode')

    if not rabbitmq and not basenode:
        raise Exception("cloudinit.d plan has neither rabbitmq or basenode services")

    if rabbitmq:
        vars['rabbitmq_host'] = rabbitmq.get_attr_from_bag("rabbitmq_host")
        vars['rabbitmq_username'] = rabbitmq.get_attr_from_bag("rabbitmq_username")
        vars['rabbitmq_password'] = rabbitmq.get_attr_from_bag("rabbitmq_password")
        try:
            vars['rabbitmq_exchange'] = rabbitmq.get_attr_from_bag("rabbitmq_exchange")
        except ConfigException:
            vars['rabbitmq_exchange'] = None
    else:
        vars['rabbitmq_host'] = basenode.get_attr_from_bag("hostname")
        vars['rabbitmq_username'] = basenode.get_attr_from_bag("rabbitmq_username")
        vars['rabbitmq_password'] = basenode.get_attr_from_bag("rabbitmq_password")
        try:
            vars['rabbitmq_exchange'] = basenode.get_attr_from_bag("rabbitmq_exchange")
        except ConfigException:
            vars['rabbitmq_exchange'] = None

    if basenode:
        try:
            vars['coi_services_system_name'] = basenode.get_attr_from_bag("coi_services_system_name")
        except ConfigException:
            vars['coi_services_system_name'] = None
        try:
            vars['dashi_sysname'] = basenode.get_attr_from_bag("dashi_sysname")
        except ConfigException:
            vars['dashi_sysname'] = None

    return vars
