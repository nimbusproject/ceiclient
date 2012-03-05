import os
from pprint import pprint

import cloudinitd
from cloudinitd.user_api import CloudInitD
from cloudinitd.exceptions import APIUsageException

def load_cloudinitd_db(run_name):
    vars = {}
    home = os.environ['HOME']

    try:
        cid = CloudInitD(home + '/.cloudinitd', db_name=run_name, terminate=False, boot=False, ready=False)
    except APIUsageException, e:
        print "Problem loading records from cloudinit.d: %s" % str(e)
        raise

    svc_list = cid.get_all_services()
    for svc in svc_list:
        if svc.name == 'basenode':
            try:
                vars['rabbitmq_host'] = svc.get_attr_from_bag("hostname")
                vars['rabbitmq_username'] = svc.get_attr_from_bag("rabbitmq_username")
                vars['rabbitmq_password'] = svc.get_attr_from_bag("rabbitmq_password")
            except Exception, e:
                raise

    return vars
