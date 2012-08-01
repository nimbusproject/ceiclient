import os


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
    for svc in svc_list:
        if svc.name == 'basenode':
            try:
                vars['rabbitmq_host'] = svc.get_attr_from_bag("hostname")
                vars['rabbitmq_username'] = svc.get_attr_from_bag("rabbitmq_username")
                vars['rabbitmq_password'] = svc.get_attr_from_bag("rabbitmq_password")
                try:
                    vars['rabbitmq_exchange'] = svc.get_attr_from_bag("rabbitmq_exchange")
                except ConfigException:
                    vars['rabbitmq_exchange'] = None
                try:
                    vars['coi_services_system_name'] = svc.get_attr_from_bag("coi_services_system_name")
                except ConfigException:
                    vars['coi_services_system_name'] = None
            except Exception, e:
                raise

    return vars
