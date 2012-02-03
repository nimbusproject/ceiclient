from pprint import pprint

import cloudinitd
from cloudinitd.user_api import CloudInitD
from cloudinitd.exceptions import APIUsageException

def load():

    try:
        cid = CloudInitD('/Users/priteau/.cloudinitd', db_name='processes', terminate=False, boot=False, ready=False)
        cid.start()
        cid.block_until_complete()
    except Exception, e:
        raise e
#IncompatibleEnvironment("Problem loading records from cloudinit.d: %s" % str(e))
    pprint(cid.get_all_services())

#def load(p, c, m, run_name, cloudinitd_dbdir, silent=False, terminate=False, wholerun=True):
#    """Load any EPU related instances from a local cloudinit.d launch with the same run name.
#    """
#
#    try:
#        cb = CloudInitD(cloudinitd_dbdir, db_name=run_name, terminate=terminate, boot=False, ready=False)
#        cb.start()
#        cb.block_until_complete()
#    except APIUsageException, e:
#        raise IncompatibleEnvironment("Problem loading records from cloudinit.d: %s" % str(e))
#    svc_list = cb.get_all_services()
#
#    count = 0
#    for svc in svc_list:
#        foundservice = None
#        if svc.name.find("epu-") == 0:
#            foundservice = svc.name
#        elif svc.name.find("provisioner") == 0:
#            foundservice = "provisioner"
#        elif wholerun:
#            foundservice = svc.name
#        if foundservice:
#            count += 1
#            instance_id = svc.get_attr_from_bag("instance_id")
#            hostname = svc.get_attr_from_bag("hostname")
#            _load_host(p, c, m, run_name, instance_id, hostname, foundservice)
#
#    if silent:
#        return cb
#
#    if not count and not wholerun:
#        msg = "Services must be named 'svc-epu-*' or 'svc-provisioner' in order to be recognized."
#        c.log.info("No EPU related services in the cloudinit.d '%s' launch. %s" % (run_name, msg))
#
#    if not count and wholerun:
#        c.log.info("No services in the cloudinit.d '%s' launch." % run_name)
#
#    stype = "EPU related service"
#    if wholerun:
#        stype = "service"
#    if count == 1:
#        c.log.info("One %s in cloudinit.d '%s' launch" % (stype, run_name))
#    else:
#        c.log.info("%d %ss in the cloudinit.d '%s' launch" % (count, stype, run_name))
#
#    return cb
