import uuid

import connection
from commands import DTRSAddDT, DTRSDescribeDT, DTRSListDT, DTRSRemoveDT, DTRSUpdateDt
from commands import DTRSAddSite, DTRSDescribeSite, DTRSListSites, DTRSRemoveSite, DTRSUpdateSite
from commands import DTRSAddCredentials, DTRSDescribeCredentials, DTRSListCredentials, DTRSRemoveCredentials, DTRSUpdateCredentials
from commands import EPUMAdd, EPUMDescribe, EPUMList, EPUMReconfigure, EPUMRemove
from commands import PDDispatch, PDDescribeProcess, PDDescribeProcesses, PDTerminateProcess, PDDump, PDRestartProcess
from commands import ProvisionerDump, ProvisionerDescribeNodes, ProvisionerProvision, ProvisionerTerminateAll


class CeiClient(object):

    def __init__(self, connection, dashi_name=None):
        pass


class DTRSDTClient(CeiClient):

    dashi_name = 'dtrs'
    name = 'dt'
    help = 'Control Deployable Types in the DTRS'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def add_dt(self, caller, dt_name, dt_definition):
        return self._connection.call(self.dashi_name, 'add_dt', caller=caller, dt_name=dt_name, dt_definition=dt_definition)

    def describe_dt(self, caller, dt_name):
        return self._connection.call(self.dashi_name, 'describe_dt', caller=caller, dt_name=dt_name)

    def list_dts(self, caller):
        return self._connection.call(self.dashi_name, 'list_dts', caller=caller)

    def remove_dt(self, caller, dt_name):
        return self._connection.call(self.dashi_name, 'remove_dt', caller=caller, dt_name=dt_name)

    def update_dt(self, caller, dt_name, dt_definition):
        return self._connection.call(self.dashi_name, 'update_dt', caller=caller, dt_name=dt_name, dt_definition=dt_definition)

    commands = {}
    for command in [DTRSAddDT, DTRSDescribeDT, DTRSListDT, DTRSRemoveDT, DTRSUpdateDt]:
        commands[command.name] = command


class DTRSSiteClient(CeiClient):

    dashi_name = 'dtrs'
    name = 'site'
    help = 'Control sites in the DTRS'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def add_site(self, site_name, site_definition):
        return self._connection.call(self.dashi_name, 'add_site', site_name=site_name, site_definition=site_definition)

    def describe_site(self, site_name):
        return self._connection.call(self.dashi_name, 'describe_site', site_name=site_name)

    def list_sites(self):
        return self._connection.call(self.dashi_name, 'list_sites')

    def remove_site(self, site_name):
        return self._connection.call(self.dashi_name, 'remove_site', site_name=site_name)

    def update_site(self, site_name, site_definition):
        return self._connection.call(self.dashi_name, 'update_site', site_name=site_name, site_definition=site_definition)

    commands = {}
    for command in [DTRSAddSite, DTRSDescribeSite, DTRSListSites, DTRSRemoveSite, DTRSUpdateSite]:
        commands[command.name] = command


class DTRSCredentialsClient(CeiClient):

    dashi_name = 'dtrs'
    name = 'credentials'
    help = 'Control credentials in the DTRS'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def add_credentials(self, caller, site_name, site_credentials):
        return self._connection.call(self.dashi_name, 'add_credentials', caller=caller, site_name=site_name, site_credentials=site_credentials)

    def describe_credentials(self, caller, site_name):
        return self._connection.call(self.dashi_name, 'describe_credentials', caller=caller, site_name=site_name)

    def list_credentials(self, caller):
        return self._connection.call(self.dashi_name, 'list_credentials', caller=caller)

    def remove_credentials(self, caller, site_name):
        return self._connection.call(self.dashi_name, 'remove_credentials', caller=caller, site_name=site_name)

    def update_credentials(self, caller, site_name, site_credentials):
        return self._connection.call(self.dashi_name, 'update_credentials', caller=caller, site_name=site_name, site_credentials=site_credentials)

    commands = {}
    for command in  [DTRSAddCredentials, DTRSDescribeCredentials, DTRSListCredentials, DTRSRemoveCredentials, DTRSUpdateCredentials]:
        commands[command.name] = command

class EPUMClient(CeiClient):

    dashi_name = 'epu_management_service'
    name = 'epu'
    help = 'Control the EPU Management Service'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def describe_epu(self, name, caller=None):
        return self._connection.call(self.dashi_name, 'describe_epu', epu_name=name, caller=caller)

    def list_epus(self, caller=None):
        return self._connection.call(self.dashi_name, 'list_epus', caller=caller)

    def reconfigure_epu(self, name, config, caller=None):
        return self._connection.call(self.dashi_name, 'reconfigure_epu', epu_name=name, epu_config=config, caller=caller)

    def add_epu(self, name, config, caller=None):
        return self._connection.call(self.dashi_name, 'add_epu', epu_name=name, epu_config=config, caller=caller)

    def remove_epu(self, name, caller=None):
        return self._connection.call(self.dashi_name, 'remove_epu', epu_name=name, caller=caller)

    commands = {}
    for command in [EPUMDescribe, EPUMList, EPUMReconfigure, EPUMAdd, EPUMRemove]:
        commands[command.name] = command

class PDClient(CeiClient):

    dashi_name = 'processdispatcher'
    name = 'process'
    help = 'Control the Process Dispatcher Service'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def dispatch_process(self, upid, spec, subscribers, constraints, immediate=False):
        return self._connection.call(self.dashi_name, 'dispatch_process',
                                     upid=upid, spec=spec,
                                     subscribers=subscribers,
                                     constraints=constraints,
                                     immediate=immediate)

    def describe_process(self, upid):
        return self._connection.call(self.dashi_name, 'describe_process', upid=upid)

    def describe_processes(self):
        return self._connection.call(self.dashi_name, 'describe_processes')

    def terminate_process(self, upid):
        return self._connection.call(self.dashi_name, 'terminate_process', upid=upid)

    def restart_process(self, upid):
        return self._connection.call(self.dashi_name, 'restart_process', upid=upid)

    def dump(self):
        return self._connection.call(self.dashi_name, 'dump')

    commands = {}
    for command in [PDDispatch, PDDescribeProcess, PDDescribeProcesses, PDTerminateProcess, PDDump, PDRestartProcess]:
        commands[command.name] = command

class ProvisionerClient(CeiClient):

    dashi_name = 'provisioner'
    name = 'provisioner'
    help = 'Control the Provisioner Service'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def provision(self, deployable_type, site, allocation, vars, caller=None):
        launch_id =  str(uuid.uuid4())
        instance_ids = [str(uuid.uuid4())]

        return self._connection.call(self.dashi_name, 'provision',
                launch_id=launch_id, deployable_type=deployable_type,
                instance_ids=instance_ids,
                subscribers=[], site=site,
                allocation=allocation, vars=vars, caller=caller)

    def describe_nodes(self, nodes=None, caller=None):
        return self._connection.call(self.dashi_name, 'describe_nodes', nodes=nodes, caller=caller)

    def dump_state(self, nodes, force_subscribe):
        return self._connection.call(self.dashi_name, 'dump_state',
                nodes=nodes, force_subscribe=force_subscribe)

    def terminate_all(self):
        return self._connection.call(self.dashi_name, 'terminate_all')

    commands = {}
    for command in [ProvisionerDump, ProvisionerDescribeNodes, ProvisionerProvision, ProvisionerTerminateAll]:
        commands[command.name] = command

SERVICES = {}
for service in [DTRSDTClient, DTRSSiteClient, DTRSCredentialsClient, EPUMClient, PDClient, ProvisionerClient]:
    SERVICES[service.name] = service
