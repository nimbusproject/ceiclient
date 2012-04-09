import uuid
from ceiclient.commands import EPUMAdd, EPUMRemove

import connection
from commands import EPUMDescribe, EPUMList, EPUMReconfigure
from commands import PDDispatch, PDDescribeProcess, PDDescribeProcesses, PDTerminateProcess, PDDump
from commands import ProvisionerDump, ProvisionerDescribeNodes, ProvisionerProvision, ProvisionerTerminateAll

class CeiClient(object):

    def __init__(self, connection, dashi_name=None):
        pass

class EPUMClient(CeiClient):

    dashi_name = 'epu_management_service'
    name = 'epu'
    help = 'Control the EPU Management Service'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def describe_epu(self, name):
        return self._connection.call(self.dashi_name, 'describe_epu', epu_name=name)

    def list_epus(self):
        return self._connection.call(self.dashi_name, 'list_epus')

    def reconfigure_epu(self, name, config):
        return self._connection.call(self.dashi_name, 'reconfigure_epu', epu_name=name, epu_config=config)

    def add_epu(self, name, config):
        return self._connection.call(self.dashi_name, 'add_epu', epu_name=name, epu_config=config)

    def remove_epu(self, name):
        return self._connection.call(self.dashi_name, 'remove_epu', epu_name=name)

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

    def dump(self):
        return self._connection.call(self.dashi_name, 'dump')

    commands = {}
    for command in [PDDispatch, PDDescribeProcess, PDDescribeProcesses, PDTerminateProcess, PDDump]:
        commands[command.name] = command

class ProvisionerClient(CeiClient):

    dashi_name = 'provisioner'
    name = 'provisioner'
    help = 'Control the Provisioner Service'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def provision(self, deployable_type, site, allocation, vars):
        launch_id =  str(uuid.uuid4())
        instance_ids = [str(uuid.uuid4())]

        return self._connection.call(self.dashi_name, 'provision',
                launch_id=launch_id, deployable_type=deployable_type,
                instance_ids=instance_ids,
                subscribers=[], site=site,
                allocation=allocation, vars=vars)

    def describe_nodes(self, nodes=None):
        return self._connection.call(self.dashi_name, 'describe_nodes', nodes=nodes)

    def dump_state(self, nodes, force_subscribe):
        return self._connection.call(self.dashi_name, 'dump_state',
                nodes=nodes, force_subscribe=force_subscribe)

    def terminate_all(self):
        return self._connection.call(self.dashi_name, 'terminate_all')

    commands = {}
    for command in [ProvisionerDump, ProvisionerDescribeNodes, ProvisionerProvision, ProvisionerTerminateAll]:
        commands[command.name] = command

SERVICES = {}
for service in [EPUMClient, PDClient, ProvisionerClient]:
    SERVICES[service.name] = service
