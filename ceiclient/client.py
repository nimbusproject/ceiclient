import connection
from commands import EPUMDescribe, EPUMList, EPUMReconfigure
from commands import PDDispatch, PDDescribeProcess, PDDescribeProcesses, PDTerminateProcess, PDDump

class CeiClient(object):

    def __init__(self):
        pass

class EPUMClient(CeiClient):

    dashi_name = 'epu_management_service'
    name = 'epu'
    help = 'Control the EPU Management Service'

    def __init__(self, connection):
        self._connection = connection

    def describe_epu(self, name):
        return self._connection.call(self.dashi_name, 'describe_epu', epu_name=name)

    def list_epus(self):
        return self._connection.call(self.dashi_name, 'list_epus')

    def reconfigure_epu(self, name, config):
        return self._connection.call(self.dashi_name, 'reconfigure_epu', epu_name=name, epu_config=config)

    commands = {}
    for command in [EPUMDescribe, EPUMList, EPUMReconfigure]:
        commands[command.name] = command

class PDClient(CeiClient):

    dashi_name = 'processdispatcher'
    name = 'process'
    help = 'Control the Process Dispatcher Service'

    def __init__(self, connection):
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

SERVICES = {}
for service in [EPUMClient, PDClient]:
    SERVICES[service.name] = service
