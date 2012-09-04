import sys
import uuid

from commands import DTRSAddDT, DTRSDescribeDT, DTRSListDT, DTRSRemoveDT, DTRSUpdateDt
from commands import DTRSAddSite, DTRSDescribeSite, DTRSListSites, DTRSRemoveSite, DTRSUpdateSite
from commands import DTRSAddCredentials, DTRSDescribeCredentials, DTRSListCredentials, DTRSRemoveCredentials, DTRSUpdateCredentials
from commands import EPUMAdd, EPUMDescribe, EPUMList, EPUMReconfigure, EPUMRemove
from commands import EPUMAddDefinition, EPUMDescribeDefinition, EPUMListDefinitions, EPUMRemoveDefinition, EPUMUpdateDefinition
from commands import PDSchedule, PDDescribeProcess, PDDescribeProcesses, PDTerminateProcess, PDDump, PDRestartProcess, PDWaitProcess
from commands import PDCreateProcessDefinition, PDDescribeProcessDefinition, PDUpdateProcessDefinition, PDRemoveProcessDefinition, PDListProcessDefinitions
from commands import PyonPDCreateProcessDefinition, PyonPDUpdateProcessDefinition, PyonPDReadProcessDefinition, PyonPDDeleteProcessDefinition, PyonPDListProcessDefinitions
from commands import PyonPDAssociateExecutionEngine, PyonPDDissociateExecutionEngine
from commands import PyonPDCreateProcess, PyonPDScheduleProcess, PyonPDCancelProcess, PyonPDReadProcess, PyonPDListProcesses, PyonPDWaitProcess
from commands import PyonHAStatus, PyonHAReconfigurePolicy
from commands import ProvisionerDump, ProvisionerDescribeNodes, ProvisionerProvision, ProvisionerTerminateAll


class CeiClient(object):

    def __init__(self, connection, dashi_name=None):
        pass


class PyonCeiClientException(BaseException):
    pass


class PyonCeiClient(object):

    def __init__(self, connection, **kwargs):
        pass

    def _format_pyon_dict(self, original, key=None, type_=None, definition_type=1):
        """Take a dictionary of parameters, and add Pyon Boilerplate
        """

        if type_ is None:
            raise PyonCeiClientException("You must supply a type_")

        changed = original
        if key is not None:
            try:
                to_change = original[key]
            except KeyError:
                msg = "You specified a key, but it isn't in the dict provided?"
                raise PyonCeiClientException(msg)
        else:
            to_change = original

        to_change['type_'] = type_

        # Pyon Boilerplate
        to_change['lcstate'] = 'DRAFT_PRIVATE'
        if definition_type is not None:
            to_change['definition_type'] = 1
        to_change['description'] = ''
        to_change['version'] = ''
        to_change['arguments'] = []
        to_change['ts_updated'] = ''
        to_change['ts_created'] = ''

        return changed

    def _strip_pyon_attrs(self, original, key=None):
        """Strip pyon metadata attributes
        """

        def del_if_present(_dict, key):
            try:
                del(_dict[key])
            except KeyError:
                pass
            except TypeError:
                pass

        changed = original
        if key is not None:
            try:
                to_change = original[key]
            except KeyError:
                msg = "You specified a key, but it isn't in the dict provided?"
                raise PyonCeiClientException(msg)
        else:
            to_change = original

        del_if_present(to_change, 'type_')
        del_if_present(to_change, 'lcstate')
        del_if_present(to_change, 'definition_type')
        del_if_present(to_change, 'description')
        del_if_present(to_change, 'version')
        del_if_present(to_change, 'arguments')
        del_if_present(to_change, 'ts_updated')
        del_if_present(to_change, 'ts_created')
        del_if_present(to_change, '_id')
        del_if_present(to_change, '_rev')

        return changed


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
    for command in [DTRSAddCredentials, DTRSDescribeCredentials, DTRSListCredentials, DTRSRemoveCredentials, DTRSUpdateCredentials]:
        commands[command.name] = command


class EPUMClient(CeiClient):

    dashi_name = 'epu_management_service'
    name = 'domain'
    help = 'Control domains in the EPU Management Service'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def describe_domain(self, name, caller=None):
        return self._connection.call(self.dashi_name, 'describe_domain', domain_id=name, caller=caller)

    def list_domains(self, caller=None):
        return self._connection.call(self.dashi_name, 'list_domains', caller=caller)

    def reconfigure_domain(self, name, config, caller=None):
        return self._connection.call(self.dashi_name, 'reconfigure_domain', domain_id=name, config=config, caller=caller)

    def add_domain(self, name, definition_id, config, caller=None):
        return self._connection.call(self.dashi_name, 'add_domain', domain_id=name, definition_id=definition_id, config=config, caller=caller)

    def remove_domain(self, name, caller=None):
        return self._connection.call(self.dashi_name, 'remove_domain', domain_id=name, caller=caller)

    commands = {}
    for command in [EPUMDescribe, EPUMList, EPUMReconfigure, EPUMAdd, EPUMRemove]:
        commands[command.name] = command


class EPUMDefinitionClient(CeiClient):

    dashi_name = 'epu_management_service'
    name = 'definition'
    help = 'Control domain definitions in the EPU Management Service'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def describe_domain_definition(self, name):
        return self._connection.call(self.dashi_name, 'describe_domain_definition', definition_id=name)

    def list_domain_definitions(self):
        return self._connection.call(self.dashi_name, 'list_domain_definitions')

    def update_domain_definition(self, name, definition):
        return self._connection.call(self.dashi_name, 'update_domain_definition', definition_id=name, definition=definition)

    def add_domain_definition(self, name, definition):
        return self._connection.call(self.dashi_name, 'add_domain_definition', definition_id=name, definition=definition)

    def remove_domain_definition(self, name):
        return self._connection.call(self.dashi_name, 'remove_domain_definition', definition_id=name)

    commands = {}
    for command in [EPUMDescribeDefinition, EPUMListDefinitions, EPUMUpdateDefinition, EPUMAddDefinition, EPUMRemoveDefinition]:
        commands[command.name] = command

class PDProcessDefinitionClient(CeiClient):

    dashi_name = 'processdispatcher'
    name = 'process-definition'
    help = 'Control the Process Dispatcher Service'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def create_process_definition(self, process_definition=None, process_definition_id=None):
        if process_definition is None:
            msg = "You must provide a process defintion"
            sys.exit(msg)

        if process_definition_id is None:
            msg = "You must provide a process defintion id"
            sys.exit(msg)

        executable = process_definition.get('executable')
        definition_type = process_definition.get('definition_type')
        name = process_definition.get('name')
        description = process_definition.get('description')
        args = dict(definition_id=process_definition_id, definition_type=definition_type,
            executable=executable, name=name, description=description)
        # TODO: what is definition_type?
        return self._connection.call(self.dashi_name, 'create_definition', args=args)

    def update_process_definition(self, process_definition=None, process_definition_id=None):
        if process_definition is None:
            msg = "You must provide a process defintion"
            sys.exit(msg)

        if process_definition_id is None:
            msg = "You must provide a process defintion id"
            sys.exit(msg)

        executable = process_definition.get('executable')
        definition_type = process_definition.get('type')
        name = process_definition.get('name')
        description = process_definition.get('description')
        # TODO: what is definition_type?
        return self._connection.call(self.dashi_name, 'update_definition',
                definition_id=process_definition_id, executable=executable,
                definition_type=definition_type, name=name, description=description)

    def describe_process_definition(self, process_definition_id=''):
        return self._connection.call(self.dashi_name, 'describe_definition', definition_id=process_definition_id)

    def remove_process_definition(self, process_definition_id=''):
        return self._connection.call(self.dashi_name, 'remove_definition', definition_id=process_definition_id)

    def list_process_definitions(self):
        return self._connection.call(self.dashi_name, 'list_definitions')

    commands = {}
    for command in [PDCreateProcessDefinition, PDUpdateProcessDefinition, PDDescribeProcessDefinition, PDRemoveProcessDefinition, PDListProcessDefinitions]:
        commands[command.name] = command

class PDClient(CeiClient):

    dashi_name = 'processdispatcher'
    name = 'process'
    help = 'Control the Process Dispatcher Service'

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self._connection = connection

    def schedule_process(self, upid, process_definition_id, subscribers, constraints):
        args = dict(upid=upid, definition_id=process_definition_id,
                       subscribers=subscribers, constraints=constraints)
        return self._connection.call(self.dashi_name, 'schedule_process',
                                     args=args)

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
    for command in [PDSchedule, PDDescribeProcess, PDDescribeProcesses, PDTerminateProcess, PDDump, PDRestartProcess, PDWaitProcess]:
        commands[command.name] = command


class PyonPDProcessDefinitionClient(PyonCeiClient):

    service_name = 'process_dispatcher'
    name = 'process-definition'
    help = 'Control the Pyon Process Dispatcher Service'

    def __init__(self, connection, **kwargs):
        self._connection = connection

    def create_process_definition(self, process_definition=None, process_definition_id=None):
        if process_definition is None:
            process_definition = {}

        message = {}
        if process_definition_id is not None:
            message['process_definition_id'] = process_definition_id
        message['process_definition'] = self._format_pyon_dict(process_definition, type_='ProcessDefinition')
        if process_definition_id is not None:
            message['process_definition_id'] = process_definition_id
        return self._connection.call(self.service_name, 'create_process_definition', **message)

    def update_process_definition(self, process_definition=None, process_definition_id=None):
        if process_definition is None:
            process_definition = {}

        message = {}
        if process_definition_id is not None:
            message['process_definition_id'] = process_definition_id
        message['process_definition'] = self._format_pyon_dict(process_definition, type_='ProcessDefinition')
        if process_definition_id is not None:
            message['process_definition_id'] = process_definition_id
        return self._connection.call(self.service_name, 'update_process_definition', **message)

    def read_process_definition(self, process_definition_id=''):
        message = {'process_definition_id': process_definition_id}
        return self._strip_pyon_attrs(self._connection.call(self.service_name, 'read_process_definition', **message))

    def delete_process_definition(self, process_definition_id=''):
        message = {'process_definition_id': process_definition_id}
        return self._connection.call(self.service_name, 'delete_process_definition', **message)

    def list_process_definitions(self):
        message = {}
        return self._connection.call(self.service_name, 'list_process_definitions', **message)

    commands = {}
    for command in [PyonPDCreateProcessDefinition, PyonPDUpdateProcessDefinition, PyonPDReadProcessDefinition, PyonPDDeleteProcessDefinition, PyonPDListProcessDefinitions]:
        commands[command.name] = command


class PyonPDExecutionEngineClient(PyonCeiClient):

    service_name = 'process_dispatcher'
    name = 'execution-engine'
    help = 'Control the Pyon Process Dispatcher Service'

    def __init__(self, connection, **kwargs):
        self._connection = connection

    def associate_execution_engine(self, process_definition_id='', execution_engine_definition_id=''):
        message = {
            'process_definition_id': process_definition_id,
            'execution_engine_definition_id': execution_engine_definition_id
        }
        return self._connection.call(self.service_name, 'associate_execution_engine', **message)

    def dissociate_execution_engine(self, process_definition_id='', execution_engine_definition_id=''):
        message = {
            'process_definition_id': process_definition_id,
            'execution_engine_definition_id': execution_engine_definition_id
        }
        return self._connection.call(self.service_name, 'associate_execution_engine', **message)

    commands = {}
    for command in [PyonPDAssociateExecutionEngine, PyonPDDissociateExecutionEngine]:
        commands[command.name] = command


class PyonPDProcessClient(PyonCeiClient):

    service_name = 'process_dispatcher'
    name = 'process'
    help = 'Control the Pyon Process Dispatcher Service'

    def __init__(self, connection, **kwargs):
        self._connection = connection

    def create_process(self, process_definition_id=''):
        message = {'process_definition_id': process_definition_id}
        return self._connection.call(self.service_name, 'create_process', **message)

    def schedule_process(self, process_definition_id='', schedule=None, configuration=None, process_id=''):
        if schedule is None:
            msg = "You must provide a process schedule"
            sys.exit(msg)
        if schedule.get('target') is None:
            msg = "You must provide a schedule target"
            sys.exit(msg)

        if configuration is None:
            configuration = {}

        message = {}
        message['process_definition_id'] = process_definition_id
        message['schedule'] = schedule
        message['schedule']['type_'] = 'ProcessSchedule'
        message['schedule']['target'] = message['schedule'].get('target', {})
        message['schedule']['target']['type_'] = 'ProcessTarget'
        message['configuration'] = configuration
        message['process_id'] = process_definition_id

        return self._connection.call(self.service_name, 'schedule_process', **message)

    def cancel_process(self, process_id=''):
        message = {'process_id': process_id}
        return self._connection.call(self.service_name, 'cancel_process', **message)

    def read_process(self, process_id=''):
        message = {'process_id': process_id}
        return self._strip_pyon_attrs(self._connection.call(self.service_name, 'read_process', **message))

    def list_processes(self):
        message = {}
        response = self._connection.call(self.service_name, 'list_processes', **message)
        cleaned_list = []
        for pyon_proc in response:
            cleaned_list.append(self._strip_pyon_attrs(pyon_proc))
        return cleaned_list

    commands = {}
    for command in [PyonPDCreateProcess, PyonPDScheduleProcess, PyonPDCancelProcess, PyonPDReadProcess, PyonPDListProcesses, PyonPDWaitProcess]:
        commands[command.name] = command


class PyonHAAgentClient(PyonCeiClient):
    service_name = 'high_availability_agent'
    name = 'ha'
    help = 'Control the Pyon High Availability Agent'

    def __init__(self, connection, service_name=None, **kwargs):
        if service_name is not None:
            self.service_name = service_name
        self._connection = connection

    def status(self):
        message = {}
        return self._connection.call(self.service_name, 'rcmd_status', **message)

    def reconfigure_policy(self, new_policy=None):
        message = {'new_policy': new_policy}
        return self._connection.call(self.service_name, 'rcmd_reconfigure_policy', **message)

    commands = {}
    for command in [PyonHAStatus, PyonHAReconfigurePolicy]:
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
        launch_id = str(uuid.uuid4())
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

DASHI_SERVICES = {}
for service in [DTRSDTClient, DTRSSiteClient, DTRSCredentialsClient,
        EPUMClient, EPUMDefinitionClient, PDClient, PDProcessDefinitionClient,
        ProvisionerClient, ]:
    DASHI_SERVICES[service.name] = service

PYON_SERVICES = {}
for service in [PyonPDProcessDefinitionClient, PyonPDProcessClient,
        PyonPDExecutionEngineClient, PyonHAAgentClient]:
    PYON_SERVICES[service.name] = service
