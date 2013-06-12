import uuid

from ceiclient.exception import CeiClientError


class DashiCeiClient(object):

    def __init__(self, connection, dashi_name=None):
        if dashi_name:
            self.dashi_name = dashi_name
        self.connection = connection


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


class DTRSClient(DashiCeiClient):

    dashi_name = 'dtrs'

    def add_dt(self, caller, dt_name, dt_definition):
        return self.connection.call(self.dashi_name, 'add_dt', caller=caller,
                                    dt_name=dt_name, dt_definition=dt_definition)

    def describe_dt(self, caller, dt_name):
        return self.connection.call(self.dashi_name, 'describe_dt', caller=caller, dt_name=dt_name)

    def list_dts(self, caller):
        return self.connection.call(self.dashi_name, 'list_dts', caller=caller)

    def remove_dt(self, caller, dt_name):
        return self.connection.call(self.dashi_name, 'remove_dt', caller=caller, dt_name=dt_name)

    def update_dt(self, caller, dt_name, dt_definition):
        return self.connection.call(self.dashi_name, 'update_dt', caller=caller,
                                    dt_name=dt_name, dt_definition=dt_definition)

    def add_site(self, caller, site_name, site_definition):
        return self.connection.call(self.dashi_name, 'add_site', caller=caller, site_name=site_name, site_definition=site_definition)

    def describe_site(self, caller, site_name):
        return self.connection.call(self.dashi_name, 'describe_site', caller=caller, site_name=site_name)

    def list_sites(self, caller):
        return self.connection.call(self.dashi_name, 'list_sites', caller=caller)

    def remove_site(self, caller, site_name):
        return self.connection.call(self.dashi_name, 'remove_site', caller=caller, site_name=site_name)

    def update_site(self, caller, site_name, site_definition):
        return self.connection.call(self.dashi_name, 'update_site', caller=caller,
                                    site_name=site_name, site_definition=site_definition)

    def add_credentials(self, caller, site_name, site_credentials, credential_type='site'):
        return self.connection.call(self.dashi_name, 'add_credentials', caller=caller,
                                    site_name=site_name, site_credentials=site_credentials,
                                    credential_type=credential_type)

    def describe_credentials(self, caller, site_name, credential_type='site'):
        return self.connection.call(self.dashi_name, 'describe_credentials',
                                    caller=caller, site_name=site_name,
                                    credential_type=credential_type)

    def list_credentials(self, caller, credential_type='site'):
        return self.connection.call(self.dashi_name, 'list_credentials',
                                    caller=caller,
                                    credential_type=credential_type)

    def remove_credentials(self, caller, site_name, credential_type='site'):
        return self.connection.call(self.dashi_name, 'remove_credentials',
                                    caller=caller, site_name=site_name,
                                    credential_type=credential_type)

    def update_credentials(self, caller, site_name, site_credentials, credential_type='site'):
        return self.connection.call(self.dashi_name, 'update_credentials',
                                    caller=caller, site_name=site_name,
                                    site_credentials=site_credentials,
                                    credential_type=credential_type)


class EPUMClient(DashiCeiClient):

    dashi_name = 'epu_management_service'

    def describe_domain(self, name, caller=None):
        return self.connection.call(self.dashi_name, 'describe_domain', domain_id=name, caller=caller)

    def list_domains(self, caller=None):
        return self.connection.call(self.dashi_name, 'list_domains', caller=caller)

    def reconfigure_domain(self, name, config, caller=None):
        return self.connection.call(self.dashi_name, 'reconfigure_domain', domain_id=name, config=config, caller=caller)

    def add_domain(self, name, definition_id, config, caller=None):
        return self.connection.call(self.dashi_name, 'add_domain', domain_id=name,
                                    definition_id=definition_id, config=config, caller=caller)

    def remove_domain(self, name, caller=None):
        return self.connection.call(self.dashi_name, 'remove_domain', domain_id=name, caller=caller)

    def describe_domain_definition(self, name):
        return self.connection.call(self.dashi_name, 'describe_domain_definition', definition_id=name)

    def list_domain_definitions(self):
        return self.connection.call(self.dashi_name, 'list_domain_definitions')

    def update_domain_definition(self, name, definition):
        return self.connection.call(self.dashi_name, 'update_domain_definition',
                                    definition_id=name, definition=definition)

    def add_domain_definition(self, name, definition):
        return self.connection.call(self.dashi_name, 'add_domain_definition', definition_id=name, definition=definition)

    def remove_domain_definition(self, name):
        return self.connection.call(self.dashi_name, 'remove_domain_definition', definition_id=name)


class PDClient(DashiCeiClient):

    dashi_name = 'process_dispatcher'

    def set_system_boot(self, system_boot):
        self.connection.call(self.dashi_name, 'set_system_boot', system_boot=system_boot)

    def create_process_definition(self, process_definition=None, process_definition_id=None):
        if process_definition is None:
            raise CeiClientError("You must provide a process defintion")

        if process_definition_id is None:
            raise CeiClientError("You must provide a process defintion id")

        executable = process_definition.get('executable')
        definition_type = process_definition.get('definition_type')
        name = process_definition.get('name')
        description = process_definition.get('description')
        args = dict(definition_id=process_definition_id, definition_type=definition_type,
            executable=executable, name=name, description=description)
        # TODO: what is definition_type?
        return self.connection.call(self.dashi_name, 'create_definition', args=args)

    def update_process_definition(self, process_definition=None, process_definition_id=None):
        if process_definition is None:
            raise CeiClientError("You must provide a process defintion")

        if process_definition_id is None:
            raise CeiClientError("You must provide a process defintion id")

        executable = process_definition.get('executable')
        definition_type = process_definition.get('type')
        name = process_definition.get('name')
        description = process_definition.get('description')
        args = dict(definition_id=process_definition_id, definition_type=definition_type,
            executable=executable, name=name, description=description)
        # TODO: what is definition_type?
        return self.connection.call(self.dashi_name, 'update_definition', args=args)

    def describe_process_definition(self, process_definition_id='', process_definition_name=''):
        kwargs = {}
        if process_definition_id:
            kwargs['definition_id'] = process_definition_id
        if process_definition_name:
            kwargs['definition_name'] = process_definition_name

        return self.connection.call(self.dashi_name, 'describe_definition', **kwargs)

    def remove_process_definition(self, process_definition_id=''):
        return self.connection.call(self.dashi_name, 'remove_definition', definition_id=process_definition_id)

    def list_process_definitions(self):
        return self.connection.call(self.dashi_name, 'list_definitions')

    def schedule_process(self, upid, process_definition_id=None,
            process_definition_name=None, configuration=None,
            subscribers=None, constraints=None, queueing_mode=None,
            restart_mode=None, execution_engine_id=None, node_exclusive=None):
        args = dict(upid=upid, definition_id=process_definition_id,
               subscribers=subscribers, constraints=constraints,
               configuration=configuration, queueing_mode=queueing_mode,
               restart_mode=restart_mode, execution_engine_id=execution_engine_id,
               node_exclusive=node_exclusive)

        # only include this arg if it is provided
        if process_definition_name is not None:
            args['definition_name'] = process_definition_name

        return self.connection.call(self.dashi_name, 'schedule_process',
                                     args=args)

    def describe_process(self, upid):
        return self.connection.call(self.dashi_name, 'describe_process', upid=upid)

    def describe_processes(self):
        return self.connection.call(self.dashi_name, 'describe_processes')

    def terminate_process(self, upid):
        return self.connection.call(self.dashi_name, 'terminate_process', upid=upid)

    def restart_process(self, upid):
        return self.connection.call(self.dashi_name, 'restart_process', upid=upid)

    def dump(self):
        return self.connection.call(self.dashi_name, 'dump')


class HAAgentClient(DashiCeiClient):

    dashi_name = 'ha_agent'  # this will almost always be overridden

    def status(self):
        return self.connection.call(self.dashi_name, 'status')

    def dump(self):
        return self.connection.call(self.dashi_name, 'dump')

    def reconfigure_policy(self, new_policy_params, new_policy=None):
        message = {'new_policy_params': new_policy_params}
        if new_policy is not None:
            message['new_policy_name'] = new_policy
        return self.connection.call(self.dashi_name, 'reconfigure_policy', **message)


class PyonPDClient(PyonCeiClient):

    service_name = 'process_dispatcher'

    def __init__(self, connection, **kwargs):
        self.connection = connection

    def create_process_definition(self, process_definition=None, process_definition_id=None):
        if process_definition is None:
            process_definition = {}

        message = {}
        if process_definition_id is not None:
            message['process_definition_id'] = process_definition_id
        message['process_definition'] = self._format_pyon_dict(process_definition, type_='ProcessDefinition')
        if process_definition_id is not None:
            message['process_definition_id'] = process_definition_id
        return self.connection.call(self.service_name, 'create_process_definition', **message)

    def update_process_definition(self, process_definition=None, process_definition_id=None):
        if process_definition is None:
            process_definition = {}

        message = {}
        if process_definition_id is not None:
            message['process_definition_id'] = process_definition_id
        message['process_definition'] = self._format_pyon_dict(process_definition, type_='ProcessDefinition')
        if process_definition_id is not None:
            message['process_definition_id'] = process_definition_id
        return self.connection.call(self.service_name, 'update_process_definition', **message)

    def read_process_definition(self, process_definition_id=''):
        message = {'process_definition_id': process_definition_id}
        return self._strip_pyon_attrs(self.connection.call(self.service_name, 'read_process_definition', **message))

    def delete_process_definition(self, process_definition_id=''):
        message = {'process_definition_id': process_definition_id}
        return self.connection.call(self.service_name, 'delete_process_definition', **message)

    def list_process_definitions(self):
        message = {}
        return self.connection.call(self.service_name, 'list_process_definitions', **message)

    def associate_execution_engine(self, process_definition_id='', execution_engine_definition_id=''):
        message = {
            'process_definition_id': process_definition_id,
            'execution_engine_definition_id': execution_engine_definition_id
        }
        return self.connection.call(self.service_name, 'associate_execution_engine', **message)

    def dissociate_execution_engine(self, process_definition_id='', execution_engine_definition_id=''):
        message = {
            'process_definition_id': process_definition_id,
            'execution_engine_definition_id': execution_engine_definition_id
        }
        return self.connection.call(self.service_name, 'associate_execution_engine', **message)

    def create_process(self, process_definition_id=''):
        message = {'process_definition_id': process_definition_id}
        return self.connection.call(self.service_name, 'create_process', **message)

    def schedule_process(self, process_definition_id='', schedule=None, configuration=None, process_id=''):
        if schedule is None:
            raise CeiClientError("You must provide a process schedule")
        if schedule.get('target') is None:
            raise CeiClientError("You must provide a schedule target")

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

        return self.connection.call(self.service_name, 'schedule_process', **message)

    def cancel_process(self, process_id=''):
        message = {'process_id': process_id}
        return self.connection.call(self.service_name, 'cancel_process', **message)

    def read_process(self, process_id=''):
        message = {'process_id': process_id}
        return self._strip_pyon_attrs(self.connection.call(self.service_name, 'read_process', **message))

    def list_processes(self):
        message = {}
        response = self.connection.call(self.service_name, 'list_processes', **message)
        cleaned_list = []
        for pyon_proc in response:
            cleaned_list.append(self._strip_pyon_attrs(pyon_proc))
        return cleaned_list


class PyonHAAgentClient(PyonCeiClient):

    service_name = 'high_availability_agent'

    def __init__(self, connection, service_name=None, **kwargs):
        if service_name is not None:
            self.service_name = service_name
        self.connection = connection

    def status(self):
        message = {}
        return self.connection.call(self.service_name, 'rcmd_status', **message)

    def reconfigure_policy(self, new_policy_params, new_policy=None):
        message = {'new_policy_params': new_policy_params}
        if new_policy is not None:
            message['new_policy_name'] = new_policy

        return self.connection.call(self.service_name, 'rcmd_reconfigure_policy', **message)


class ProvisionerClient(DashiCeiClient):

    dashi_name = 'provisioner'

    def provision(self, deployable_type, site, allocation, vars, caller=None):
        launch_id = str(uuid.uuid4())
        instance_ids = [str(uuid.uuid4())]

        return self.connection.call(self.dashi_name, 'provision',
            launch_id=launch_id, deployable_type=deployable_type,
            instance_ids=instance_ids,
            subscribers=[], site=site,
            allocation=allocation, vars=vars, caller=caller)

    def describe_nodes(self, nodes=None, caller=None):
        return self.connection.call(self.dashi_name, 'describe_nodes', nodes=nodes, caller=caller)

    def dump_state(self, nodes, force_subscribe):
        return self.connection.call(self.dashi_name, 'dump_state',
                nodes=nodes, force_subscribe=force_subscribe)

    def terminate_all(self):
        return self.connection.call(self.dashi_name, 'terminate_all')
