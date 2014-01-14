import re
import yaml
import time
import uuid

from jinja2 import Template
from dashi.exceptions import NotFoundError, WriteConflictError, BadRequestError

from client import DTRSClient, EPUMClient, HAAgentClient, PDClient, \
    ProvisionerClient, PyonPDClient, PyonHAAgentClient, PyonHTTPPDClient, \
    PyonHTTPHAAgentClient
from exception import CeiClientError
from common import safe_print, safe_pprint

# Classes for different kinds of output


class CeiCommand(object):

    def __init__(self, subparsers):
        pass

    @staticmethod
    def output(result):
        safe_pprint(result)

    @staticmethod
    def details(result):
        safe_pprint(result)


class CeiCommandPrintOutput(CeiCommand):

    @staticmethod
    def output(result):
        safe_print(result)

    @staticmethod
    def details(result):
        safe_print(result)


class CeiCommandPrintListOutput(CeiCommand):

    @staticmethod
    def output(result):
        for element in result:
            safe_print(element)

    @staticmethod
    def details(result):
        for element in result:
            safe_print(element)


class DTRSAddDT(CeiCommandPrintOutput):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('dt_name', action='store', help='The name of the DT to be added.')
        parser.add_argument('--definition', dest='dt_def_file', action='store', help='Set the DT definition to use.')
        parser.add_argument('--force', '-f', help="Update the DT if it exists", action='store_true')

    @staticmethod
    def execute(client, opts):
        if opts.dt_def_file is None:
            raise CeiClientError("The --definition argument is missing")

        try:
            with open(opts.dt_def_file) as f:
                dt_def = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading DT definition file %s: %s" % (opts.dt_def_file, e))

        try:
            client.add_dt(opts.caller, opts.dt_name, dt_def)
        except WriteConflictError:
            if opts.force:
                client.update_dt(opts.caller, opts.dt_name, dt_def)
                return "Updated DT %s for user %s" % (opts.dt_name, opts.caller)
            else:
                raise

        return "Added DT %s for user %s" % (opts.dt_name, opts.caller)


class DTRSDescribeDT(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('dt_name', action='store', help='The DT to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_dt(opts.caller, opts.dt_name)


class DTRSListDT(CeiCommandPrintListOutput):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_dts(caller=opts.caller)


class DTRSRemoveDT(CeiCommandPrintOutput):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('dt_name', action='store', help='The DT to remove')

    @staticmethod
    def execute(client, opts):
        client.remove_dt(opts.caller, opts.dt_name)
        return "Removed DT %s for user %s" % (opts.dt_name, opts.caller)


class DTRSUpdateDt(CeiCommandPrintOutput):

    name = 'update'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('dt_name', action='store', help='The name of the DT to be updated.')
        parser.add_argument('--definition', dest='dt_def_file', action='store', help='The DT definition to use.')

    @staticmethod
    def execute(client, opts):
        if opts.dt_def_file is None:
            raise CeiClientError("The --definition argument is missing")

        try:
            with open(opts.dt_def_file) as f:
                dt_def = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading DT definition file %s: %s" % (opts.dt_def_file, e))

        client.update_dt(opts.caller, opts.dt_name, dt_def)
        return "Updated DT %s for user %s" % (opts.dt_name, opts.caller)


class DTRSAddSite(CeiCommandPrintOutput):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The name of the site to be added.')
        parser.add_argument(
            '--definition', dest='site_def_file', action='store',
            help='Set the site definition to use.')
        parser.add_argument('--force', '-f', help="Update the site if it exists", action='store_true')

    @staticmethod
    def execute(client, opts):
        if opts.site_def_file is None:
            raise CeiClientError("The --definition argument is missing")

        try:
            with open(opts.site_def_file) as f:
                site_def = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading site definition file %s: %s" % (opts.site_def_file, e))

        try:
            client.add_site(opts.caller, opts.site_name, site_def)
        except WriteConflictError:
            if opts.force:
                client.update_site(opts.caller, opts.site_name, site_def)
                return "Updated site %s" % opts.site_name
            else:
                raise
        return "Added site %s" % opts.site_name


class DTRSDescribeSite(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The site to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_site(opts.caller, opts.site_name)


class DTRSListSites(CeiCommandPrintListOutput):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_sites(opts.caller)


class DTRSRemoveSite(CeiCommandPrintOutput):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The site to remove')

    @staticmethod
    def execute(client, opts):
        client.remove_site(opts.caller, opts.site_name)
        return "Removed site %s" % opts.site_name


class DTRSUpdateSite(CeiCommandPrintOutput):

    name = 'update'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The name of the site to be updated.')
        parser.add_argument('--definition', dest='site_def_file', action='store', help='The site definition to use.')

    @staticmethod
    def execute(client, opts):
        if opts.site_def_file is None:
            raise CeiClientError("The --definition argument is missing")

        try:
            with open(opts.site_def_file) as f:
                site_def = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading site definition file %s: %s" % (opts.site_def_file, e))

        client.update_site(opts.caller, opts.site_name, site_def)
        return "Updated site %s" % opts.site_name


class DTRSAddCredentials(CeiCommandPrintOutput):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('name', action='store', help='The name of the credential to be added.')
        parser.add_argument(
            '--definition', dest='credentials_def_file', action='store',
            help='Set the credentials definition to use.')
        parser.add_argument('--type', action='store', help='Credential type to store. default is site')
        parser.add_argument('--force', '-f', help="Update the credential if it exists", action='store_true')

    @staticmethod
    def execute(client, opts):
        if opts.credentials_def_file is None:
            raise CeiClientError("The --definition argument is missing")

        try:
            with open(opts.credentials_def_file) as f:
                credentials_def = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading credentials definition file %s: %s" % (opts.credentials_def_file, e))

        kwargs = {'credential_type': opts.type} if opts.type else {}

        try:
            client.add_credentials(opts.caller, opts.name, credentials_def, **kwargs)
        except WriteConflictError:
            if opts.force:
                client.update_credentials(opts.caller, opts.name, credentials_def, **kwargs)
                return "Updated credentials of site %s for user %s" % (opts.name, opts.caller)
            else:
                raise
        return "Added credentials of site %s for user %s" % (opts.name, opts.caller)


class DTRSDescribeCredentials(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('name', action='store', help='The credential to describe')
        parser.add_argument('--type', action='store', help='Credential type to query. default is site')

    @staticmethod
    def execute(client, opts):
        kwargs = {'credential_type': opts.type} if opts.type else {}
        return client.describe_credentials(opts.caller, opts.name, **kwargs)


class DTRSListCredentials(CeiCommandPrintListOutput):

    name = 'list'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('--type', action='store', help='Credential type to query. default is site')

    @staticmethod
    def execute(client, opts):
        kwargs = {'credential_type': opts.type} if opts.type else {}
        return client.list_credentials(caller=opts.caller, **kwargs)


class DTRSRemoveCredentials(CeiCommandPrintOutput):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('name', action='store', help='The credential to remove')
        parser.add_argument('--type', action='store', help='Credential type to query. default is site')

    @staticmethod
    def execute(client, opts):
        kwargs = {'credential_type': opts.type} if opts.type else {}
        client.remove_credentials(opts.caller, opts.name, **kwargs)
        return "Removed credentials of site %s for user %s" % (opts.name, opts.caller)


class DTRSUpdateCredentials(CeiCommandPrintOutput):

    name = 'update'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('name', action='store', help='The name of the credential to update.')
        parser.add_argument('--definition', dest='credentials_def_file', action='store',
            help='The credentials definition to use.')
        parser.add_argument('--type', action='store', help='Credential type to update. default is site')

    @staticmethod
    def execute(client, opts):
        if opts.credentials_def_file is None:
            raise CeiClientError("The --definition argument is missing")

        try:
            with open(opts.credentials_def_file) as f:
                credentials_def = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading credentials definition file %s: %s" % (opts.credentials_def_file, e))

        kwargs = {'credential_type': opts.type} if opts.type else {}

        client.update_credentials(opts.caller, opts.name, credentials_def, **kwargs)
        return "Updated credentials of site %s for user %s" % (opts.name, opts.caller)


class AddDomain(CeiCommand):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('domain_id', action='store', help='The name of the domain to be added.')
        parser.add_argument('--definition', dest='definition_id', action='store',
            help='The name of the domain definition to use.')
        parser.add_argument('--conf', dest='de_conf', action='store',
            help='Additional configuration for the decision engine.')

    @staticmethod
    def execute(client, opts):
        try:
            with open(opts.de_conf, 'r') as f:
                conf = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading decision engine configuration file %s: %s" % (opts.de_conf, e))

        return client.add_domain(opts.domain_id, opts.definition_id, conf, caller=opts.caller)


class RemoveDomain(CeiCommand):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('domain_id', action='store', help='The domain to remove')

    @staticmethod
    def execute(client, opts):
        return client.remove_domain(opts.domain_id, caller=opts.caller)


class DescribeDomain(CeiCommand):

    name = 'describe'
    output_template = '''Name:                    {{ result.name }}
Engine configuration:    EPU worker type = {{ result.config.engine_conf.epuworker_type }}
                         Preserve N      = {{ result.config.engine_conf.preserve_n }}
General:                 Engine class    = {{ result.config.general.engine_class }}
Health:                  Monitor health  = {{result.config.health.monitor_health}}'''

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('domain_id', action='store', help='The domain to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_domain(opts.domain_id, caller=opts.caller)

    @staticmethod
    def output(result):
        template = Template(DescribeDomain.output_template)
        safe_print(template.render(result=result))


class ListDomains(CeiCommandPrintListOutput):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_domains(caller=opts.caller)


class ReconfigureDomain(CeiCommand):

    name = 'reconfigure'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('domain_id', action='store', help='The domain to reconfigure')
        parser.add_argument('--bool', dest='updated_kv_bool', action='append',
            help='Key to modify in the domain configuration with a boolean value')
        parser.add_argument('--int', dest='updated_kv_int', action='append',
            help='Key to modify in the domain configuration with a integer value')
        parser.add_argument('--string', dest='updated_kv_string', action='append',
            help='Key to modify in the domain configuration with a string value')

    @staticmethod
    def format_reconfigure(bool_reconfs=[], int_reconfs=[], string_reconfs=[]):
        h = {}
        r = '([a-zA-Z_0-9]+)\.([a-zA-Z_0-9]+)=(.*)'
        for reconf in bool_reconfs or []:
            m = re.match(r, reconf)
            if m:
                if m.group(1) in h:
                    section = h[m.group(1)]
                else:
                    section = {}
                t = re.compile('true', re.IGNORECASE)
                f = re.compile('false', re.IGNORECASE)
                if t.match(m.group(3)):
                    section[m.group(2)] = True
                elif f.match(m.group(3)):
                    section[m.group(2)] = False
                else:
                    raise ValueError("Bad boolean value %s" % m.group(3))
                h[m.group(1)] = section

        for reconf in int_reconfs or []:
            m = re.match(r, reconf)
            if m:
                if m.group(1) in h:
                    section = h[m.group(1)]
                else:
                    section = {}

                section[m.group(2)] = int(m.group(3))
                h[m.group(1)] = section

        for reconf in string_reconfs or []:
            m = re.match(r, reconf)
            if m:
                if m.group(1) in h:
                    section = h[m.group(1)]
                else:
                    section = {}

                section[m.group(2)] = m.group(3)
                h[m.group(1)] = section

        return h

    @staticmethod
    def execute(client, opts):
        updated_kvs = ReconfigureDomain.format_reconfigure(
            bool_reconfs=opts.updated_kv_bool, int_reconfs=opts.updated_kv_int, string_reconfs=opts.updated_kv_string)
        return client.reconfigure_domain(opts.domain_id, updated_kvs, caller=opts.caller)


class AddDomainDefinition(CeiCommand):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('definition_id', action='store', help='The name of the domain definition to be added')
        parser.add_argument('--definition', dest='definition', action='store',
            help='File containing the domain definition description')

    @staticmethod
    def execute(client, opts):
        try:
            with open(opts.definition, 'r') as f:
                definition = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading domain definition file %s: %s" % (opts.definition, e))

        return client.add_domain_definition(opts.definition_id, definition)


class RemoveDomainDefinition(CeiCommand):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('definition_id', action='store', help='The definition domain to remove')

    @staticmethod
    def execute(client, opts):
        return client.remove_domain_definition(opts.definition_id)


class DescribeDomainDefinition(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('definition_id', action='store', help='The definition domain to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_domain_definition(opts.definition_id)


class ListDomainDefinitions(CeiCommandPrintListOutput):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_domain_definitions()


class UpdateDomainDefinition(CeiCommand):

    name = 'update'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('definition_id', action='store', help='The domain definition to reconfigure')
        parser.add_argument('--definition', dest='definition', action='store',
            help='File containing the new domain definition')

    @staticmethod
    def execute(client, opts):
        try:
            with open(opts.definition, 'r') as f:
                definition = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading domain definition file %s: %s" % (opts.definition, e))

        return client.update_domain_definition(opts.definition_id, definition)


class PDSystemBootOn(CeiCommand):

    name = 'on'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        client.set_system_boot(True)


class PDSystemBootOff(CeiCommand):

    name = 'off'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        client.set_system_boot(False)


def _validate_process_definition(definition):
    if not isinstance(definition, dict):
        raise ValueError("invalid definition")

    name = definition.get('name')
    if not name:
        raise ValueError("definition missing name")
    executable = definition.get('executable')
    if not executable or not isinstance(executable, dict):
        raise ValueError("definition has missing or invalid executable")

    module = executable.get('module')
    cls = executable.get('class')
    execu = executable.get('exec')
    if not execu and (not module or not cls):
        raise ValueError("definition has invalid executable")


def _load_process_definitions(definition_files):
    definition_names = set()
    definitions = []
    for definition_path in definition_files:
        try:
            with open(definition_path) as f:
                definition = yaml.load(f)
                _validate_process_definition(definition)
                definitions.append(definition)
        except Exception, e:
            raise CeiClientError("Problem reading process specification file %s: %s" % (definition_path, e))
        name = definition['name']
        if name in definition_names:
            raise CeiClientError("Process definition name '%s' found in multiple definitions!" % name)
        definition_names.add(name)
    return definitions


class PDCreateProcessDefinition(CeiCommand):

    name = 'create'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('definitions', nargs="+")
        parser.add_argument('-i', '--definition-id', dest="definition_id", metavar='ID')

    @staticmethod
    def execute(client, opts):

        if len(opts.definitions) > 1 and opts.definition_id:
            raise CeiClientError("Cannot specify command line definition_id with multiple files")
        definitions = _load_process_definitions(opts.definitions)

        result = []
        for definition, filename in zip(definitions, opts.definitions):

            if opts.definition_id:
                # above check prevents this from happening with multiple definitions
                definition_id = opts.definition_id
            else:
                definition_id = definition.get("process_definition_id")
                if not definition_id:
                    raise CeiClientError("Process definition id not specified in opts or spec")

            pd_id = client.create_process_definition(process_definition=definition,
                    process_definition_id=definition_id)
            result.append(pd_id)

        return result


class PDSyncProcessDefinitions(CeiCommand):

    name = 'sync'
    description = """
    Ensure all of the provided process definitions exist and are up to date.

    Definitions which already exist are updated (but retain the same ID).
    New definitions are created.
    """

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name, description=self.description)
        parser.add_argument('definitions', nargs="+", metavar="definition.yml")

    @staticmethod
    def execute(client, opts):

        definitions = _load_process_definitions(opts.definitions)

        result = []
        for definition in definitions:
            name = definition['name']
            try:
                found_definition = client.describe_process_definition(
                    process_definition_name=name)
            except NotFoundError:
                found_definition = None

            if found_definition:
                found_definition_id = found_definition.get('definition_id')
                if not found_definition_id:
                    found_definition_id = found_definition.get('_id')
                if not found_definition_id:
                    raise CeiClientError("Found '%s' definition without an ID??" % name)
                found_executable = found_definition.get('executable')
                if found_executable != definition['executable']:
                    client.update_process_definition(process_definition=definition,
                        process_definition_id=found_definition_id)
                    result.append((name, "UPDATED"))
                else:
                    result.append((name, "OK"))
            else:
                definition_id = uuid.uuid4().hex
                client.create_process_definition(process_definition=definition,
                    process_definition_id=definition_id)
                result.append((name, "CREATED"))
        return result

    @staticmethod
    def output(result):
        for name, status in result:
            print str(name).ljust(45) + "     " + str(status)


class PDDescribeProcessDefinition(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_definition_id', metavar='pd_id')

    @staticmethod
    def execute(client, opts):
        return client.describe_process_definition(opts.process_definition_id)

class PDNodeState(CeiCommand):

    name = 'nodestate'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('node_id')
        parser.add_argument('domain_id')
        parser.add_argument('state')

    @staticmethod
    def execute(client, opts):
        return client.node_state(opts.node_id, opts.domain_id, opts.state)

class PDUpdateProcessDefinition(CeiCommand):

    name = 'update'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_spec', metavar='process_spec.yml')
        parser.add_argument('-i', '--definition-id', dest="definition_id", metavar='ID')

    @staticmethod
    def execute(client, opts):
        try:
            with open(opts.process_spec) as f:
                process_spec = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading process specification file %s: %s" % (opts.process_spec, e))

        return client.update_process_definition(process_spec, opts.definition_id)


class PDRemoveProcessDefinition(CeiCommand):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_definition_id', metavar='pd_id')

    @staticmethod
    def execute(client, opts):
        return client.remove_process_definition(opts.process_definition_id)


class PDListProcessDefinitions(CeiCommandPrintListOutput):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_process_definitions()


class PDScheduleProcess(CeiCommand):

    name = 'schedule'

    description = "Schedule a new or existing process in the system"

    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name, description=self.description)
        parser.add_argument('--id', metavar='ID', help="process ID")

        group = parser.add_mutually_exclusive_group()
        group.add_argument('--definition-id', metavar='ID', help="process definition id")
        group.add_argument('--definition-name', metavar='DEFINITION', help="process definition name")

        parser.add_argument('--config', metavar='config.yml')
        parser.add_argument('--execution-engine-id', metavar='execution_engine_id')
        parser.add_argument('--queueing-mode', metavar='queueing_mode')
        parser.add_argument('--restart-mode', metavar='restart_mode')

    @staticmethod
    def execute(client, opts):

        if not (opts.id or opts.definition_name or opts.definition_id):
            raise CeiClientError("Need a process ID or process definition to launch")

        process_id = opts.id or uuid.uuid4().hex

        configuration = None
        if opts.config:
            try:
                with open(opts.config) as f:
                    configuration = yaml.load(f)
            except Exception, e:
                raise CeiClientError("Problem reading process configuration file %s: %s"
                    % (opts.configuration, e))

        return client.schedule_process(process_id,
                process_definition_id=opts.definition_id,
                process_definition_name=opts.definition_name,
                configuration=configuration, queueing_mode=opts.queueing_mode,
                execution_engine_id=opts.execution_engine_id,
                restart_mode=opts.restart_mode)


class PDDescribeProcesses(CeiCommand):

    name = 'list'
    output_template = '''
Process ID    = {{ result.upid }}
Process Name  = {{ result.name }}
Process State = {{ result.state }}
Hostname      = {{ result.hostname }}
EEAgent       = {{ result.assigned }}
'''

    details_template = '''
Process ID    = {{ result.upid }}
Process Name  = {{ result.name }}
Process State = {{ result.state }}
Hostname      = {{ result.hostname }}
EEAgent       = {{ result.assigned }}
Constraints   = {{ result.constraints }}
Configuration = {{ result.configuration }}
'''

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.describe_processes()

    @staticmethod
    def output(result):
        template = Template(PDDescribeProcesses.output_template)
        for raw_proc in result:
            raw_proc = PDDescribeProcess.extract_details(raw_proc)
            safe_print(template.render(result=raw_proc))

    @staticmethod
    def details(result):
        template = Template(PDDescribeProcesses.details_template)
        for raw_proc in result:
            raw_proc = PDDescribeProcess.extract_details(raw_proc)
            raw_proc['constraints'] = yaml.safe_dump(raw_proc.get('constraints', {})).rstrip('\n')
            raw_proc['configuration'] = yaml.safe_dump(raw_proc.get('configuration', {})).rstrip('\n')
            safe_print(template.render(result=raw_proc))


class PDTerminateProcess(CeiCommand):

    name = 'kill'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', action='store', help='The UPID of the process to kill')

    @staticmethod
    def execute(client, opts):
        return client.terminate_process(opts.process_id)


class PDRestartProcess(CeiCommand):

    name = 'restart'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', action='store', help='The UPID of the process to kill')

    @staticmethod
    def execute(client, opts):
        return client.restart_process(opts.process_id)


class PDDescribeProcess(CeiCommand):

    name = 'describe'
    output_template = '''ID             = {{ result.upid }}
Name           = {{ result.name }}
State          = {{ result.state }}
Hostname       = {{ result.hostname }}
EEAgent        = {{ result.assigned }}
Node Exclusive = {{ result.node_exclusive }}
Queueing Mode  = {{ result.queueing_mode }}
Restart Mode   = {{ result.restart_mode }}
Starts         = {{ result.starts }}
Constraints    = {{ result.constraints }}
Configuration  = {{ result.configuration }}
'''

    pyon_process_state_map = {
        1: '200-REQUESTED', 2: '300-WAITING', 3: '400-PENDING', 4: '500-RUNNING',
        5: '600-TERMINATING', 6: '700-TERMINATED', 7: '850-FAILED',
        8: '900-REJECTED', 9: '800-EXITED'
    }

    pyon_process_queue_map = {
        1: 'NEVER', 2: 'ALWAYS', 3: 'START_ONLY', 4: 'RESTART_ONLY',
    }

    pyon_process_restart_map = {
        1: 'NEVER', 2: 'ALWAYS', 3: 'ABNORMAL',
    }

    pyon_process_queue_reverse_map = {
        'NEVER': 1, 'ALWAYS': 2, 'START_ONLY': 3, 'RESTART_ONLY': 4,
    }

    pyon_process_restart_map = {
        'NEVER': 1, 'ALWAYS': 2, 'ABNORMAL': 3,
    }

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', action='store', help='The UPID of the process to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_process(opts.process_id)

    @staticmethod
    def extract_details(process):
        result = dict(process)
        if 'process_id' in result:
            result['upid'] = result['process_id']

        if 'process_configuration' in result:
            result['configuration'] = result['process_configuration']

        if 'process_state' in result:
            result['state'] = PDDescribeProcess.pyon_process_state_map.get(int(result['process_state']))
        if 'detail' in result:
            detail = result['detail']
            result['hostname'] = detail.get('hostname')
            result['assigned'] = detail.get('assigned')
            result['node_exclusive'] = detail.get('node_exclusive')
            result['queueing_mode'] = detail.get('queueing_mode')
            result['restart_mode'] = detail.get('restart_mode')
            result['starts'] = detail.get('starts')

        return result

    @staticmethod
    def output(result):
        template = Template(PDDescribeProcess.output_template)

        result = PDDescribeProcess.extract_details(result)

        result['constraints'] = yaml.safe_dump(result.get('constraints', {})).rstrip('\n')
        result['configuration'] = yaml.safe_dump(result.get('configuration', {})).rstrip('\n')
        safe_print(template.render(result=result))


class PDWaitProcess(CeiCommand):

    name = 'wait'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', action='store',
                help='The UPID of the process to wait on')
        parser.add_argument('--max', action='store', type=float, default=9600,
                help='Max seconds to wait for process state')
        parser.add_argument('--poll', action='store', type=float, default=0.1,
                help='Seconds to wait between polls')

    @staticmethod
    def execute(client, opts):

        deadline = time.time() + opts.max
        while 1:
            process = client.describe_process(opts.process_id)

            if process:
                process = PDDescribeProcess.extract_details(process)
                state = process['state']

                if state in ("500-RUNNING", "800-EXITED"):
                    return process

                if state in ("850-FAILED", "900-REJECTED"):
                    raise CeiClientError("FAILED. Process in %s state" % state)

            if time.time() + opts.poll >= deadline:
                raise CeiClientError("Timed out waiting for process %s" % opts.process_id)
            time.sleep(opts.poll)


class PDDump(CeiCommand):

    name = 'dump'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.dump()

# TODO Other dashi calls for the PD:
#dt_state
#heartbeat, sender_kwarg='sender'


class PyonPDCreateProcessDefinition(CeiCommand):

    name = 'create'

    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_spec', metavar='process_spec.yml')
        parser.add_argument('-i', '--definition-id', dest="definition_id", metavar='ID')

    @staticmethod
    def execute(client, opts):
        try:
            with open(opts.process_spec) as f:
                process_spec = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading process specification file %s: %s" % (opts.process_spec, e))

        return client.create_process_definition(process_spec, opts.definition_id)


class PyonPDUpdateProcessDefinition(CeiCommand):

    name = 'update'

    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_spec', metavar='process_spec.yml')
        parser.add_argument('-i', '--definition-id', dest="definition_id", metavar='ID')

    @staticmethod
    def execute(client, opts):
        try:
            with open(opts.process_spec) as f:
                process_spec = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading process specification file %s: %s" % (opts.process_spec, e))

        return client.update_process_definition(process_spec, opts.definition_id)


class PyonPDReadProcessDefinition(CeiCommand):

    name = 'read'

    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_definition_id', metavar='pd_id')

    @staticmethod
    def execute(client, opts):

        return client.read_process_definition(opts.process_definition_id)


class PyonPDDeleteProcessDefinition(CeiCommand):

    name = 'delete'

    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_definition_id', metavar='pd_id')

    @staticmethod
    def execute(client, opts):

        return client.delete_process_definition(opts.process_definition_id)


class PyonPDListProcessDefinitions(CeiCommand):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):

        return client.list_process_definitions()


class PyonPDAssociateExecutionEngine(CeiCommand):

    name = 'associate'

    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_definition_id', metavar='pd_id')
        parser.add_argument('execution_engine_definition_id', metavar='eed_id')

    @staticmethod
    def execute(client, opts):

        return client.associate_execution_engine(opts.process_definition_id, opts.execution_engine_definition_id)


class PyonPDDissociateExecutionEngine(CeiCommand):

    name = 'dissociate'

    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_definition_id', metavar='pd_id')
        parser.add_argument('execution_engine_definition_id', metavar='eed_id')

    @staticmethod
    def execute(client, opts):

        return client.dissociate_execution_engine(opts.process_definition_id, opts.execution_engine_definition_id)


class ProcessStateEnum(object):
    """WARNING: THIS COULD CHANGE
    """
    SPAWN = 1
    TERMINATE = 2
    ERROR = 3

    _str_map = {
        "1": "SPAWN",
        "2": "TERMINATE",
        "3": "ERROR"
    }

    @staticmethod
    def to_str(int_state):
        return ProcessStateEnum._str_map.get(str(int_state))


class PyonPDCreateProcess(CeiCommand):

    name = 'create'

    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_definition_id', metavar='pd_id')

    @staticmethod
    def execute(client, opts):

        return client.create_process(opts.process_definition_id)


class PyonPDScheduleProcess(CeiCommand):

    name = 'schedule'

    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_definition_id', metavar='pd_id')
        parser.add_argument('schedule', metavar='process_schedule.yml')
        parser.add_argument('configuration', metavar='process_configuration.yml')
        parser.add_argument('process_id', metavar='proc_id')

    @staticmethod
    def execute(client, opts):
        try:
            with open(opts.schedule) as f:
                schedule = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading process schedule file %s: %s" % (opts.schedule, e))

        try:
            with open(opts.configuration) as f:
                configuration = yaml.load(f)
        except Exception:
            raise CeiClientError("Problem reading process configuration file %s: %s" % (opts.configuration, e))
        return client.schedule_process(opts.process_definition_id, schedule, configuration, opts.process_id)


class PyonPDCancelProcess(CeiCommand):

    name = 'cancel'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', metavar='pd_id')

    @staticmethod
    def execute(client, opts):
        return client.cancel_process(opts.process_id)


class PyonPDReadProcess(CeiCommand):

    name = 'read'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', metavar='pd_id')

    @staticmethod
    def execute(client, opts):
        return client.read_process(opts.process_id)


class PyonPDListProcesses(CeiCommand):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_processes()


class PyonPDWaitProcess(CeiCommand):

    name = 'wait'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', action='store',
                help='The UPID of the process to wait on')
        parser.add_argument('--max', action='store', type=float, default=9600,
                help='Max seconds to wait for process state')
        parser.add_argument('--poll', action='store', type=float, default=0.5,
                help='Seconds to wait between polls')

    @staticmethod
    def execute(client, opts):

        deadline = time.time() + opts.max
        while 1:
            process = client.read_process(opts.process_id)

            if process:
                state = process['process_state']

                if state in (ProcessStateEnum.SPAWN, ProcessStateEnum.TERMINATE):
                    return process

                if state in (ProcessStateEnum.ERROR,):
                    raise CeiClientError("FAILED. Process in %s state" % ProcessStateEnum.to_str(state))

            if time.time() + opts.poll >= deadline:
                raise CeiClientError("Timed out waiting for process %s" % opts.process_id)
            time.sleep(opts.poll)


class HAList(CeiCommand):

    name = 'list'

    details_template = '''
HA Agent for {{ result.configuration.highavailability.process_definition_name }}
Process ID    = {{ result.upid }}
Process Name  = {{ result.name }}
Process State = {{ result.state }}
Dashi Name    = {{ result.configuration.highavailability.dashi_name }}
Hostname      = {{ result.hostname }}
EEAgent       = {{ result.assigned }}
'''

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        all_procs = client.describe_processes()
        ha_procs = [proc for proc in all_procs if proc['name'] and proc['name'].startswith('haagent')]
        non_terminated_procs = [proc for proc in ha_procs if proc['state'] < '600']
        return non_terminated_procs

    @staticmethod
    def output(result):
        for raw_proc in result:
            safe_print(raw_proc['configuration']['highavailability']['process_definition_name'])

    @staticmethod
    def details(result):
        template = Template(HAList.details_template)
        for raw_proc in result:
            safe_print(template.render(result=raw_proc))


class HADescribe(CeiCommand):

    name = 'describe'

    output_template = '''HA Agent for {{ result.name}}
Service ID    = {{ result.service_id }}
HA Status     = {{ result.status }}
Processes     = {{ result.managed_upids }}
Policy        = {{ result.policy }}
'''

    details_template = '''HA Agent for {{ result.name}}
Service ID    = {{ result.service_id }}
HA Status     = {{ result.status }}
Processes     = {{ result.managed_upids }}
Policy        = {{ result.policy }}
Policy Parameters: {% for key, val in result.policy_params.iteritems() %}
  {{ key }} = {{ val }}{% endfor %}
'''

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process', metavar='HAPROCESS')

    @staticmethod
    def execute(client, opts):
        ha_dashi_name = "ha_%s" % opts.process
        ha_client = HAAgent.ha_client(client.connection, dashi_name=ha_dashi_name)
        dump = ha_client.dump()
        dump['name'] = opts.process
        dump['status'] = ha_client.status()
        return dump

    @staticmethod
    def output(result):
        result['managed_upids'] = ",".join(result['managed_upids'])
        template = Template(HADescribe.output_template)
        safe_print(template.render(result=result))

    @staticmethod
    def details(result):
        result['managed_upids'] = ",".join(result['managed_upids'])
        template = Template(HADescribe.details_template)
        safe_print(template.render(result=result))


class HADumpPolicy(CeiCommand):

    name = 'dump_policy'

    output_template = '''---
policy_name: {{ result.policy }}
policy_params: {% for key, val in result.policy_params.iteritems() %}
  {{key}}: {{val}}{% endfor %}
'''

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process', metavar='HAPROCESS')

    @staticmethod
    def execute(client, opts):
        ha_dashi_name = "ha_%s" % opts.process
        ha_client = HAAgent.ha_client(client.connection, dashi_name=ha_dashi_name)
        dump = ha_client.dump()
        for key in dump.keys():
            if key not in ('policy', 'policy_params'):
                del dump[key]
        return dump

    @staticmethod
    def output(result):
        template = Template(HADumpPolicy.output_template)
        safe_print(template.render(result=result))


class HAStatus(CeiCommand):

    name = 'status'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process', metavar='HAPROCESS')

    @staticmethod
    def execute(client, opts):
        ha_dashi_name = "ha_%s" % opts.process
        ha_client = HAAgent.ha_client(client.connection, dashi_name=ha_dashi_name)
        return ha_client.status()

    @staticmethod
    def output(result):
        safe_print(result)


class HAReconfigurePolicy(CeiCommand):

    name = 'reconfigure'
    POLICY_PARAMS = ('preserve_n', 'metric', 'minimum_processes', 'maximum_processes',
        'sample_period', 'sample_function', 'cooldown_period', 'scale_up_threshold',
        'scale_up_n_processes', 'scale_down_threshold', 'scale_down_n_processes')

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process', metavar='HAPROCESS')
        parser.add_argument('policy', nargs='?', metavar='new_policy.yml', default=None)
        parser.add_argument('--preserve_n', metavar='N',
            help="The number of instances to maintain of this service.")
        parser.add_argument('--metric', metavar='METRICNAME',
            help="The traffic sentinel metric to scale on.")
        parser.add_argument('--minimum_processes', metavar='N',
            help="The minimum number of processes to keep running.")
        parser.add_argument('--maximum_processes', metavar='N',
            help="The maximum number of processes to keep running.")
        parser.add_argument('--sample_period', metavar='N',
            help="The period of time (in seconds) to sample the metrics from.")
        parser.add_argument('--sample_function', metavar='SAMPLEFUNC',
            help="The function to apply to sampled metrics to get a single "
            "value that can be compared against your scale thresholds. "
            "Choose from Average, Sum, SampleCount, Maximum, Minimum.")
        parser.add_argument('--cooldown_period', metavar='N',
            help="The amount of time (in seconds) to wait in between each scaling action.")
        parser.add_argument('--scale_up_threshold', metavar='N',
            help="Scale up if metric exceeds this value. ")
        parser.add_argument('--scale_up_n_processes', metavar='N',
                help="The number of processes to start when scaling up.")
        parser.add_argument('--scale_down_threshold', metavar='N',
            help="Scale down if metric is lower than this value. ")
        parser.add_argument('--scale_down_n_processes', metavar='N',
                help="The number of processes to terminate when scaling down.")

    @staticmethod
    def execute(client, opts):
        ha_dashi_name = "ha_%s" % opts.process
        ha_client = HAAgent.ha_client(client.connection, dashi_name=ha_dashi_name)
        if opts.policy is not None:
            try:
                with open(opts.policy) as f:
                    policy = yaml.load(f)
                    policy_name = policy.get('policy_name')
                    policy_parameters = policy.get('policy_params')
            except Exception, e:
                raise CeiClientError("Problem reading policy file %s: %s" % (opts.policy, e))
        else:
            policy_name = None
            policy_parameters = {}

        if policy_name is not None and policy_parameters is None:
            err = "You have set a new policy_name, but no new parameters"
            raise CeiClientError("Problem with policy file %s: %s" % (opts.policy, err))

        for key, val in opts.__dict__.iteritems():

            if val is not None and key in HAReconfigurePolicy.POLICY_PARAMS:
                policy_parameters[key] = val

        try:
            return ha_client.reconfigure_policy(policy_parameters, new_policy=policy_name)
        except BadRequestError as e:
            raise CeiClientError("Bad Request: %s" % e.value)

    @staticmethod
    def output(result):
        if result is not None:
            safe_print(result)


class HAWaitStatus(CeiCommand):

    name = 'wait'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process', metavar='HAPROCESS')
        parser.add_argument('--max', action='store', type=float, default=9600,
                help='Max seconds to wait for ready state')
        parser.add_argument('--poll', action='store', type=float, default=0.1,
                help='Seconds to wait between polls')

    @staticmethod
    def execute(client, opts):
        ha_dashi_name = "ha_%s" % opts.process
        ha_client = HAAgent.ha_client(client.connection, dashi_name=ha_dashi_name)

        deadline = time.time() + opts.max
        while 1:

            status = ha_client.status()
            if status:
                if status in ("READY", "STEADY"):
                    return status
                elif status == "FAILED":
                    raise CeiClientError("HA Agent in %s state" % status)

            if time.time() + opts.poll >= deadline:
                raise CeiClientError("Timed out waiting for HA Agent")
            time.sleep(opts.poll)

    @staticmethod
    def output(result):
        safe_print(result)


class PyonHTTPHAList(CeiCommand):

    name = 'list'

    details_template = '''
HA Agent for {{ result.configuration.highavailability.process_definition_name }}
Process ID    = {{ result.upid }}
Process Name  = {{ result.name }}
Process State = {{ result.state }}
Dashi Name    = {{ result.configuration.highavailability.dashi_name }}
Hostname      = {{ result.hostname }}
EEAgent       = {{ result.assigned }}
'''

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        all_procs = client.describe_processes()
        ha_procs = [proc for proc in all_procs if proc['name'] and proc['name'].startswith('haagent')]
        non_terminated_procs = [proc for proc in ha_procs if proc['state'] < '600']
        return non_terminated_procs

    @staticmethod
    def output(result):
        for raw_proc in result:
            safe_print(raw_proc['configuration']['highavailability']['process_definition_name'])

    @staticmethod
    def details(result):
        template = Template(HAList.details_template)
        for raw_proc in result:
            safe_print(template.render(result=raw_proc))


class PyonHTTPHADescribe(CeiCommand):

    name = 'describe'

    output_template = '''HA Agent for {{ result.name}}
Service ID    = {{ result.service_id }}
HA Status     = {{ result.status }}
Processes     = {{ result.managed_upids }}
Policy        = {{ result.policy }}
'''

    details_template = '''HA Agent for {{ result.name}}
Service ID    = {{ result.service_id }}
HA Status     = {{ result.status }}
Processes     = {{ result.managed_upids }}
Policy        = {{ result.policy }}
Policy Parameters: {% for key, val in result.policy_params.iteritems() %}
  {{ key }} = {{ val }}{% endfor %}
'''

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process', metavar='HAPROCESS')

    @staticmethod
    def execute(client, opts):
        ha_dashi_name = "ha_%s" % opts.process
        ha_client = HAAgent.ha_client(client.connection, dashi_name=ha_dashi_name)
        dump = ha_client.dump()
        dump['name'] = opts.process
        dump['status'] = ha_client.status()
        return dump

    @staticmethod
    def output(result):
        result['managed_upids'] = ",".join(result['managed_upids'])
        template = Template(HADescribe.output_template)
        safe_print(template.render(result=result))

    @staticmethod
    def details(result):
        result['managed_upids'] = ",".join(result['managed_upids'])
        template = Template(HADescribe.details_template)
        safe_print(template.render(result=result))


class PyonHTTPHADumpPolicy(CeiCommand):

    name = 'dump_policy'

    output_template = '''---
policy_name: {{ result.policy }}
policy_params: {% for key, val in result.policy_params.iteritems() %}
  {{key}}: {{val}}{% endfor %}
'''

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process', metavar='HAPROCESS')

    @staticmethod
    def execute(client, opts):
        ha_dashi_name = "ha_%s" % opts.process
        ha_client = HAAgent.ha_client(client.connection, dashi_name=ha_dashi_name)
        dump = ha_client.dump()
        for key in dump.keys():
            if key not in ('policy', 'policy_params'):
                del dump[key]
        return dump

    @staticmethod
    def output(result):
        template = Template(HADumpPolicy.output_template)
        safe_print(template.render(result=result))


class PyonHTTPHAStatus(CeiCommand):

    name = 'status'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process', metavar='HAPROCESS')

    @staticmethod
    def execute(client, opts):
        process_id = client.get_ha_process_id(opts.process)
        if not process_id:
            raise CeiClientError("Couldn't find agent for process %s" % opts.process)
        ha_client = PyonHTTPHAAgent.ha_client(client.connection, dashi_name=process_id)
        return ha_client.status()

    @staticmethod
    def output(result):
        safe_print(result)


class PyonHTTPHAReconfigurePolicy(CeiCommand):

    name = 'reconfigure'
    POLICY_PARAMS = ('preserve_n', 'metric', 'minimum_processes', 'maximum_processes',
        'sample_period', 'sample_function', 'cooldown_period', 'scale_up_threshold',
        'scale_up_n_processes', 'scale_down_threshold', 'scale_down_n_processes')

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process', metavar='HAPROCESS')
        parser.add_argument('policy', nargs='?', metavar='new_policy.yml', default=None)
        parser.add_argument('--preserve_n', metavar='N',
            help="The number of instances to maintain of this service.")
        parser.add_argument('--metric', metavar='METRICNAME',
            help="The traffic sentinel metric to scale on.")
        parser.add_argument('--minimum_processes', metavar='N',
            help="The minimum number of processes to keep running.")
        parser.add_argument('--maximum_processes', metavar='N',
            help="The maximum number of processes to keep running.")
        parser.add_argument('--sample_period', metavar='N',
            help="The period of time (in seconds) to sample the metrics from.")
        parser.add_argument('--sample_function', metavar='SAMPLEFUNC',
            help="The function to apply to sampled metrics to get a single "
            "value that can be compared against your scale thresholds. "
            "Choose from Average, Sum, SampleCount, Maximum, Minimum.")
        parser.add_argument('--cooldown_period', metavar='N',
            help="The amount of time (in seconds) to wait in between each scaling action.")
        parser.add_argument('--scale_up_threshold', metavar='N',
            help="Scale up if metric exceeds this value. ")
        parser.add_argument('--scale_up_n_processes', metavar='N',
                help="The number of processes to start when scaling up.")
        parser.add_argument('--scale_down_threshold', metavar='N',
            help="Scale down if metric is lower than this value. ")
        parser.add_argument('--scale_down_n_processes', metavar='N',
                help="The number of processes to terminate when scaling down.")

    @staticmethod
    def execute(client, opts):
        ha_dashi_name = "ha_%s" % opts.process
        ha_client = HAAgent.ha_client(client.connection, dashi_name=ha_dashi_name)
        if opts.policy is not None:
            try:
                with open(opts.policy) as f:
                    policy = yaml.load(f)
                    policy_name = policy.get('policy_name')
                    policy_parameters = policy.get('policy_params')
            except Exception, e:
                raise CeiClientError("Problem reading policy file %s: %s" % (opts.policy, e))
        else:
            policy_name = None
            policy_parameters = {}

        if policy_name is not None and policy_parameters is None:
            err = "You have set a new policy_name, but no new parameters"
            raise CeiClientError("Problem with policy file %s: %s" % (opts.policy, err))

        for key, val in opts.__dict__.iteritems():
            if val is not None and key in HAReconfigurePolicy.POLICY_PARAMS:
                policy_parameters[key] = val

        try:
            return ha_client.reconfigure_policy(policy_parameters, new_policy=policy_name)
        except BadRequestError as e:
            raise CeiClientError("Bad Request: %s" % e.value)

    @staticmethod
    def output(result):
        if result is not None:
            safe_print(result)


class PyonHTTPHAWaitStatus(CeiCommand):

    name = 'wait'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process', metavar='HAPROCESS')
        parser.add_argument('--max', action='store', type=float, default=9600,
                help='Max seconds to wait for ready state')
        parser.add_argument('--poll', action='store', type=float, default=0.1,
                help='Seconds to wait between polls')

    @staticmethod
    def execute(client, opts):
        ha_dashi_name = "ha_%s" % opts.process
        ha_client = HAAgent.ha_client(client.connection, dashi_name=ha_dashi_name)

        deadline = time.time() + opts.max
        while 1:

            status = ha_client.status()
            if status:
                if status in ("READY", "STEADY"):
                    return status
                elif status == "FAILED":
                    raise CeiClientError("HA Agent in %s state" % status)

            if time.time() + opts.poll >= deadline:
                raise CeiClientError("Timed out waiting for HA Agent")
            time.sleep(opts.poll)

    @staticmethod
    def output(result):
        safe_print(result)


class PyonHAStatus(CeiCommand):

    name = 'status'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.status()


class PyonHAReconfigurePolicy(CeiCommand):

    name = 'reconfigure_policy'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('policy', metavar='new_policy.yml')

    @staticmethod
    def execute(client, opts):
        try:
            with open(opts.policy) as f:
                policy = yaml.load(f)
        except Exception, e:
            raise CeiClientError("Problem reading policy file %s: %s" % (opts.policy, e))
        return client.reconfigure_policy(policy)


class ProvisionerDump(CeiCommand):

    name = 'dump_state'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('node', action='store', help='The node state to dump')

    @staticmethod
    def execute(client, opts):
        subscriber = str(uuid.uuid4())
        return client.dump_state([opts.node], subscriber)


class ProvisionerDescribeNodes(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('node', help='A node to describe (describes all nodes if none provided)', nargs='*')

    @staticmethod
    def execute(client, opts):
        nodes = opts.node or []
        return client.describe_nodes(nodes=nodes, caller=opts.caller)


class ProvisionerProvision(CeiCommand):

    name = 'provision'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('deployable_type', help='DT to provision')
        parser.add_argument('site', help='IaaS site to use (e.g. ec2-east)')
        parser.add_argument('allocation', help='Type of instance to use (e.g. t1.micro)')
        parser.add_argument('provisioning_var_file', help='File containing provisioning vars')

    @staticmethod
    def execute(client, opts):
        try:
            with open(opts.provisioning_var_file) as vars_file:
                vars = yaml.load(vars_file)
        except Exception, e:
            raise CeiClientError("Problem reading provisioning variables file %s: %s" % (opts.provisioning_var_file, e))

        # Update the provisioning variables with secret RabbitMQ credentials
        vars['broker_ip_address'] = client.connection.amqp_broker
        vars['broker_username'] = client.connection.amqp_username
        vars['broker_password'] = client.connection.amqp_password

        return client.provision(opts.deployable_type, opts.site, opts.allocation, vars, caller=opts.caller)


class ProvisionerTerminateAll(CeiCommand):

    name = 'terminate_all'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    # From https://confluence.oceanobservatories.org/display/CIDev/R2+EPU+Provisioner+Improvements
    #
    # Disable the provisioner and terminate all running instances.  Prevent
    # other instances from being started.  This is used during system shutdown
    # to clean up running VMs.
    # This operation can be called RPC-style and returns a boolean indicating
    # whether all instances are terminated. It should be called repeatedly
    # until True is received.
    @staticmethod
    def execute(client, opts):
        while not client.terminate_all():
            pass


############
# SERVICES #
############


class CeiService(object):

    def __init__(self, subparsers):
        pass


class DT(CeiService):
    name = 'dt'
    help = 'Control Deployable Types in the DTRS'

    commands = {}
    for command in [DTRSAddDT, DTRSDescribeDT, DTRSListDT, DTRSRemoveDT, DTRSUpdateDt]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return DTRSClient(connection, dashi_name=dashi_name)


class Site(CeiService):

    dashi_name = 'dtrs'
    name = 'site'
    help = 'Control sites in the DTRS'

    commands = {}
    for command in [DTRSAddSite, DTRSDescribeSite, DTRSListSites, DTRSRemoveSite, DTRSUpdateSite]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return DTRSClient(connection, dashi_name=dashi_name)


class Credentials(CeiService):

    name = 'credentials'
    help = 'Control credentials in the DTRS'

    commands = {}
    for command in [DTRSAddCredentials, DTRSDescribeCredentials,
            DTRSListCredentials, DTRSRemoveCredentials, DTRSUpdateCredentials]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return DTRSClient(connection, dashi_name=dashi_name)


class Domain(CeiService):

    name = 'domain'
    help = 'Control domains in the EPU Management Service'

    commands = {}
    for command in [DescribeDomain, ListDomains, ReconfigureDomain, AddDomain, RemoveDomain]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return EPUMClient(connection, dashi_name=dashi_name)


class DomainDefinition(CeiService):

    name = 'domain-definition'
    help = 'Control domain definitions in the EPU Management Service'

    commands = {}
    for command in [DescribeDomainDefinition, ListDomainDefinitions,
                UpdateDomainDefinition, AddDomainDefinition, RemoveDomainDefinition]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return EPUMClient(connection, dashi_name=dashi_name)


class PDSystemBoot(CeiService):

    name = 'system-boot'
    help = 'Control the Process Dispatcher system boot state'

    commands = {}
    for command in [PDSystemBootOn, PDSystemBootOff]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return PDClient(connection, dashi_name=dashi_name)


class PyonHTTPProcess(CeiService):

    name = 'process'
    help = 'Control the Process Dispatcher Service'

    commands = {}
    for command in [PDScheduleProcess, PDDescribeProcess, PDDescribeProcesses,
            PDTerminateProcess, PDDump, PDRestartProcess, PDWaitProcess]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return PyonHTTPPDClient(connection, dashi_name=dashi_name)


class PyonHTTPProcessDefinition(CeiService):

    name = 'process-definition'
    help = 'Control the Process Dispatcher Service'

    commands = {}
    for command in [PDCreateProcessDefinition, PDUpdateProcessDefinition,
            PDSyncProcessDefinitions, PDDescribeProcessDefinition,
            PDRemoveProcessDefinition, PDListProcessDefinitions]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return PyonHTTPPDClient(connection, dashi_name=dashi_name)


class Process(CeiService):

    name = 'process'
    help = 'Control the Process Dispatcher Service'

    commands = {}
    for command in [PDScheduleProcess, PDDescribeProcess, PDDescribeProcesses,
            PDTerminateProcess, PDDump, PDRestartProcess, PDWaitProcess, PDNodeState]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return PDClient(connection, dashi_name=dashi_name)


class ProcessDefinition(CeiService):

    name = 'process-definition'
    help = 'Control the Process Dispatcher Service'

    commands = {}
    for command in [PDCreateProcessDefinition, PDUpdateProcessDefinition,
            PDSyncProcessDefinitions, PDDescribeProcessDefinition,
            PDRemoveProcessDefinition, PDListProcessDefinitions]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return PDClient(connection, dashi_name=dashi_name)


class Provisioner(CeiService):

    name = 'provisioner'
    help = 'Control the Provisioner Service'

    commands = {}
    for command in [ProvisionerDump, ProvisionerDescribeNodes, ProvisionerProvision, ProvisionerTerminateAll]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return ProvisionerClient(connection, dashi_name=dashi_name)


class HAAgent(CeiService):

    name = 'ha'
    help = 'Control a High Availability Agent'

    commands = {}
    for command in [HAStatus, HAReconfigurePolicy, HAWaitStatus, HADumpPolicy, HADescribe, HAList]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return PDClient(connection, dashi_name=dashi_name)

    @staticmethod
    def ha_client(connection, dashi_name=None):
        return HAAgentClient(connection, dashi_name=dashi_name)


class PyonHTTPHAAgent(CeiService):

    name = 'ha'
    help = 'Control a High Availability Agent'

    commands = {}
    for command in [PyonHTTPHAStatus, PyonHTTPHAReconfigurePolicy, PyonHTTPHAWaitStatus,
            PyonHTTPHADumpPolicy, PyonHTTPHADescribe, PyonHTTPHAList]:
        commands[command.name] = command

    @staticmethod
    def client(connection, dashi_name=None):
        return PyonHTTPPDClient(connection, dashi_name=dashi_name)

    @staticmethod
    def ha_client(connection, dashi_name=None):

        return PyonHTTPHAAgentClient(connection, dashi_name=dashi_name)


class PyonProcessDefinition(CeiService):

    name = 'process-definition'
    help = 'Control the Pyon Process Dispatcher Service'

    commands = {}
    for command in [PyonPDCreateProcessDefinition, PyonPDUpdateProcessDefinition,
            PyonPDReadProcessDefinition, PyonPDDeleteProcessDefinition, PyonPDListProcessDefinitions]:
        commands[command.name] = command

    @staticmethod
    def client(connection, service_name=None):
        return PyonPDClient(connection, service_name=service_name)


class PyonPDExecutionEngine(CeiService):

    name = 'execution-engine'
    help = 'Control the Pyon Process Dispatcher Service'

    commands = {}
    for command in [PyonPDAssociateExecutionEngine, PyonPDDissociateExecutionEngine]:
        commands[command.name] = command

    @staticmethod
    def client(connection, service_name=None):
        return PyonPDClient(connection, service_name=service_name)


class PyonPDProcess(CeiService):

    name = 'process'
    help = 'Control the Pyon Process Dispatcher Service'

    commands = {}
    for command in [PyonPDCreateProcess, PyonPDScheduleProcess, PyonPDCancelProcess,
            PyonPDReadProcess, PyonPDListProcesses, PyonPDWaitProcess]:
        commands[command.name] = command

    @staticmethod
    def client(connection, service_name=None):
        return PyonPDClient(connection, service_name=service_name)


class PyonHAAgent(CeiService):

    name = 'ha'
    help = 'Control the Pyon High Availability Agent'

    commands = {}
    for command in [PyonHAStatus, PyonHAReconfigurePolicy]:
        commands[command.name] = command

    @staticmethod
    def client(connection, service_name=None):
        return PyonHAAgentClient(connection, service_name=service_name)


DASHI_SERVICES = {}
for service in [DT, Site, Credentials, Domain, DomainDefinition, Process,
        ProcessDefinition, Provisioner, HAAgent, PDSystemBoot]:
    DASHI_SERVICES[service.name] = service

PYON_SERVICES = {}
for service in [PyonProcessDefinition, PyonPDProcess,
        PyonPDExecutionEngine, PyonHAAgent]:
    PYON_SERVICES[service.name] = service

PYON_GATEWAY_SERVICES = {}
for service in [PyonHTTPProcess, PyonHTTPProcessDefinition, PyonHTTPHAAgent]:
    PYON_GATEWAY_SERVICES[service.name] = service
