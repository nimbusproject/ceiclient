import pprint
import re
import sys
import uuid
import time

from jinja2 import Template
import yaml


class CeiCommand(object):

    def __init__(self, subparsers):
        pass

    @staticmethod
    def output(result):
        pprint.pprint(result)


class DTRSAddDT(CeiCommand):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('dt_name', action='store', help='The name of the DT to be added.')
        parser.add_argument('--definition', dest='dt_def_file', action='store', help='Set the DT definition to use.')

    @staticmethod
    def execute(client, opts):
        stream = open(opts.dt_def_file, 'r')
        dt_def = yaml.load(stream)
        stream.close()
        return client.add_dt(opts.caller, opts.dt_name, dt_def)


class DTRSDescribeDT(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('dt_name', action='store', help='The DT to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_dt(opts.caller, opts.dt_name)


class DTRSListDT(CeiCommand):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_dts(caller=opts.caller)


class DTRSRemoveDT(CeiCommand):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('dt_name', action='store', help='The DT to remove')

    @staticmethod
    def execute(client, opts):
        return client.remove_dt(opts.caller, opts.dt_name)


class DTRSUpdateDt(CeiCommand):

    name = 'update'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('dt_name', action='store', help='The name of the DT to be updated.')
        parser.add_argument('--definition', dest='dt_def_file', action='store', help='The DT definition to use.')

    @staticmethod
    def execute(client, opts):
        stream = open(opts.dt_def_file, 'r')
        dt_def = yaml.load(stream)
        stream.close()
        return client.update_dt(opts.caller, opts.dt_name, dt_def)


class DTRSAddSite(CeiCommand):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The name of the site to be added.')
        parser.add_argument('--definition', dest='site_def_file', action='store', help='Set the site definition to use.')

    @staticmethod
    def execute(client, opts):
        stream = open(opts.site_def_file, 'r')
        site_def = yaml.load(stream)
        stream.close()
        return client.add_site(opts.site_name, site_def)


class DTRSDescribeSite(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The site to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_site(opts.site_name)


class DTRSListSites(CeiCommand):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_sites()


class DTRSRemoveSite(CeiCommand):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The site to remove')

    @staticmethod
    def execute(client, opts):
        return client.remove_site(opts.site_name)


class DTRSUpdateSite(CeiCommand):

    name = 'update'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The name of the site to be updated.')
        parser.add_argument('--definition', dest='site_def_file', action='store', help='The site definition to use.')

    @staticmethod
    def execute(client, opts):
        stream = open(opts.site_def_file, 'r')
        site_def = yaml.load(stream)
        stream.close()
        return client.update_site(opts.site_name, site_def)


class DTRSAddCredentials(CeiCommand):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The name of the site to be added.')
        parser.add_argument('--definition', dest='credentials_def_file', action='store', help='Set the credentials definition to use.')

    @staticmethod
    def execute(client, opts):
        stream = open(opts.credentials_def_file, 'r')
        credentials_def = yaml.load(stream)
        stream.close()
        return client.add_credentials(opts.caller, opts.site_name, credentials_def)


class DTRSDescribeCredentials(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The site to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_credentials(opts.caller, opts.site_name)


class DTRSListCredentials(CeiCommand):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_credentials(caller=opts.caller)


class DTRSRemoveCredentials(CeiCommand):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The site to remove')

    @staticmethod
    def execute(client, opts):
        return client.remove_credentials(opts.caller, opts.site_name)


class DTRSUpdateCredentials(CeiCommand):

    name = 'update'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('site_name', action='store', help='The name of the site to be updated.')
        parser.add_argument('--definition', dest='credentials_def_file', action='store', help='The credentials definition to use.')

    @staticmethod
    def execute(client, opts):
        stream = open(opts.credentials_def_file, 'r')
        credentials_def = yaml.load(stream)
        stream.close()
        return client.update_credentials(opts.caller, opts.site_name, credentials_def)


class EPUMAdd(CeiCommand):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('domain_id', action='store', help='The name of the domain to be added.')
        parser.add_argument('--definition', dest='definition_id', action='store', help='The name of the domain definition to use.')
        parser.add_argument('--conf', dest='de_conf', action='store', help='Additional configuration for the decision engine.')

    @staticmethod
    def execute(client, opts):
        stream = open(opts.de_conf, 'r')
        conf = yaml.load(stream)
        stream.close()
        return client.add_domain(opts.domain_id, opts.definition_id, conf, caller=opts.caller)


class EPUMRemove(CeiCommand):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('domain_id', action='store', help='The domain to remove')

    @staticmethod
    def execute(client, opts):
        return client.remove_domain(opts.domain_id, caller=opts.caller)


class EPUMDescribe(CeiCommand):

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
        template = Template(EPUMDescribe.output_template)
        print template.render(result=result)


class EPUMList(CeiCommand):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_domains(caller=opts.caller)

    @staticmethod
    def output(result):
        for domain_id in result:
            print domain_id


class EPUMReconfigure(CeiCommand):

    name = 'reconfigure'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('domain_id', action='store', help='The domain to reconfigure')
        parser.add_argument('--bool', dest='updated_kv_bool', action='append', help='Key to modify in the domain configuration with a boolean value')
        parser.add_argument('--int', dest='updated_kv_int', action='append', help='Key to modify in the domain configuration with a integer value')
        parser.add_argument('--string', dest='updated_kv_string', action='append', help='Key to modify in the domain configuration with a string value')

    @staticmethod
    def format_reconfigure(bool_reconfs=[], int_reconfs=[], string_reconfs=[]):
        h = {}
        r = '([a-zA-Z_0-9]+)\.([a-zA-Z_0-9]+)=(.*)'
        for reconf in bool_reconfs or []:
            m = re.match(r, reconf)
            if m:
                if h.has_key(m.group(1)):
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
                if h.has_key(m.group(1)):
                    section = h[m.group(1)]
                else:
                    section = {}

                section[m.group(2)] = int(m.group(3))
                h[m.group(1)] = section

        for reconf in string_reconfs or []:
            m = re.match(r, reconf)
            if m:
                if h.has_key(m.group(1)):
                    section = h[m.group(1)]
                else:
                    section = {}

                section[m.group(2)] = m.group(3)
                h[m.group(1)] = section

        return h

    @staticmethod
    def execute(client, opts):
        updated_kvs = EPUMReconfigure.format_reconfigure(bool_reconfs=opts.updated_kv_bool, int_reconfs=opts.updated_kv_int, string_reconfs=opts.updated_kv_string)
        return client.reconfigure_domain(opts.domain_id, updated_kvs, caller=opts.caller)


class EPUMAddDefinition(CeiCommand):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('definition_id', action='store', help='The name of the domain definition to be added')
        parser.add_argument('--definition', dest='definition', action='store', help='File containing the domain definition description')

    @staticmethod
    def execute(client, opts):
        stream = open(opts.definition, 'r')
        definition = yaml.load(stream)
        stream.close()
        return client.add_domain_definition(opts.definition_id, definition)


class EPUMRemoveDefinition(CeiCommand):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('definition_id', action='store', help='The definition domain to remove')

    @staticmethod
    def execute(client, opts):
        return client.remove_domain_definition(opts.definition_id)


class EPUMDescribeDefinition(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('definition_id', action='store', help='The definition domain to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_domain_definition(opts.definition_id)


class EPUMListDefinitions(CeiCommand):

    name = 'list'

    def __init__(self, subparsers):
        subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_domain_definitions()

    @staticmethod
    def output(result):
        for definition_id in result:
            print definition_id


class EPUMUpdateDefinition(CeiCommand):

    name = 'update'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('definition_id', action='store', help='The domain definition to reconfigure')
        parser.add_argument('--definition', dest='definition', action='store', help='File containing the new domain definition')

    @staticmethod
    def execute(client, opts):
        stream = open(opts.definition, 'r')
        definition = yaml.load(stream)
        stream.close()
        return client.update_domain_definition(opts.definition_id, definition)


class PDCreateProcessDefinition(CeiCommand):

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
            print "Problem reading process specification file %s: %s" % (opts.process_spec, e)
            sys.exit(1)

        return client.create_process_definition(process_definition=process_spec, 
                process_definition_id=opts.definition_id)


class PDDescribeProcessDefinition(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_definition_id', metavar='pd_id')

    @staticmethod
    def execute(client, opts):
        return client.describe_process_definition(opts.process_definition_id)


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
            print "Problem reading process specification file %s: %s" % (opts.process_spec, e)
            sys.exit(1)

        return client.update_process_definition(process_spec, opts.definition_id)


class PDRemoveProcessDefinition(CeiCommand):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_definition_id', metavar='pd_id')

    @staticmethod
    def execute(client, opts):
        return client.remove_process_definition(opts.process_definition_id)


class PDListProcessDefinitions(CeiCommand):

    name = 'list'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.list_process_definitions()


class PDScheduleProcess(CeiCommand):

    name = 'schedule'

    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', metavar='proc_id')
        parser.add_argument('process_definition_id', metavar='pd_id')
        parser.add_argument('configuration', metavar='process_configuration.yml')
        parser.add_argument('--queueing-mode', metavar='queueing_mode')
        parser.add_argument('--restart-mode', metavar='restart_mode')

    @staticmethod
    def execute(client, opts):

        try:
            with open(opts.configuration) as f:
                configuration = yaml.load(f)
        except Exception, e:
            print "problem reading process configuration file %s: %s" % (opts.configuration, e)
            sys.exit(1)
        return client.schedule_process(opts.process_id, opts.process_definition_id,
                configuration=configuration, queueing_mode=opts.queueing_mode,
                restart_mode=opts.restart_mode)


class PDDescribeProcesses(CeiCommand):

    name = 'list'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.describe_processes()


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

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', action='store', help='The UPID of the process to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_process(opts.process_id)


class PDWaitProcess(CeiCommand):

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
            process = client.describe_process(opts.process_id)

            if process:
                state = process['state']

                if state in ("500-RUNNING", "800-EXITED"):
                    return process

                if state in ("850-FAILED", "900-REJECTED"):
                    print "FAILED. Process in %s state" % state
                    sys.exit(1)

            if time.time() + opts.poll >= deadline:
                print "Timed out waiting for process %s" % opts.process_id
                sys.exit(1)
            time.sleep(opts.poll)


class PDDump(CeiCommand):

    name = 'dump'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)

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
            print "Problem reading process specification file %s: %s" % (opts.process_spec, e)
            sys.exit(1)

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
            print "Problem reading process specification file %s: %s" % (opts.process_spec, e)
            sys.exit(1)

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

        parser = subparsers.add_parser(self.name)

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
            print "Problem reading process schedule file %s: %s" % (opts.schedule, e)
            sys.exit(1)

        try:
            with open(opts.configuration) as f:
                configuration = yaml.load(f)
        except exception, e:
            print "problem reading process configuration file %s: %s" % (opts.configuration, e)
            sys.exit(1)
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
        parser = subparsers.add_parser(self.name)

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
                    print "FAILED. Process in %s state" % ProcessStateEnum.to_str(state)
                    sys.exit(1)

            if time.time() + opts.poll >= deadline:
                print "Timed out waiting for process %s" % opts.process_id
                sys.exit(1)
            time.sleep(opts.poll)


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
            print "problem reading policy file %s: %s" % (opts.policy, e)
            sys.exit(1)
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
            print "Problem reading provisioning variables file %s: %s" % (opts.provisioning_var_file, e)
            sys.exit(1)

        # Update the provisioning variables with secret RabbitMQ credentials
        vars['broker_ip_address'] = client._connection.amqp_broker
        vars['broker_username'] = client._connection.amqp_username
        vars['broker_password'] = client._connection.amqp_password

        return client.provision(opts.deployable_type, opts.site, opts.allocation, vars, caller=opts.caller)


class ProvisionerTerminateAll(CeiCommand):

    name = 'terminate_all'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)

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
