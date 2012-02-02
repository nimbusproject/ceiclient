import uuid

from jinja2 import Template
import yaml

class CeiCommand(object):

    def __init__(self, subparsers):
        pass

class EPUMDescribe(CeiCommand):

    name = 'describe'
    output_template = '''Name:                    {{ result.name }}
Engine configuration:    EPU worker type = {{ result.config.engine_conf.epuworker_type }}
                         Preserve N      = {{ result.config.engine_conf.preserve_n }}
General:                 Engine class    = {{ result.config.general.engine_class }}
Health:                  Monitor health  = {{result.config.health.monitor_health}}'''

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('epu_name', action='store', help='The EPU to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_epu(opts.epu_name)

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
        return client.list_epus()

    @staticmethod
    def output(result):
        for epu_name in result:
            print epu_name

class EPUMReconfigure(CeiCommand):

    name = 'reconfigure'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('epu_name', action='store', help='The EPU to reconfigure')
        parser.add_argument('--bool', dest='updated_kv_bool', action='append', help='Key to modify in the EPU configuration with a boolean value')
        parser.add_argument('--int', dest='updated_kv_int', action='append', help='Key to modify in the EPU configuration with a integer value')
        parser.add_argument('--string', dest='updated_kv_string', action='append', help='Key to modify in the EPU configuration with a string value')

    @staticmethod
    def _TODO_hash_helper():
        pass

    @staticmethod
    def execute(client, opts):
        # TODO Create a hash for the merge
        return client.reconfigure_epu(opts.epu_name, opts.updated_key_value)

    @staticmethod
    def output(result):
        print(result)

class PDDispatch(CeiCommand):

    name = 'dispatch'
    def __init__(self, subparsers):

        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_spec', metavar='process_spec.yml')
        parser.add_argument('--immediate', '-i', action='store_const', const=True, default=False)

    @staticmethod
    def execute(client, opts):
        try:
            with open(opts.process_spec) as f:
                process_spec = yaml.load(f)
        except Exception, e:
            print "Problem reading process specification file %s: %s" % e
            sys.exit(1)

        return client.dispatch_process(str(uuid.uuid4().hex), process_spec, None, None, opts.immediate)

    @staticmethod
    def output(result):
        print(result)

class PDDescribeProcesses(CeiCommand):

    name = 'list'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.describe_processes()

    @staticmethod
    def output(result):
        print(result)

class PDTerminateProcess(CeiCommand):

    name = 'kill'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', action='store', help='The PID of the process to kill')

    @staticmethod
    def execute(client, opts):
        return client.terminate_process(opts.process_id)

    @staticmethod
    def output(result):
        print(result)

class PDDescribeProcess(CeiCommand):

    name = 'describe'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('process_id', action='store', help='The PID of the process to describe')

    @staticmethod
    def execute(client, opts):
        return client.describe_process(opts.process_id)

    @staticmethod
    def output(result):
        print(result)

class PDDump(CeiCommand):

    name = 'dump'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)

    @staticmethod
    def execute(client, opts):
        return client.dump()

    @staticmethod
    def output(result):
        print(result)

# TODO Other dashi calls for the PD:
#dt_state
#heartbeat, sender_kwarg='sender'
