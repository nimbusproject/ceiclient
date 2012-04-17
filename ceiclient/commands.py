import json
import os
import pprint
import re
import sys
import uuid

import kombu
from jinja2 import Template
import yaml

class CeiCommand(object):

    def __init__(self, subparsers):
        pass

    @staticmethod
    def output(result):
        pprint.pprint(result)


class EPUMAdd(CeiCommand):

    name = 'add'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('epu_name', action='store', help='The name of the EPU to be added.')
        parser.add_argument('--conf', dest='de_conf', action='store', help='Set the type of decision engine to use.')

    @staticmethod
    def execute(client, opts):
        stream = open(opts.de_conf, 'r')
        conf = yaml.load(stream)
        stream.close()
        return client.add_epu(opts.epu_name, conf, caller=opts.caller)

class EPUMRemove(CeiCommand):

    name = 'remove'

    def __init__(self, subparsers):
        parser = subparsers.add_parser(self.name)
        parser.add_argument('epu_name', action='store', help='The EPU to remove')

    @staticmethod
    def execute(client, opts):
        return client.remove_epu(opts.epu_name, caller=opts.caller)


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
        return client.describe_epu(opts.epu_name, caller=opts.caller)

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
        return client.list_epus(caller=opts.caller)

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
        return client.reconfigure_epu(opts.epu_name, updated_kvs, caller=opts.caller)

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
            print "Problem reading process specification file %s: %s" % (opts.process_spec, e)
            sys.exit(1)

        return client.dispatch_process(str(uuid.uuid4().hex), process_spec, None, None, opts.immediate)

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
