import sys
from dashi import DashiConnection
from dashi.bootstrap import DEFAULT_EXCHANGE


class CeiConnection(object):
    """Abstract class defining the interface to talk with CEI services"""

    def call(self, service, operation, **kwargs):
        pass


class DashiCeiConnection(CeiConnection):

    _name = 'ceiclient'

    def __init__(self, broker, username, password, exchange=None, timeout=None, port=5672, ssl=False):
        self.amqp_broker = broker
        self.amqp_username = username
        self.amqp_password = password
        self.amqp_port = port
        self.amqp_exchange = exchange or DEFAULT_EXCHANGE
        self.timeout = timeout

        self.dashi_connection = DashiConnection(self._name,
                'amqp://%s:%s@%s:%s//' % (self.amqp_username,
                    self.amqp_password, self.amqp_broker,
                    self.amqp_port), self.amqp_exchange, ssl=ssl)

    def call(self, service, operation, **kwargs):
        return self.dashi_connection.call(service, operation, self.timeout, **kwargs)

    def fire(self, service, operation, **kwargs):
        return self.dashi_connection.fire(service, operation, **kwargs)

    def disconnect(self):
        self.dashi_connection.disconnect()


class PyonCeiConnection(CeiConnection):

    _name = 'ceiclient'

    def __init__(self, broker, username, password, vhost='/',
            sysname=None, timeout=None, port=5672, ssl=False):

        try:
            from pyon.net.messaging import make_node
            from pyon.net.endpoint import RPCClient
            from pyon.util.containers import get_default_sysname
        except ImportError:
            err = "Pyon isn't available in your environment"
            sys.exit(err)

        self.RPCClient = RPCClient

        self.connection_params = {
            'host': broker,
            'username': username,
            'password': password,
            'vhost': vhost,
            'port': port
        }
        self.timeout = timeout

        self.sysname = sysname or get_default_sysname()

        node, ioloop = make_node(connection_params=self.connection_params,
                timeout=self.timeout)

        interceptor_config = {
            'interceptors': {
                'encode': {
                    'class': 'pyon.core.interceptor.encode.EncodeInterceptor'
                }
            },
            'stack': {
                'message_incoming': ['encode'],
                'message_outgoing': ['encode']
            }
        }
        node.setup_interceptors(interceptor_config)

        self.pyon_node = node
        self.pyon_ioloop = ioloop

    def call(self, service, operation, **kwargs):
        to_name = (self.sysname, service)
        client = self.RPCClient(node=self.pyon_node, to_name=to_name)
        return client.request(kwargs, op=operation)

    def fire(self, service, operation, **kwargs):
        to_name = (self.sysname, service)
        client = self.RPCClient(node=self.pyon_node, to_name=to_name)
        return client.request(kwargs, op=operation)

    def disconnect(self):
        self.pyon_node.stop_node()
        self.pyon_ioloop.kill()
