from dashi import DashiConnection
from dashi.bootstrap import DEFAULT_EXCHANGE

class CeiConnection(object):
    """Abstract class defining the interface to talk with CEI services"""

    def call(self, service, operation, **kwargs):
        pass

class DashiCeiConnection(CeiConnection):

    _name = 'ceiclient'

    def __init__(self, broker, username, password, exchange=None, timeout=None, port=5672):
        self.amqp_broker = broker
        self.amqp_username = username
        self.amqp_password = password
        self.amqp_port = port
        self.amqp_exchange = exchange or DEFAULT_EXCHANGE
        self.timeout = timeout

        self.dashi_connection = DashiConnection(self._name,
                'amqp://%s:%s@%s:%s//' % (self.amqp_username,
                    self.amqp_password, self.amqp_broker,
                    self.amqp_port), self.amqp_exchange)

    def call(self, service, operation, **kwargs):
        return self.dashi_connection.call(service, operation, self.timeout, **kwargs)

    def fire(self, service, operation, **kwargs):
        return self.dashi_connection.fire(service, operation, **kwargs)

    def disconnect(self):
        self.dashi_connection.disconnect()
