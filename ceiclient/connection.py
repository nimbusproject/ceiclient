from dashi import DashiConnection
from dashi.bootstrap import DEFAULT_EXCHANGE

class CeiConnection(object):
    """Abstract class defining the interface to talk with CEI services"""

    def call(self, service, operation, **kwargs):
        pass

class DashiCeiConnection(CeiConnection):

    _name = 'ceiclient'

    def __init__(self, broker, username, password, exchange=None):
        self.amqp_broker = broker
        self.amqp_username = username
        self.amqp_password = password
        self.amqp_port = 5672
        self.amqp_exchange = exchange or DEFAULT_EXCHANGE

        self.dashi_connection = DashiConnection(self._name,
                'amqp://%s:%s@%s:%s//' % (self.amqp_username,
                    self.amqp_password, self.amqp_broker,
                    self.amqp_port), self.amqp_exchange)

    def call(self, service, operation, timeout=5, **kwargs):
        return self.dashi_connection.call(service, operation, timeout, **kwargs)

    def disconnect(self):
        self.dashi_connection.disconnect()
