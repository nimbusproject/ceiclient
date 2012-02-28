from mock import Mock, patch
import dashi

from ceiclient.connection import DashiCeiConnection

def test_set_up_dashi_cei_connection():
    server = 'localhost'
    username = 'guest'
    password = 'guest'

    with patch('ceiclient.connection.DashiConnection') as mock:
        dashi_cei_conn = DashiCeiConnection(server, username, password)
        mock.assert_called_with('ceiclient', 'amqp://%s:%s@%s:5672//' %
                (username, password, server), dashi.bootstrap.DEFAULT_EXCHANGE, ssl=False)
