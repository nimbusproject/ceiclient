import unittest

from mock import Mock, patch

from ceiclient.client import EPUMClient


class TestEPUMClient(unittest.TestCase):

    def setUp(self):
        self.conn = Mock()
        self.epum_client = EPUMClient(self.conn)

    def test_epum_service_name(self):
        self.assertEqual(self.epum_client.dashi_name, 'epu_management_service')

    def test_list_domains(self):
        self.conn.call = Mock(return_value=[])
        self.assertEqual(len(self.epum_client.list_domains()), 0)
        self.conn.call.assert_called_once_with(self.epum_client.dashi_name, 'list_domains', caller=None)

        caller= "asterix"
        self.conn.call = Mock(return_value=[])
        self.assertEqual(len(self.epum_client.list_domains(caller=caller)), 0)
        self.conn.call.assert_called_once_with(self.epum_client.dashi_name, 'list_domains', caller=caller)

    def test_add_remove_domain(self):
        self.conn.call = Mock(return_value=None)
        self.assertEqual(self.epum_client.add_domain("domain", "definition", {}), None)

        self.conn.call = Mock(return_value=None)
        self.assertEqual(self.epum_client.remove_domain("domain"), None)

    def test_add_remove_definition(self):
        self.conn.call = Mock(return_value=None)
        self.assertEqual(self.epum_client.add_domain_definition("definition", {}), None)

        self.conn.call = Mock(return_value=None)
        self.assertEqual(self.epum_client.remove_domain_definition("definition"), None)
