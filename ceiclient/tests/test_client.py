from mock import Mock, patch

from ceiclient.client import EPUMClient

def test_create_epum_client():
    conn = Mock()
    epum_client = EPUMClient(conn)
    assert epum_client.dashi_name == 'epu_management_service'
