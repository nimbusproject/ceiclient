import os
import yaml
import uuid
import tempfile
import subprocess

from nose.plugins.skip import Skip, SkipTest

try:
    from epuharness.fixture import TestFixture
    from epuharness.harness import EPUHarness
except ImportError:
    raise SkipTest("EPUHarness must be installed for integration tests")


basic_deployment = """
process-dispatchers:
  processdispatcher:
    config:
      processdispatcher:
        engines:
          default:
            deployable_type: eeagent
            slots: 4
            base_need: 1
epums:
  epum_0:
    config:
      epumanagement:
        default_user: %(default_user)s
        provisioner_service_name: prov_0
      logging:
        handlers:
          file:
            filename: /tmp/epum_0.log
provisioners:
  prov_0:
    config:
      ssl_no_host_check: True
      provisioner:
        default_user: %(default_user)s
dt_registries:
  dtrs:
    config: {}
"""

default_user = "default"

fake_credentials = {
  'access_key': 'xxx',
  'secret_key': 'xxx',
  'key_name': 'ooi'
}

dt_name = "example"
example_dt = {
  'mappings': {
    'real-site':{
      'iaas_image': 'r2-worker',
      'iaas_allocation': 'm1.large',
    },
    'ec2-fake':{
      'iaas_image': 'ami-fake',
      'iaas_allocation': 't1.micro',
    }
  },
  'contextualization':{
    'method': 'chef-solo',
    'chef_config': {}
  }
}

class TestIntegration(TestFixture):

    def setup(self):
        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.deployment = basic_deployment % {"default_user" : default_user}

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.user = default_user

        self.epuh_persistence = "/tmp/SupD/epuharness"
        if os.path.exists(self.epuh_persistence):
            raise SkipTest("EPUHarness running. Can't run this test")

        # Set up fake libcloud and start deployment
        self.fake_site = self.make_fake_libcloud_site()

        self.epuharness = EPUHarness(exchange=self.exchange)
        self.dashi = self.epuharness.dashi

        self.epuharness.start(deployment_str=self.deployment)

        clients = self.get_clients(self.deployment, self.dashi)
        self.provisioner_client = clients['prov_0']
        self.epum_client = clients['epum_0']
        self.dtrs_client = clients['dtrs']

        self.block_until_ready(self.deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, dt_name, example_dt)
        self.dtrs_client.add_site(self.fake_site['name'], self.fake_site)
        self.dtrs_client.add_credentials(self.user, self.fake_site['name'], fake_credentials)

    def teardown(self):
        self.epuharness.stop()
        os.remove(self.fake_libcloud_db)
            

    def test_process_definitions(self):

        cmd = "ceictl -x %s process-definition list" % self.exchange
        out = subprocess.check_output(cmd, shell=True)
        assert out.rstrip() == '[]'

        definition = """---
definition_type: process
name: myproc
description: test proc
executable:
    class: something
    module: else"""
        fh, definition_file_path = tempfile.mkstemp()
        with os.fdopen(fh, 'w') as d:
            d.write(definition)
        definition_name = "proc"

        process_config = "{}"
        fh, process_config_path = tempfile.mkstemp()
        with os.fdopen(fh, 'w') as p:
            p.write(process_config)

        cmd = "ceictl -x %s process-definition create -i %s %s" % (
                self.exchange, definition_name, definition_file_path)
        out = subprocess.check_output(cmd, shell=True)

        os.remove(definition_file_path)

        cmd = "ceictl -Y -x %s process-definition describe %s" % (
                self.exchange, definition_name)
        out = subprocess.check_output(cmd, shell=True)
        parsed_return = yaml.load(out.rstrip())
        assert parsed_return['definition_type'] == 'process'
        assert parsed_return['name'] == 'myproc'

        cmd = "ceictl -x %s process-definition list" % (self.exchange)
        out = subprocess.check_output(cmd, shell=True)
        parsed_return = yaml.load(out.rstrip())
        assert len(parsed_return) == 1

        cmd = "ceictl -x %s process schedule %s %s %s" % (self.exchange,
                str(uuid.uuid4()), definition_name, process_config_path)
        out = subprocess.check_output(cmd, shell=True)

        cmd = "ceictl -x %s process list" % (self.exchange)
        out = subprocess.check_output(cmd, shell=True)
        parsed_return = yaml.load(out.rstrip())
        assert len(parsed_return) == 1

        cmd = "ceictl -Y -x %s process-definition remove %s" % (
                self.exchange, definition_name)
        out = subprocess.check_output(cmd, shell=True)

        cmd = "ceictl -x %s process-definition list" % (self.exchange)
        out = subprocess.check_output(cmd, shell=True)
        parsed_return = yaml.load(out.rstrip())
        assert len(parsed_return) == 0
