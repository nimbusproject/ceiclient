import os
import yaml
import uuid
import tempfile
import subprocess
import unittest

from nose.plugins.skip import SkipTest

try:
    from epuharness.fixture import TestFixture
except ImportError:
    raise SkipTest("EPUHarness must be installed for integration tests")


basic_deployment = """
process-dispatchers:
  process_dispatcher:
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
    'real-site': {
      'iaas_image': 'r2-worker',
      'iaas_allocation': 'm1.large',
    },
    'ec2-fake': {
      'iaas_image': 'ami-fake',
      'iaas_allocation': 't1.micro',
    }
  },
  'contextualization': {
    'method': 'chef-solo',
    'chef_config': {}
  }
}


class TestIntegration(TestFixture, unittest.TestCase):

    def setUp(self):
        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.deployment = basic_deployment % {"default_user": default_user}

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.user = default_user

        self.epuh_persistence = "/tmp/SupD/epuharness"
        if os.path.exists(self.epuh_persistence):
            raise SkipTest("EPUHarness running. Can't run this test")

        # Set up fake libcloud and start deployment
        self.fake_site, _ = self.make_fake_libcloud_site()
        self.fake_site_name = "fake_site"

        self.setup_harness(exchange=self.exchange)
        self.addCleanup(self.teardown_harness)

        self.epuharness.start(deployment_str=self.deployment)

        clients = self.get_clients(self.deployment, self.dashi)
        self.provisioner_client = clients['prov_0']
        self.epum_client = clients['epum_0']
        self.dtrs_client = clients['dtrs']

        self.block_until_ready(self.deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, dt_name, example_dt)
        self.dtrs_client.add_site(self.fake_site_name, self.fake_site)
        self.dtrs_client.add_credentials(self.user, self.fake_site_name, fake_credentials)

    def test_process_definitions(self):

        cmd = "ceictl -Y -x %s process-definition list" % self.exchange
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

        cmd = "ceictl -Y -x %s process-definition list" % (self.exchange)
        out = subprocess.check_output(cmd, shell=True)
        parsed_return = yaml.load(out.rstrip())
        assert len(parsed_return) == 1

        cmd = "ceictl -Y -x %s process schedule --definition-id %s --config %s" % (self.exchange,
                definition_name, process_config_path)
        out = subprocess.check_output(cmd, shell=True)

        cmd = "ceictl -Y -x %s process list" % (self.exchange)
        out = subprocess.check_output(cmd, shell=True)
        parsed_return = yaml.load(out.rstrip())
        assert len(parsed_return) == 1

        cmd = "ceictl -Y -x %s process-definition remove %s" % (
                self.exchange, definition_name)
        out = subprocess.check_output(cmd, shell=True)

        cmd = "ceictl -Y -x %s process-definition list" % (self.exchange)
        out = subprocess.check_output(cmd, shell=True)
        parsed_return = yaml.load(out.rstrip())
        assert len(parsed_return) == 0

    def test_dtrs_dts(self):
        cmd = "ceictl -x %s -c %s dt list" % (self.exchange, self.user)
        out = subprocess.check_output(cmd, shell=True)
        self.assertEqual(out.rstrip(), dt_name)

        missing_dt_name = "nonexistent"
        cmd = "ceictl -x %s -c %s dt remove %s" % (self.exchange, self.user, missing_dt_name)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            self.assertEqual(e.returncode, 1)
            self.assertEqual(e.output.rstrip(), "Error: Caller default has no DT named %s" % missing_dt_name)
        else:
            self.fail("Expected failure to remove a nonexistent DT")

        new_dt_name = dt_name + "_bis"
        cmd = "ceictl -x %s -c %s dt add %s" % (self.exchange, self.user, new_dt_name)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            self.assertEqual(e.returncode, 1)
            self.assertEqual(e.output.rstrip(), "Error: The --definition argument is missing")
        else:
            self.fail("Expected failure to add a DT with no definition argument")

        cmd = "ceictl -x %s -c %s dt update %s" % (self.exchange, self.user, dt_name)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            self.assertEqual(e.returncode, 1)
            self.assertEqual(e.output.rstrip(), "Error: The --definition argument is missing")
        else:
            self.fail("Expected failure to update a DT with no definition argument")

        dt_file = tempfile.NamedTemporaryFile(delete=False)
        dt_file.write(yaml.safe_dump(example_dt, default_flow_style=False))
        dt_file.close()

        try:
            cmd = "ceictl -x %s -c %s dt add %s --definition %s" % (self.exchange, self.user, new_dt_name, dt_file.name)
            out = subprocess.check_output(cmd, shell=True)
            self.assertEqual(out.rstrip(), "Added DT %s for user %s" % (new_dt_name, self.user))

            cmd = "ceictl -x %s -c %s dt add %s --definition %s" % (self.exchange, self.user, new_dt_name, dt_file.name)
            try:
                subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
            except subprocess.CalledProcessError as e:
                self.assertEqual(e.returncode, 1)
                self.assertEqual(e.output.rstrip(), "Error: DT %s already exists" % new_dt_name)
            else:
                self.fail("Expected failure to add DT that already exists")

            cmd = "ceictl -x %s -c %s dt add --force %s --definition %s" % (self.exchange, self.user, new_dt_name, dt_file.name)
            out = subprocess.check_output(cmd, shell=True)
            self.assertEqual(out.rstrip(), "Updated DT %s for user %s" % (new_dt_name, self.user))

            cmd = "ceictl -x %s -c %s dt update %s --definition %s" % (self.exchange, self.user, new_dt_name, dt_file.name)
            out = subprocess.check_output(cmd, shell=True)
            self.assertEqual(out.rstrip(), "Updated DT %s for user %s" % (new_dt_name, self.user))
        finally:
            os.remove(dt_file.name)

        cmd = "ceictl -x %s -c %s dt list" % (self.exchange, self.user)
        out = subprocess.check_output(cmd, shell=True)
        self.assertIn(dt_name, out.split("\n"))
        self.assertIn(new_dt_name, out.split("\n"))

        cmd = "ceictl -x %s -c %s dt remove %s" % (self.exchange, self.user, new_dt_name)
        out = subprocess.check_output(cmd, shell=True)
        self.assertEqual(out.rstrip(), "Removed DT %s for user %s" % (new_dt_name, self.user))

        cmd = "ceictl -x %s -c %s dt list" % (self.exchange, self.user)
        out = subprocess.check_output(cmd, shell=True)
        self.assertEqual(out.rstrip(), dt_name)

    def test_dtrs_credentials(self):
        site_name = self.fake_site_name

        cmd = "ceictl -x %s -c %s credentials list" % (self.exchange, self.user)
        out = subprocess.check_output(cmd, shell=True)
        self.assertEqual(out.rstrip(), site_name)

        missing_site_name = "nonexistent"
        cmd = "ceictl -x %s -c %s credentials remove %s" % (self.exchange, self.user, missing_site_name)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            self.assertEqual(e.returncode, 1)
            self.assertEqual(e.output.rstrip(), "Error: Credentials '%s' not found for user %s and type site" % (missing_site_name, self.user))
        else:
            self.fail("Expected failure to remove nonexistent credentials")

        new_site_name = site_name + "_bis"
        new_site = self.fake_site
        new_site['name'] = new_site_name

        cmd = "ceictl -x %s -c %s credentials add %s" % (self.exchange, self.user, new_site_name)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            self.assertEqual(e.returncode, 1)
            self.assertEqual(e.output.rstrip(), "Error: The --definition argument is missing")
        else:
            self.fail("Expected failure to add credentials with no definition argument")

        cmd = "ceictl -x %s -c %s credentials update %s" % (self.exchange, self.user, new_site_name)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            self.assertEqual(e.returncode, 1)
            self.assertEqual(e.output.rstrip(), "Error: The --definition argument is missing")
        else:
            self.fail("Expected failure to update credentials with no definition argument")

        credentials_file = tempfile.NamedTemporaryFile(delete=False)
        credentials_file.write(yaml.safe_dump(fake_credentials, default_flow_style=False))
        credentials_file.close()

        # Credentials cannot be added without a corresponding site
        # Test for failure first
        cmd = "ceictl -x %s -c %s credentials add %s --definition %s" % (self.exchange, self.user, new_site_name, credentials_file.name)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            self.assertEqual(e.returncode, 1)
            self.assertEqual(e.output.rstrip(), "Error: Cannot add credentials for unknown site %s" % new_site_name)
        else:
            self.fail("Expected failure to add credentials with no definition argument")

        # Now add the site
        site_file = tempfile.NamedTemporaryFile(delete=False)
        site_file.write(yaml.safe_dump(new_site, default_flow_style=False))
        site_file.close()

        try:
            cmd = "ceictl -x %s site add %s --definition %s" % (self.exchange, new_site_name, site_file.name)
            subprocess.check_output(cmd, shell=True)
        finally:
            os.remove(site_file.name)

        # We can now test credentials for this site
        try:
            cmd = "ceictl -x %s -c %s credentials add %s --definition %s" % (self.exchange, self.user, new_site_name, credentials_file.name)
            out = subprocess.check_output(cmd, shell=True)
            self.assertEqual(out.rstrip(), "Added credentials of site %s for user %s" % (new_site_name, self.user))

            cmd = "ceictl -x %s -c %s credentials add %s --definition %s" % (self.exchange, self.user, new_site_name, credentials_file.name)
            try:
                subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
            except subprocess.CalledProcessError as e:
                self.assertEqual(e.returncode, 1)
                self.assertEqual(e.output.rstrip(), "Error: Credentials '%s' of type 'site' already exist" % new_site_name)
            else:
                self.fail("Expected failure to add site credentials that already exist")

            cmd = "ceictl -x %s -c %s credentials add --force %s --definition %s" % (self.exchange, self.user, new_site_name, credentials_file.name)
            out = subprocess.check_output(cmd, shell=True)
            self.assertEqual(out.rstrip(), "Updated credentials of site %s for user %s" % (new_site_name, self.user))

            cmd = "ceictl -x %s -c %s credentials update %s --definition %s" % (self.exchange, self.user, new_site_name, credentials_file.name)
            out = subprocess.check_output(cmd, shell=True)
            self.assertEqual(out.rstrip(), "Updated credentials of site %s for user %s" % (new_site_name, self.user))
        finally:
            os.remove(credentials_file.name)

        cmd = "ceictl -x %s -c %s credentials list" % (self.exchange, self.user)
        out = subprocess.check_output(cmd, shell=True)
        self.assertIn(site_name, out.split("\n"))
        self.assertIn(new_site_name, out.split("\n"))

        cmd = "ceictl -x %s -c %s credentials remove %s" % (self.exchange, self.user, new_site_name)
        out = subprocess.check_output(cmd, shell=True)
        self.assertEqual(out.rstrip(), "Removed credentials of site %s for user %s" % (new_site_name, self.user))

        cmd = "ceictl -x %s -c %s credentials list" % (self.exchange, self.user)
        out = subprocess.check_output(cmd, shell=True)
        self.assertEqual(out.rstrip(), site_name)

    def test_dtrs_sites(self):
        site_name = self.fake_site_name

        cmd = "ceictl -x %s site list" % self.exchange
        out = subprocess.check_output(cmd, shell=True)
        self.assertEqual(out.rstrip(), site_name)

        missing_site_name = "nonexistent"
        cmd = "ceictl -x %s site remove %s" % (self.exchange, missing_site_name)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            self.assertEqual(e.returncode, 1)
            self.assertEqual(e.output.rstrip(), "Error: No site named %s" % missing_site_name)
        else:
            self.fail("Expected failure to remove a nonexistent site")

        new_site_name = site_name + "_bis"
        new_site = self.fake_site
        new_site['name'] = new_site_name

        cmd = "ceictl -x %s site add %s" % (self.exchange, new_site_name)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            self.assertEqual(e.returncode, 1)
            self.assertEqual(e.output.rstrip(), "Error: The --definition argument is missing")
        else:
            self.fail("Expected failure to add site with no definition argument")

        cmd = "ceictl -x %s site update %s" % (self.exchange, new_site_name)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            self.assertEqual(e.returncode, 1)
            self.assertEqual(e.output.rstrip(), "Error: The --definition argument is missing")
        else:
            self.fail("Expected failure to update site with no definition argument")

        site_file = tempfile.NamedTemporaryFile(delete=False)
        site_file.write(yaml.safe_dump(new_site, default_flow_style=False))
        site_file.close()

        try:
            cmd = "ceictl -x %s site add %s --definition %s" % (self.exchange, new_site_name, site_file.name)
            out = subprocess.check_output(cmd, shell=True)
            self.assertEqual(out.rstrip(), "Added site %s" % new_site_name)

            cmd = "ceictl -x %s site add %s --definition %s" % (self.exchange, new_site_name, site_file.name)
            try:
                subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
            except subprocess.CalledProcessError as e:
                self.assertEqual(e.returncode, 1)
                self.assertEqual(e.output.rstrip(), "Error: Site %s already exists" % new_site_name)
            else:
                self.fail("Expected failure to add a site that already exists")

            cmd = "ceictl -x %s site add %s --force --definition %s" % (self.exchange, new_site_name, site_file.name)
            out = subprocess.check_output(cmd, shell=True)
            self.assertEqual(out.rstrip(), "Updated site %s" % new_site_name)

            cmd = "ceictl -x %s site update %s --definition %s" % (self.exchange, new_site_name, site_file.name)
            out = subprocess.check_output(cmd, shell=True)
            self.assertEqual(out.rstrip(), "Updated site %s" % new_site_name)
        finally:
            os.remove(site_file.name)

        cmd = "ceictl -x %s site list" % self.exchange
        out = subprocess.check_output(cmd, shell=True)
        self.assertIn(site_name, out.split("\n"))
        self.assertIn(new_site_name, out.split("\n"))

        cmd = "ceictl -x %s site remove %s" % (self.exchange, new_site_name)
        out = subprocess.check_output(cmd, shell=True)
        self.assertEqual(out.rstrip(), "Removed site %s" % new_site_name)

        cmd = "ceictl -x %s site list" % self.exchange
        out = subprocess.check_output(cmd, shell=True)
        self.assertEqual(out.rstrip(), site_name)
