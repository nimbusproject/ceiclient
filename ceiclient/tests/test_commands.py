from mock import Mock, patch
from nose.tools import raises
import argparse
import pprint

from ceiclient.commands import EPUMDescribe, EPUMList, EPUMReconfigure
from ceiclient.commands import PDDispatch

class TestCommandParsing:

    def setUp(self):
        self.parser = argparse.ArgumentParser()
        self.subparsers = self.parser.add_subparsers(dest='command')

    def test_command_parsing_epum_describe_ok(self):
        EPUMDescribe(self.subparsers)
        opts = self.parser.parse_args(['describe', 'epu1'])
        assert opts.command == 'describe'
        assert opts.epu_name == 'epu1'

    @raises(SystemExit)
    def test_command_parsing_epum_describe_failing_wrong_command(self):
        EPUMDescribe(self.subparsers)
        opts = self.parser.parse_args(['list', 'epu1'])

    @raises(SystemExit)
    def test_command_parsing_epum_describe_failing_missing_argument(self):
        EPUMDescribe(self.subparsers)
        opts = self.parser.parse_args(['describe'])

    def test_command_parsing_epum_list_ok(self):
        EPUMList(self.subparsers)
        opts = self.parser.parse_args(['list'])
        assert opts.command == 'list'

    @raises(SystemExit)
    def test_command_parsing_epum_list_failing(self):
        EPUMList(self.subparsers)
        opts = self.parser.parse_args(['describe'])

    def test_command_parsing_epum_reconfigure_ok(self):
        EPUMReconfigure(self.subparsers)
        boolkvpair = 'health.monitor_health=false'
        intkvpair = 'engine_conf.preserve_n=42'
        stringkvpair = 'general.engine_class=epu.decisionengine.impls.simplest.SimplestEngine'
        opts = self.parser.parse_args(['reconfigure', 'epu1', '--bool', boolkvpair, '--int', intkvpair, '--string', stringkvpair])

        assert opts.command == 'reconfigure'
        assert opts.epu_name == 'epu1'
        assert opts.updated_kv_bool == [boolkvpair]
        assert opts.updated_kv_int == [intkvpair]
        assert opts.updated_kv_string == [stringkvpair]

    @raises(SystemExit)
    def test_command_parsing_epum_reconfigure_failing_wrong_command(self):
        EPUMReconfigure(self.subparsers)
        opts = self.parser.parse_args(['describe', 'epu1', '42'])

    @raises(SystemExit)
    def test_command_parsing_epum_reconfigure_failing_not_enough_arguments(self):
        EPUMReconfigure(self.subparsers)
        opts = self.parser.parse_args(['describe', 'epu1'])

    @raises(SystemExit)
    def test_command_parsing_epum_reconfigure_failing_wrong_arguments(self):
        EPUMReconfigure(self.subparsers)
        opts = self.parser.parse_args(['describe', 'epu1', 'notanumber'])
