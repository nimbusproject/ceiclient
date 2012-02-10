from mock import Mock, patch
from nose.tools import raises
import argparse
import pprint

from ceiclient.commands import EPUMDescribe, EPUMList, EPUMReconfigure, EPUMAdd, EPUMRemove
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

    def test_command_parsing_epum_add_ok(self):
        EPUMAdd(self.subparsers)
        de_conf = "XXX"
        opts = self.parser.parse_args(['add', 'epu2', "--conf", de_conf])
        assert opts.de_conf == "XXX"
        assert opts.epu_name == 'epu2'

    def test_command_parsing_epum_remove_ok(self):
        EPUMRemove(self.subparsers)
        opts = self.parser.parse_args(['remove', 'epu2'])
        assert opts.epu_name == 'epu2'

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
        boolkvpair1 = 'some.key=false'
        boolkvpair2 = 'some.other_key=true'
        intkvpair1 = 'one.key=42'
        intkvpair2 = 'another.key=24'
        stringkvpair1 = 'a.string=a_string'
        stringkvpair2 = 'a.string_again=another_string'
        opts = self.parser.parse_args(['reconfigure', 'epu1', '--bool',
            boolkvpair1, '--int', intkvpair1, '--bool', boolkvpair2,
            '--string', stringkvpair1, '--string', stringkvpair2, '--int',
            intkvpair2])

        assert opts.command == 'reconfigure'
        assert opts.epu_name == 'epu1'
        assert opts.updated_kv_bool == [boolkvpair1, boolkvpair2]
        assert opts.updated_kv_int == [intkvpair1, intkvpair2]
        assert opts.updated_kv_string == [stringkvpair1, stringkvpair2]

        reconfiguration = EPUMReconfigure.format_reconfigure(bool_reconfs=opts.updated_kv_bool,
                                             int_reconfs=opts.updated_kv_int,
                                             string_reconfs=opts.updated_kv_string)
        assert reconfiguration == {
                'some': {
                    'key': False,
                    'other_key': True,
                },
                'one': {
                    'key': 42,
                },
                'another': {
                    'key': 24,
                },
                'a': {
                    'string': 'a_string',
                    'string_again': 'another_string',
                },
        }


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
