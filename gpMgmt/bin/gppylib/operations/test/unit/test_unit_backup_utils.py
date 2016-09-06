#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2016. All Rights Reserved.
#

import os
import shutil
import unittest2 as unittest
from gppylib.commands.base import CommandResult
from gppylib.operations.backup_utils import *

from mock import call, mock_open, patch, MagicMock, Mock
from optparse import Values

class BackupUtilsTestCase(unittest.TestCase):

    def setUp(self):
        self.context = Context()
        self.context.master_datadir = '/data'
        self.context.timestamp = '20160101010101'
        self.netbackup_filepath = "/tmp/db_dumps/foo"

    def test_generate_filename_schema(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_schema'
        output = self.context.generate_filename("schema")
        self.assertEquals(output, expected_output)

    def test_generate_filename_report(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101.rpt'
        output = self.context.generate_filename("report")
        self.assertEquals(output, expected_output)

    def test_generate_filename_increments(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_increments'
        output = self.context.generate_filename("increments")
        self.assertEquals(output, expected_output)

    def test_generate_filename_last_operation(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_last_operation'
        output = self.context.generate_filename("last_operation")
        self.assertEquals(output, expected_output)

    def test_generate_filename_dirty_table(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_dirty_list'
        output = self.context.generate_filename("dirty_table")
        self.assertEquals(output, expected_output)

    def test_generate_filename_plan(self):
        expected_output = '/data/db_dumps/20160101/gp_restore_20160101010101_plan'
        output = self.context.generate_filename("plan")
        self.assertEquals(output, expected_output)

    def test_generate_filename_metadata(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_1_1_20160101010101.gz'
        output = self.context.generate_filename("metadata")
        self.assertEquals(output, expected_output)

    def test_generate_filename_postdata(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_1_1_20160101010101_post_data.gz'
        output = self.context.generate_filename("postdata")
        self.assertEquals(output, expected_output)

    def test_generate_filename_partition_list(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_table_list'
        output = self.context.generate_filename("partition_list")
        self.assertEquals(output, expected_output)

    def test_generate_filename_ao(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_ao_state_file'
        output = self.context.generate_filename("ao")
        self.assertEquals(output, expected_output)

    def test_generate_filename_co_(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_co_state_file'
        output = self.context.generate_filename("co")
        self.assertEquals(output, expected_output)

    def test_generate_filename_files(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_regular_files'
        output = self.context.generate_filename("files")
        self.assertEquals(output, expected_output)

    def test_generate_filename_pipes(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_pipes'
        output = self.context.generate_filename("pipes")
        self.assertEquals(output, expected_output)

    def test_generate_filename_master_config(self):
        expected_output = '/data/db_dumps/20160101/gp_master_config_files_20160101010101.tar'
        output = self.context.generate_filename("master_config")
        self.assertEquals(output, expected_output)

    def test_generate_filename_segment_config(self):
        dbid = 2
        expected_output = '/data/db_dumps/20160101/gp_segment_config_files_0_2_20160101010101.tar'
        output = self.context.generate_filename("segment_config", dbid=dbid)
        self.assertEquals(output, expected_output)

    def test_generate_filename_filter(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_filter'
        output = self.context.generate_filename("filter")
        self.assertEquals(output, expected_output)

    def test_generate_filename_cgenerate(self):
        expected_output = '/data/db_dumps/20160101/gp_cdatabase_1_1_20160101010101'
        output = self.context.generate_filename("cdatabase")
        self.assertEquals(output, expected_output)

    def test_generate_filename_status_master(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_status_1_1_20160101010101'
        output = self.context.generate_filename("status")
        self.assertEquals(output, expected_output)

    def test_generate_filename_status_segment(self):
        dbid = 2
        expected_output = '/data/db_dumps/20160101/gp_dump_status_0_2_20160101010101'
        output = self.context.generate_filename("status", dbid=dbid)
        self.assertEquals(output, expected_output)

    def test_generate_filename_global(self):
        expected_output = '/data/db_dumps/20160101/gp_global_1_1_20160101010101'
        output = self.context.generate_filename("global")
        self.assertEquals(output, expected_output)

    def test_generate_filename_stats(self):
        expected_output = '/data/db_dumps/20160101/gp_statistics_1_1_20160101010101'
        output = self.context.generate_filename("stats")
        self.assertEquals(output, expected_output)

    def test_generate_filename_dump_master(self):
        expected_output = '/data/db_dumps/20160101/gp_dump_1_1_20160101010101.gz'
        output = self.context.generate_filename("dump")
        self.assertEquals(output, expected_output)

    def test_generate_filename_dump_segment(self):
        dbid = 2
        expected_output = '/data/db_dumps/20160101/gp_dump_0_2_20160101010101.gz'
        output = self.context.generate_filename("dump", dbid=dbid)
        self.assertEquals(output, expected_output)

    def test_generate_filename_different_backup_dir(self):
        self.context.backup_dir = '/datadomain'
        expected_output = '/datadomain/db_dumps/20160101/gp_dump_20160101010101_schema'
        output = self.context.generate_filename("schema")
        self.assertEquals(output, expected_output)

    def test_generate_filename_no_mdd(self):
        self.context.master_datadir = None
        self.context.backup_dir = '/datadomain'
        expected_output = '/datadomain/db_dumps/20160101/gp_dump_20160101010101_schema'
        output = self.context.generate_filename("schema")
        self.assertEquals(output, expected_output)

    def test_generate_filename_no_mdd_or_backup_dir(self):
        self.context.master_datadir = None
        with self.assertRaisesRegexp(Exception, 'Cannot locate backup directory with existing parameters'):
            self.context.generate_filename("schema")

    def test_generate_filename_no_timestamp(self):
        self.context.timestamp = None
        with self.assertRaisesRegexp(Exception, 'Cannot locate backup directory without timestamp'):
            self.context.generate_filename("schema")

    def test_generate_filename_bad_timestamp(self):
        self.context.timestamp = 'xx160101010101'
        with self.assertRaisesRegexp(Exception, 'Invalid timestamp'):
            self.context.generate_filename("schema")

    def test_generate_filename_short_timestamp(self):
        self.context.timestamp = '2016'
        with self.assertRaisesRegexp(Exception, 'Invalid timestamp'):
            self.context.generate_filename("schema")

    def test_validate_timestamp_default(self):
        ts = "20160101010101"
        result = validate_timestamp(ts)
        self.assertTrue(result)

    def test_validate_timestamp_too_short(self):
        ts = "2016010101010"
        result = validate_timestamp(ts)
        self.assertFalse(result)

    def test_validate_timestamp_too_long(self):
        ts = "201601010101010"
        result = validate_timestamp(ts)
        self.assertFalse(result)

    def test_validate_timestamp_zero(self):
        ts = "00000000000000"
        result = validate_timestamp(ts)
        self.assertTrue(result)

    def test_validate_timestamp_hex(self):
        ts = "0a000000000000"
        result = validate_timestamp(ts)
        self.assertFalse(result)

    def test_validate_timestamp_leading_space(self):
        ts = " 00000000000000"
        result = validate_timestamp(ts)
        self.assertFalse(result)

    def test_validate_timestamp_trailing_space(self):
        ts = "00000000000000 "
        result = validate_timestamp(ts)
        self.assertFalse(result);

    def test_validate_timestamp_none(self):
        ts = None
        result = validate_timestamp(ts)
        self.assertFalse(result)

    def test_generate_filename_with_timestamp(self):
        ts = '20150101010101'
        expected_output = '/data/db_dumps/20150101/gp_dump_20150101010101_increments'
        output = self.context.generate_filename("increments", timestamp=ts)
        self.assertEquals(output, expected_output)

    def test_generate_filename_with_ddboost(self):
        self.context.ddboost = True
        self.context.backup_dir = "/tmp"
        expected_output = '/data/db_dumps/20160101/gp_dump_20160101010101_increments'
        output = self.context.generate_filename("increments")
        self.assertEquals(output, expected_output)

    def test_convert_report_filename_to_cdatabase_filename(self):
        report_file = '/data/db_dumps/20160101/gp_dump_20160101010101.rpt'
        expected_output = '/data/db_dumps/20160101/gp_cdatabase_1_1_20160101010101'
        cdatabase_file = convert_report_filename_to_cdatabase_filename(self.context, report_file)
        self.assertEquals(expected_output, cdatabase_file)

    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['--', '-- Database creation', '--', '', "CREATE DATABASE bkdb WITH TEMPLATE = template0 ENCODING = 'UTF8' OWNER = dcddev;"])
    def test_check_cdatabase_exists_default(self, mock):
        self.context.dump_database = 'bkdb'
        report_file = '/data/db_dumps/20160101/gp_dump_20160101010101.rpt'
        result = check_cdatabase_exists(self.context, report_file)
        self.assertTrue(result)

    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['--', '-- Database creation', '--', '', "CREATE DATABASE fullbkdb WITH TEMPLATE = template0 ENCODING = 'UTF8' OWNER = dcddev;"])
    def test_check_cdatabase_exists_bad_dbname(self, mock):
        self.context.dump_database = 'bkdb'
        report_file = '/data/db_dumps/20160101/gp_dump_20160101010101.rpt'
        result = check_cdatabase_exists(self.context, report_file)
        self.assertFalse(result)

    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['--', '-- Database creation', '--', '', "CREATE bkdb WITH TEMPLATE = template0 ENCODING = 'UTF8' OWNER = dcddev;"])
    def test_check_cdatabase_exists_no_database(self, mock):
        self.context.dump_database = 'bkdb'
        report_file = '/data/db_dumps/20160101/gp_dump_20160101010101.rpt'
        result = check_cdatabase_exists(self.context, report_file)
        self.assertFalse(result)

    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=[])
    def test_check_cdatabase_exists_empty_file(self, mock):
        self.context.dump_database = 'bkdb'
        report_file = '/data/db_dumps/20160101/gp_dump_20160101010101.rpt'
        result = check_cdatabase_exists(self.context, report_file)
        self.assertFalse(result)

    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['--', '-- Database creation', '--', '', 'CREATE DATABASE'])
    def test_check_cdatabase_exists_no_dbname(self, mock):
        self.context.dump_database = 'bkdb'
        report_file = '/data/db_dumps/20160101/gp_dump_20160101010101.rpt'
        result = check_cdatabase_exists(self.context, report_file)
        self.assertFalse(result)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.dump.Command.get_results', return_value=CommandResult(0, "CREATE DATABASE", "", True, False))
    def test_check_cdatabase_exists_command_result(self, mock1, mock2):
        self.context.dump_database = 'bkdb'
        report_file = '/data/db_dumps/20160101/gp_dump_20160101010101.rpt'
        self.context.ddboost = True
        result = check_cdatabase_exists(self.context, report_file)
        self.assertFalse(result)

    def test_get_backup_dir_default(self):
        expected = '/data/db_dumps/20160101'
        result = self.context.get_backup_dir()
        self.assertTrue(result, expected)

    def test_get_backup_dir_different_backup_dir(self):
        self.context.backup_dir = '/tmp/foo'
        expected = '/tmp/foo/db_dumps/20160101'
        result = self.context.get_backup_dir()
        self.assertTrue(result, expected)

    def test_get_backup_dir_no_mdd(self):
        self.context.master_datadir= None
        with self.assertRaisesRegexp(Exception, 'Cannot locate backup directory with existing parameters'):
            result = self.context.get_backup_dir()

    def test_get_backup_dir_bad_timestamp(self):
        timestamp = 'a0160101010101'
        with self.assertRaisesRegexp(Exception, 'Invalid timestamp'):
            result = self.context.get_backup_dir(timestamp)

    def test_check_successful_dump_default(self):
        successful_dump = check_successful_dump(['gp_dump utility finished successfully.'])
        self.assertTrue(successful_dump)

    def test_check_successful_dump_failure(self):
        successful_dump = check_successful_dump(['gp_dump utility finished unsuccessfully.'])
        self.assertFalse(successful_dump)

    def test_check_successful_dump_no_result(self):
        successful_dump = check_successful_dump([])
        self.assertFalse(successful_dump)

    def test_check_successful_dump_with_whitespace(self):
        successful_dump = check_successful_dump(['gp_dump utility finished successfully.\n'])
        self.assertTrue(successful_dump)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Full', 'Timestamp Key: 01234567891234', 'gp_dump utility finished successfully.'])
    def test_get_full_ts_from_report_file_default(self, mock1, mock2):
        expected_output = '01234567891234'
        ts = get_full_ts_from_report_file(self.context, 'foo')
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=False)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Full', 'Timestamp Key: 01234567891234', 'gp_dump utility finished successfully.'])
    def test_get_full_ts_from_report_file_no_database(self, mock1, mock2):
        expected_output = None
        ts = get_full_ts_from_report_file(self.context, 'foo')
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Full', 'Timestamp Key: 01234567891234567', 'gp_dump utility finished successfully.'])
    def test_get_full_ts_from_report_file_timestamp_too_long(self, mock1, mock2):
        with self.assertRaisesRegexp(Exception, 'Invalid timestamp value found in report_file'):
            get_full_ts_from_report_file(self.context, 'foo')

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Full', 'Timestamp Key: xxx34567891234', 'gp_dump utility finished successfully.'])
    def test_get_full_ts_from_report_file_bad_timestamp(self, mock1, mock2):
        with self.assertRaisesRegexp(Exception, 'Invalid timestamp value found in report_file'):
            get_full_ts_from_report_file(self.context, 'foo')

    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Full'])
    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    def test_get_full_ts_from_report_file_missing_output(self, mock1, mock2):
        expected_output = None
        ts = get_full_ts_from_report_file(self.context, 'foo')
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Full', 'gp_dump utility finished successfully.'])
    def test_get_full_ts_from_report_file_missing_timestamp(self, mock1, mock2):
        expected_output = None
        ts = get_full_ts_from_report_file(self.context, 'foo')
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Full', 'Timestamp Key: xxx34567891234', 'gp_dump utility finished successfully.'])
    def test_get_full_ts_from_report_file_with_ddboost_bad_ts(self, mock1, mock2):
        self.context.ddboost = True
        with self.assertRaisesRegexp(Exception, 'Invalid timestamp value found in report_file'):
            ts = get_full_ts_from_report_file(self.context, 'foo')

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Full', 'Timestamp Key: 01234567891234', 'gp_dump utility finished successfully.'])
    def test_get_full_ts_from_report_file_with_ddboost_good_ts(self, mock1, mock2):
        expected_output = '01234567891234'
        self.context.ddboost = True
        ts = get_full_ts_from_report_file(self.context, 'foo')
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Incremental', 'Timestamp Key: 01234567891234', 'gp_dump utility finished successfully.'])
    def test_get_incremental_ts_from_report_file_default(self, mock1, mock2):
        expected_output = '01234567891234'
        ts = get_incremental_ts_from_report_file(self.context, 'foo')
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Incremental'])
    def test_get_incremental_ts_from_report_file_missing_output(self, mock1, mock2):
        expected_output = None
        ts = get_incremental_ts_from_report_file(self.context, 'foo')
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Incremental', 'gp_dump utility finished successfully.'])
    def test_get_incremental_ts_from_report_file_success(self, mock1, mock2):
        expected_output = None
        ts = get_incremental_ts_from_report_file(self.context, 'foo')
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Full', 'Timestamp Key: 01234567891234', 'gp_dump utility finished successfully.'])
    def test_get_incremental_ts_from_report_file_full(self, mock1, mock2):
        expected_output = None
        ts = get_incremental_ts_from_report_file(self.context, 'foo')
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=False)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Incremental', 'Timestamp Key: 01234567891234', 'gp_dump utility finished successfully.'])
    def test_get_incremental_ts_from_report_file_no_database(self, mock1, mock2):
        expected_output = None
        ts = get_incremental_ts_from_report_file(self.context, 'foo')
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Incremental', 'Timestamp Key: 01234567891234567', 'gp_dump utility finished successfully.'])
    def test_get_incremental_ts_from_report_file_timestamp_too_long(self, mock1, mock2):
        with self.assertRaisesRegexp(Exception, 'Invalid timestamp value found in report_file'):
            get_incremental_ts_from_report_file(self.context, 'foo')

    @patch('gppylib.operations.backup_utils.check_cdatabase_exists', return_value=True)
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['Backup Type: Incremental', 'Timestamp Key: xxx34567891234', 'gp_dump utility finished successfully.'])
    def test_get_incremental_ts_from_report_file_bad_timestamp(self, mock1, mock2):
        with self.assertRaisesRegexp(Exception, 'Invalid timestamp value found in report_file'):
            get_incremental_ts_from_report_file(self.context, 'foo')

    def test_check_backup_type_full(self):
        backup_type = check_backup_type(['Backup Type: Full'], 'Full')
        self.assertEqual(backup_type, True)

    def test_check_backup_type_mismatch(self):
        backup_type = check_backup_type(['Backup Type: Incremental'], 'Full')
        self.assertEqual(backup_type, False)

    def test_check_backup_type_invalid_type(self):
        backup_type = check_backup_type(['foo'], 'Full')
        self.assertEqual(backup_type, False)

    def test_check_backup_type_type_too_long(self):
        backup_type = check_backup_type(['Backup Type: FullQ'], 'Full')
        self.assertEqual(backup_type, False)

    def test_get_timestamp_val_default(self):
        ts_key = get_timestamp_val(['Timestamp Key: 01234567891234'])
        self.assertEqual(ts_key, '01234567891234')

    def test_get_timestamp_val_timestamp_too_short(self):
        ts_key = get_timestamp_val(['Time: 00000'])
        self.assertEqual(ts_key, None)

    def test_get_timestamp_val_bad_timestamp(self):
        with self.assertRaisesRegexp(Exception, 'Invalid timestamp value found in report_file'):
            get_timestamp_val(['Timestamp Key: '])

    @patch('os.path.isdir', return_value=True)
    @patch('os.listdir', return_value=['20161212'])
    def test_get_dump_dirs_single(self, mock, mock1):
        self.context.backup_dir = '/tmp'
        expected_output = ['/tmp/db_dumps/20161212']
        ddir = get_dump_dirs(self.context)
        self.assertEqual(ddir, expected_output)

    @patch('os.path.isdir', return_value=True)
    @patch('os.listdir', return_value=['20161212', '20161213', '20161214'])
    def test_get_dump_dirs_multiple(self, mock, mock1):
        self.context.backup_dir = '/tmp'
        expected_output = ['20161212', '20161213', '20161214']
        ddir = get_dump_dirs(self.context)
        self.assertEqual(ddir.sort(), expected_output.sort())

    @patch('os.path.isdir', return_value=True)
    @patch('os.listdir', return_value=[])
    def test_get_dump_dirs_empty(self, mock, mock2):
        self.context.backup_dir = '/tmp'
        self.assertEquals([], get_dump_dirs(self.context))

    @patch('os.path.isdir', return_value=True)
    @patch('os.listdir', return_value=['2016120a', '201612121', 'abcde'])
    def test_get_dump_dirs_bad_dirs(self, mock, mock2):
        self.context.backup_dir = '/tmp'
        self.assertEquals([], get_dump_dirs(self.context))

    @patch('os.listdir', return_value=['11111111', '20161201']) # Second file shouldn't be picked up, pretend it's a file
    @patch('os.path.isdir', side_effect=[True, True, False]) # First value verifies dump dir exists, second and third are for the respective date dirs above
    def test_get_dump_dirs_file_not_dir(self, mock, mock2):
        self.context.backup_dir = '/tmp'
        expected_output = ['/tmp/db_dumps/11111111']
        ddir = get_dump_dirs(self.context)
        self.assertEqual(ddir, expected_output)

    @patch('gppylib.operations.backup_utils.get_dump_dirs', return_value=['20161212', '20161213', '20161214'])
    @patch('os.listdir', return_value=['gp_cdatabase_1_1_20161212111111', 'gp_dump_20161212000000.rpt', 'gp_cdatabase_1_1_20161212000001'])
    @patch('gppylib.operations.backup_utils.get_full_ts_from_report_file', return_value=['000000'])
    def test_get_latest_full_dump_timestamp_default(self, mock1, mock2, mock3):
        expected_output = ['000000']
        ts = get_latest_full_dump_timestamp(self.context)
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.get_dump_dirs', return_value=[])
    def test_get_latest_full_dump_timestamp_no_full(self, mock1):
        with self.assertRaisesRegexp(Exception, 'No full backup found for incremental'):
            get_latest_full_dump_timestamp(self.context)

    @patch('gppylib.operations.backup_utils.get_dump_dirs', return_value=['20161212', '20161213', '20161214'])
    @patch('os.listdir', return_value=['gp_cdatabase_1_1_2016121211111', 'gp_cdatabase_1_1_201612120000010', 'gp_cdatabase_1_1_2016121a111111'])
    def test_get_latest_full_dump_timestamp_bad_timestamp(self, mock1, mock2):
        with self.assertRaisesRegexp(Exception, 'No full backup found for incremental'):
            ts = get_latest_full_dump_timestamp(self.context)

    @patch('gppylib.operations.backup_utils.get_dump_dirs', return_value=['20161212', '20161213', '20161214'])
    @patch('os.listdir', return_value=['gp_cdatabase_1_1_20161212111111', 'gp_dump_20161212000000.rpt.bk', 'gp_cdatabase_1_1_20161212000001'])
    def test_get_latest_full_dump_timestamp_no_report_file(self, mock1, mock2):
        with self.assertRaisesRegexp(Exception, 'No full backup found for incremental'):
            ts = get_latest_full_dump_timestamp(self.context)

    def test_generate_filename_with_ddboost(self):
        expected_output = '/data/backup/DCA-35/20160101/gp_dump_20160101010101_last_operation'
        self.context.ddboost = True
        self.context.dump_dir = 'backup/DCA-35'
        output = self.context.generate_filename("last_operation")
        self.assertEquals(output, expected_output)

    def test_generate_filename_with_env_mdd(self):
        timestamp = '20160101010101'
        expected_output = '%s/db_dumps/20160101/gp_dump_20160101010101_ao_state_file' % self.context.master_datadir
        output = self.context.generate_filename("ao")
        self.assertEqual(output, expected_output)

    @patch('gppylib.operations.backup_utils.get_dump_dirs', return_value=['20160930'])
    @patch('gppylib.operations.backup_utils.get_latest_report_in_dir', return_value='20160930093000')
    def test_get_latest_report_timestamp_default(self, mock1, mock2):
        self.context.backup_dir = '/foo'
        result = get_latest_report_timestamp(self.context)
        self.assertEquals(result, '20160930093000')

    @patch('gppylib.operations.backup_utils.get_dump_dirs', return_value=[])
    @patch('gppylib.operations.backup_utils.get_latest_report_in_dir', return_value=[])
    def test_get_latest_report_timestamp_no_dirs(self, mock1, mock2):
        self.context.backup_dir = '/foo'
        result = get_latest_report_timestamp(self.context)
        self.assertEquals(result, None)

    @patch('gppylib.operations.backup_utils.get_dump_dirs', return_value=['20160930'])
    @patch('gppylib.operations.backup_utils.get_latest_report_in_dir', return_value=None)
    def test_get_latest_report_timestamp_no_report_file(self, mock1, mock2):
        self.context.backup_dir = '/foo'
        result = get_latest_report_timestamp(self.context)
        self.assertEquals(result, None)

    @patch('gppylib.operations.backup_utils.get_dump_dirs', return_value=['20160930', '20160929'])
    @patch('gppylib.operations.backup_utils.get_latest_report_in_dir', side_effect=[None, '20160929093000'])
    def test_get_latest_report_timestamp_multiple_dirs(self, mock1, mock2):
        self.context.backup_dir = '/foo'
        result = get_latest_report_timestamp(self.context)
        self.assertEquals(result, '20160929093000')

    @patch('os.listdir', return_value=[])
    def test_get_latest_report_in_dir_no_dirs(self, mock1):
        bdir = '/foo'
        result = get_latest_report_in_dir(bdir, self.context.dump_prefix)
        self.assertEquals(result, None)

    @patch('os.listdir', return_value=['gp_dump_20130125140013.rpt', 'gp_dump_20160125140013.FOO'])
    def test_get_latest_report_in_dir_bad_extension(self, mock1):
        bdir = '/foo'
        result = get_latest_report_in_dir(bdir, self.context.dump_prefix)
        self.assertEquals(result, '20130125140013')

    @patch('os.listdir', return_value=['gp_dump_20130125140013.rpt', 'gp_dump_20160125140013.rpt'])
    def test_get_latest_report_in_dir_different_years(self, mock1):
        bdir = '/foo'
        result = get_latest_report_in_dir(bdir, self.context.dump_prefix)
        self.assertEquals(result, '20160125140013')

    @patch('os.listdir', return_value=['gp_dump_20160125140013.rpt', 'gp_dump_20130125140013.rpt'])
    def test_get_latest_report_in_dir_different_years_different_order(self, mock1):
        bdir = '/foo'
        result = get_latest_report_in_dir(bdir, self.context.dump_prefix)
        self.assertEquals(result, '20160125140013')

    def test_create_temp_file_with_tables_default(self):
        dirty_tables = [('public', 't1'), ('public', 't2'), ('testschema', 't3')]
        dirty_file = create_temp_file_with_tables(dirty_tables)
        self.assertTrue(os.path.basename(dirty_file).startswith('table_list'))
        self.assertTrue(os.path.exists(dirty_file))
        content = get_lines_from_csv_file(dirty_file)
        self.assertEqual(dirty_tables, content)
        os.remove(dirty_file)

    def test_create_temp_file_with_tables_no_tables(self):
        dirty_tables = ['']
        dirty_file = create_temp_file_with_tables(dirty_tables)
        self.assertTrue(os.path.basename(dirty_file).startswith('table_list'))
        self.assertTrue(os.path.exists(dirty_file))
        content = get_lines_from_file(dirty_file)
        self.assertEqual(dirty_tables, content)
        os.remove(dirty_file)

    def test_create_temp_file_from_list_nonstandard_name(self):
        dirty_tables = ['public', 'tempschema', 'testschema']
        dirty_file = create_temp_file_from_list(dirty_tables, 'dirty_hackup_list_')
        self.assertTrue(os.path.basename(dirty_file).startswith('dirty_hackup_list'))
        self.assertTrue(os.path.exists(dirty_file))
        content = get_lines_from_file(dirty_file)
        self.assertEqual(dirty_tables, content)
        os.remove(dirty_file)

    def test_create_temp_file_from_list_no_tables_different_name(self):
        dirty_tables = ['']
        dirty_file = create_temp_file_from_list(dirty_tables, 'dirty_hackup_list_')
        self.assertTrue(os.path.basename(dirty_file).startswith('dirty_hackup_list'))
        self.assertTrue(os.path.exists(dirty_file))
        content = get_lines_from_file(dirty_file)
        self.assertEqual(dirty_tables, content)
        os.remove(dirty_file)

    def test_get_timestamp_from_increments_filename_default(self):
        fname = '/data/foo/db_dumps/20130207/gp_dump_20130207133000_increments'
        ts = get_timestamp_from_increments_filename(fname, self.context.dump_prefix)
        self.assertEquals(ts, '20130207133000')

    def test_get_timestamp_from_increments_filename_bad_file(self):
        fname = '/data/foo/db_dumps/20130207/gpdump_20130207133000_increments'
        with self.assertRaisesRegexp(Exception, 'Invalid increments file'):
            get_timestamp_from_increments_filename(fname, self.context.dump_prefix)

    @patch('glob.glob', return_value=[])
    def test_get_full_timestamp_for_incremental_no_backup(self, mock1):
        self.context.backup_dir = 'home'
        with self.assertRaisesRegexp(Exception, "Could not locate full backup associated with timestamp '20160101010101'. Either increments file or full backup is missing."):
            get_full_timestamp_for_incremental(self.context)

    @patch('glob.glob', return_value=['foo'])
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=[])
    def test_get_full_timestamp_for_incremental_bad_files(self, mock1, mock2):
        self.context.backup_dir = 'home'
        with self.assertRaisesRegexp(Exception, "Could not locate full backup associated with timestamp '20160101010101'. Either increments file or full backup is missing."):
            get_full_timestamp_for_incremental(self.context)

    @patch('glob.glob', return_value=['/tmp/db_dumps/20130207/gp_dump_20130207093000_increments'])
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['20130207133001', '20130207133000'])
    @patch('os.path.exists', return_value = True)
    def test_get_full_timestamp_for_incremental_default(self, mock1, mock2, mock3):
        self.context.timestamp = '20130207133000'
        full_ts = get_full_timestamp_for_incremental(self.context)
        self.assertEquals(full_ts, '20130207093000')

    def test_check_funny_chars_in_names_exclamation_mark(self):
        tablenames = ['hello! world', 'correct']
        with self.assertRaisesRegexp(Exception, 'Name has an invalid character'):
            check_funny_chars_in_names(tablenames)

    def test_check_funny_chars_in_names_newline(self):
        tablenames = ['hello\nworld', 'propertablename']
        with self.assertRaisesRegexp(Exception, 'Name has an invalid character'):
            check_funny_chars_in_names(tablenames)

    def test_check_funny_chars_in_names_default(self):
        tablenames = ['helloworld', 'propertablename']
        check_funny_chars_in_names(tablenames) #should not raise an exception

    def test_check_funny_chars_in_names_comma(self):
        tablenames = ['hello, world', 'correct']
        with self.assertRaisesRegexp(Exception, 'Name has an invalid character'):
            check_funny_chars_in_names(tablenames)

    def test_expand_partition_tables_do_nothing(self):
        self.assertEqual(expand_partition_tables('foo', None), None)

    @patch('gppylib.operations.backup_utils.dbconn.execSQL')
    @patch('gppylib.operations.backup_utils.dbconn.connect')
    @patch('pygresql.pgdb.pgdbCursor.fetchall', return_value=[['public', 'tl1'], ['public', 'tl2']])
    def test_expand_partition_tables_default(self, mock1, mock2, mock3):
        dbname = 'foo'
        restore_tables = [('public', 't1'), ('public', 't2')]
        expected_output = [('public', 'tl1'), ('public', 'tl2'), ('public', 't2')]
        result = expand_partition_tables(dbname, restore_tables)
        self.assertEqual(result.sort(), expected_output.sort())

    @patch('gppylib.operations.backup_utils.dbconn.execSQL')
    @patch('gppylib.operations.backup_utils.dbconn.connect')
    @patch('pygresql.pgdb.pgdbCursor.fetchall', return_value=[])
    def test_expand_partition_tables_no_change(self, mock1, mock2, mock3):
        dbname = 'foo'
        restore_tables = [('public', 't1'), ('public', 't2')]
        expected_output = [('public', 't1'), ('public', 't2')]
        result = expand_partition_tables(dbname, restore_tables)
        self.assertEqual(result.sort(), expected_output.sort())

    @patch('gppylib.operations.backup_utils.expand_partition_tables', return_value=[('public', 't1_p1'), ('public', 't1_p2'), ('public', 't1_p3'), ('public', 't2'), ('public', 't3')])
    def test_expand_partitions_and_populate_filter_file_part_tables(self, mock):
        dbname = 'bkdb'
        partition_list = [('public', 't1'), ('public', 't2'), ('public', 't3')]
        file_prefix = 'include_dump_tables_file'
        expected_output = [('public', 't2'), ('public', 't3'), ('public', 't1'), ('public', 't1_p1'), ('public', 't1_p2'), ('public', 't1_p3')]
        result = expand_partitions_and_populate_filter_file(dbname, partition_list, file_prefix)
        self.assertTrue(os.path.basename(result).startswith(file_prefix))
        self.assertTrue(os.path.exists(result))
        contents = get_lines_from_file(result)
        self.assertEqual(contents.sort(), expected_output.sort())
        os.remove(result)

    @patch('gppylib.operations.backup_utils.expand_partition_tables', return_value=[('public', 't1'), ('public', 't2'), ('public', 't3')])
    def test_expand_partitions_and_populate_filter_file_no_part_tables(self, mock):
        dbname = 'bkdb'
        partition_list = [('public', 't1'), ('public', 't2'), ('public', 't3')]
        file_prefix = 'exclude_dump_tables_file'
        result = expand_partitions_and_populate_filter_file(dbname, partition_list, file_prefix)
        self.assertTrue(os.path.basename(result).startswith(file_prefix))
        self.assertTrue(os.path.exists(result))
        contents = get_lines_from_file(result)
        self.assertEqual(contents.sort(), partition_list.sort())
        os.remove(result)

    @patch('gppylib.operations.backup_utils.expand_partition_tables', return_value=[])
    def test_expand_partitions_and_populate_filter_file_no_tables(self, mock):
        dbname = 'bkdb'
        partition_list = ['part_table']
        file_prefix = 'exclude_dump_tables_file'
        result = expand_partitions_and_populate_filter_file(dbname, partition_list, file_prefix)
        self.assertTrue(os.path.basename(result).startswith(file_prefix))
        self.assertTrue(os.path.exists(result))
        contents = get_lines_from_file(result)
        self.assertEqual(contents.sort(), partition_list.sort())
        os.remove(result)

    def test_get_batch_from_list_default(self):
        batch = 1000
        length = 3033
        expected = [(0,1000), (1000,2000), (2000,3000), (3000,4000)]
        indices = get_batch_from_list(length, batch)
        self.assertEqual(expected, indices)

    def test_get_batch_from_list_one_job(self):
        batch = 1000
        length = 1
        expected = [(0,1000)]
        indices = get_batch_from_list(length, batch)
        self.assertEqual(expected, indices)

    def test_get_batch_from_list_matching_jobs(self):
        batch = 1000
        length = 1000
        expected = [(0,1000)]
        indices = get_batch_from_list(length, batch)
        self.assertEqual(expected, indices)

    def test_get_batch_from_list_no_jobs(self):
        batch = 1000
        length = 0
        expected = []
        indices = get_batch_from_list(length, batch)
        self.assertEqual(expected, indices)

    def test_get_batch_from_list_more_jobs(self):
        batch = 1000
        length = 2000
        expected = [(0,1000), (1000,2000)]
        indices = get_batch_from_list(length, batch)
        self.assertEqual(expected, indices)

    def test_list_to_quoted_string_default(self):
        input = [('public', 'ao_table'), ('public', 'co_table')]
        expected = "('public','ao_table'), ('public','co_table')"
        output = list_to_quoted_string(input)
        self.assertEqual(expected, output)

    def test_list_to_quoted_string_whitespace(self):
        input = [('   public', 'ao_table'), ('public', 'co_table   ')]
        expected = "('   public','ao_table'), ('public','co_table   ')"
        output = list_to_quoted_string(input)
        self.assertEqual(expected, output)

    def test_list_to_quoted_string_one_table(self):
        input = [('public', 'ao_table')]
        expected = "('public','ao_table')"
        output = list_to_quoted_string(input)
        self.assertEqual(expected, output)

    def test_list_to_quoted_string_no_tables(self):
        input = []
        expected = '()'
        output = list_to_quoted_string(input)
        self.assertEqual(expected, output)

    def test_generate_filename_with_prefix(self):
        self.context.dump_prefix = 'foo_'
        expected_output = '/data/db_dumps/20160101/%sgp_dump_20160101010101.rpt' % self.context.dump_prefix
        output = self.context.generate_filename("report")
        self.assertEquals(output, expected_output)

    def test_generate_filename_with_prefix_and_ddboost(self):
        self.context.dump_prefix = 'foo_'
        expected_output = '/data/backup/DCA-35/20160101/%sgp_dump_20160101010101.rpt' % self.context.dump_prefix
        self.context.ddboost = True
        self.context.dump_dir = 'backup/DCA-35'
        output = self.context.generate_filename("report")
        self.assertEquals(output, expected_output)

    @patch('os.listdir', return_value=['bar_gp_dump_20160125140013.rpt', 'foo_gp_dump_20130125140013.rpt'])
    def test_get_latest_report_in_dir_with_mixed_prefixes(self, mock1):
        bdir = '/foo'
        self.context.dump_prefix = 'foo_'
        result = get_latest_report_in_dir(bdir, self.context.dump_prefix)
        self.assertEquals(result, '20130125140013')

    @patch('os.listdir', return_value=['gp_dump_20130125140013.rpt'])
    def test_get_latest_report_in_dir_with_no_prefix(self, mock1):
        bdir = '/foo'
        self.context.dump_prefix = 'foo_'
        result = get_latest_report_in_dir(bdir, self.context.dump_prefix)
        self.assertEquals(result, None)

    @patch('glob.glob', return_value=['/tmp/db_dumps/20130207/foo_gp_dump_20130207093000_increments'])
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['20130207133001', '20130207133000'])
    @patch('os.path.exists', return_value = True)
    def test_get_full_timestamp_for_incremental_with_prefix_default(self, mock1, mock2, mock3):
        self.context.backup_dir = 'home'
        self.context.dump_prefix = 'foo_'
        self.context.timestamp = '20130207133000'
        full_ts = get_full_timestamp_for_incremental(self.context)
        self.assertEquals(full_ts, '20130207093000')

    @patch('glob.glob', return_value=[])
    def test_get_full_timestamp_for_incremental_with_prefix_no_files(self, mock1):
        self.context.backup_dir = 'home'
        self.context.dump_prefix = 'foo_'
        with self.assertRaisesRegexp(Exception, "Could not locate full backup associated with timestamp '20160101010101'. Either increments file or full backup is missing."):
            get_full_timestamp_for_incremental(self.context)

    @patch('glob.glob', return_value=['foo'])
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=[])
    def test_get_full_timestamp_for_incremental_with_prefix_bad_files(self, mock1, mock2):
        self.context.backup_dir = 'home'
        self.context.dump_prefix = 'foo_'
        with self.assertRaisesRegexp(Exception, "Could not locate full backup associated with timestamp '20160101010101'. Either increments file or full backup is missing."):
            get_full_timestamp_for_incremental(self.context)

    @patch('gppylib.operations.backup_utils.get_dump_dirs', return_value=['20161212', '20161213', '20161214'])
    @patch('os.listdir', return_value=['foo_gp_cdatabase_1_1_20161212111111', 'foo_gp_dump_20161212000000.rpt', 'foo_gp_cdatabase_1_1_20161212000001'])
    @patch('gppylib.operations.backup_utils.get_full_ts_from_report_file', return_value=['000000'])
    def test_get_latest_full_dump_timestamp_with_prefix_multiple_files(self, mock1, mock2, mock3):
        expected_output = ['000000']
        self.context.dump_prefix = 'foo_'
        ts = get_latest_full_dump_timestamp(self.context)
        self.assertEqual(ts, expected_output)

    @patch('gppylib.operations.backup_utils.get_dump_dirs', return_value=[])
    def test_get_latest_full_dump_timestamp_with_prefix_no_backup(self, mock1):
        self.context.dump_prefix = 'foo_'
        with self.assertRaisesRegexp(Exception, 'No full backup found for incremental'):
            get_latest_full_dump_timestamp(self.context)

    def test_convert_report_filename_to_cdatabase_filename_with_prefix_default(self):
        report_file = '/data/db_dumps/20160101/bar_gp_dump_20160101010101.rpt'
        expected_output = '/data/db_dumps/20160101/bar_gp_cdatabase_1_1_20160101010101'
        self.context.dump_prefix = 'bar_'
        cdatabase_file = convert_report_filename_to_cdatabase_filename(self.context, report_file)
        self.assertEquals(expected_output, cdatabase_file)

    @patch('gppylib.operations.backup_utils.Command.run')
    def test_backup_file_with_nbu_default(self, mock1):
        backup_file_with_nbu(self.context, path=self.netbackup_filepath)

    @patch('gppylib.operations.backup_utils.Command.run')
    def test_backup_file_with_nbu_with_segment(self, mock1):
        segment_hostname = "sdw"

        backup_file_with_nbu(self.context, path=self.netbackup_filepath, hostname=segment_hostname)

    @patch('gppylib.operations.backup_utils.Command.run', side_effect=Exception('Error backing up file to NetBackup'))
    def test_backup_file_with_nbu_with_Error(self, mock1):
        with self.assertRaisesRegexp(Exception, 'Error backing up file to NetBackup'):
            backup_file_with_nbu(self.context, path=self.netbackup_filepath)

    @patch('gppylib.operations.backup_utils.Command.run')
    def test_backup_file_with_nbu_no_block_size(self, mock1):
        self.context.netbackup_block_size = None

        backup_file_with_nbu(self.context, path=self.netbackup_filepath)

    @patch('gppylib.operations.backup_utils.Command.run', side_effect=Exception('Error backing up file to NetBackup'))
    def test_backup_file_with_nbu_no_block_size_with_error(self, mock1):
        self.context.netbackup_block_size = None

        with self.assertRaisesRegexp(Exception, 'Error backing up file to NetBackup'):
            backup_file_with_nbu(self.context, path=self.netbackup_filepath)

    @patch('gppylib.operations.backup_utils.Command.run')
    def test_backup_file_with_nbu_with_keyword(self, mock1):
        netbackup_keyword = "hello"

        backup_file_with_nbu(self.context, path=self.netbackup_filepath)

    @patch('gppylib.operations.backup_utils.Command.run')
    def test_backup_file_with_nbu_with_keyword_and_segment(self, mock1):
        netbackup_keyword = "hello"
        segment_hostname = "sdw"

        backup_file_with_nbu(self.context, path=self.netbackup_filepath, hostname=segment_hostname)

    @patch('gppylib.operations.backup_utils.Command.run')
    def test_backup_file_with_nbu_no_block_size_with_keyword_and_segment(self, mock1):
        self.context.netbackup_block_size = None
        segment_hostname = "sdw"
        netbackup_keyword = "hello"

        backup_file_with_nbu(self.context, path=self.netbackup_filepath, hostname=segment_hostname)

    @patch('gppylib.operations.backup_utils.Command.run')
    def test_backup_file_with_nbu_with_keyword_and_segment(self, mock1):
        self.context.netbackup_block_size = None
        netbackup_keyword = "hello"

        backup_file_with_nbu(self.context, path=self.netbackup_filepath)

    @patch('gppylib.operations.backup_utils.Command.run')
    def test_restore_file_with_nbu_no_block_size_with_segment(self, mock1):
        segment_hostname = "sdw"
        self.context.netbackup_block_size = None

        backup_file_with_nbu(self.context, path=self.netbackup_filepath, hostname=segment_hostname)

    @patch('gppylib.operations.backup_utils.Command.run', side_effect=Exception('Error backing up file to NetBackup'))
    def test_restore_file_with_nbu_no_block_size_with_segment_and_error(self, mock1):
        segment_hostname = "sdw"
        self.context.netbackup_block_size = None

        with self.assertRaisesRegexp(Exception, 'Error backing up file to NetBackup'):
            restore_file_with_nbu(self.context, path=self.netbackup_filepath, hostname=segment_hostname)

    @patch('gppylib.operations.backup_utils.Command.run')
    def test_restore_file_with_nbu_big_block_size(self, mock1):
        self.context.netbackup_block_size = 1024

        restore_file_with_nbu(self.context, path=self.netbackup_filepath)

    @patch('gppylib.operations.backup_utils.Command.run')
    def test_restore_file_with_nbu_with_segment_and_big_block_size(self, mock1):
        segment_hostname = "sdw"
        self.context.netbackup_block_size = 2048

        restore_file_with_nbu(self.context, path=self.netbackup_filepath, hostname=segment_hostname)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.dump.Command.get_results', return_value=CommandResult(0, "/tmp/db_dumps/foo", "", True, False))
    def test_check_file_dumped_with_nbu_default(self, mock1, mock2):
        self.assertTrue(check_file_dumped_with_nbu(self.context, path=self.netbackup_filepath))

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.dump.Command.get_results', return_value=CommandResult(0, "", "", True, False))
    def test_check_file_dumped_with_nbu_no_return(self, mock1, mock2):
        self.assertFalse(check_file_dumped_with_nbu(self.context, path=self.netbackup_filepath))

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.dump.Command.get_results', return_value=CommandResult(0, "/tmp/db_dumps/foo", "", True, False))
    def test_check_file_dumped_with_nbu_with_segment(self, mock1, mock2):
        hostname = "sdw"

        self.assertTrue(check_file_dumped_with_nbu(self.context, path=self.netbackup_filepath, hostname=hostname))

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.dump.Command.get_results', return_value=CommandResult(0, "", "", True, False))
    def test_check_file_dumped_with_nbu_with_segment_and_no_return(self, mock1, mock2):
        hostname = "sdw"

        self.assertFalse(check_file_dumped_with_nbu(self.context, path=self.netbackup_filepath, hostname=hostname))

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/tmp/gp_dump_20160701000000_increments\n", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['20160701000000', '20160715000000', '20160804000000'])
    def test_get_full_timestamp_for_incremental_with_nbu_default(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        self.context.timestamp = '20160804000000'
        expected_output = '20160701000000'

        result = get_full_timestamp_for_incremental_with_nbu(self.context)
        self.assertEquals(result, expected_output)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/tmp/gp_dump_20160701000000_increments\n", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['20160701000000', '20160715000000'])
    def test_get_full_timestamp_for_incremental_with_nbu_no_full_timestamp(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        self.context.timestamp = '20160804000000'
        expected_output = None

        result = get_full_timestamp_for_incremental_with_nbu(self.context)
        self.assertEquals(result, expected_output)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_lines_from_file')
    def test_get_full_timestamp_for_incremental_with_nbu_empty_file(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        self.context.timestamp = '20160804000000'
        expected_output = None

        result = get_full_timestamp_for_incremental_with_nbu(self.context)
        self.assertEquals(result, expected_output)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/tmp/gp_dump_20160701000000_increments\n/tmp/gp_dump_20160801000000_increments\n", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['20160701000000', '20160715000000'])
    def test_get_full_timestamp_for_incremental_with_nbu_later_timestamp(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        self.context.timestamp = '20160804000000'
        expected_output = None

        result = get_full_timestamp_for_incremental_with_nbu(self.context)
        self.assertEquals(result, expected_output)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/tmp/gp_dump_20160701000000_increments\n/tmp/gp_dump_20160801000000_increments\n", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['20160710000000', '20160720000000', '20160804000000'])
    def test_get_full_timestamp_for_incremental_with_nbu_multiple_increments(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        self.context.timestamp = '20160804000000'
        expected_output = '20160701000000'

        result = get_full_timestamp_for_incremental_with_nbu(self.context)
        self.assertEquals(result, expected_output)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/tmp/foo_gp_dump_20160701000000_increments\n/tmp/foo_gp_dump_20160801000000_increments\n", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['20160710000000', '20160720000000', '20160804000000'])
    def test_get_full_timestamp_for_incremental_with_nbu_with_prefix(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        self.context.timestamp = '20160804000000'
        self.context.dump_prefix = 'foo'
        expected_output = '20160701000000'

        result = get_full_timestamp_for_incremental_with_nbu(self.context)
        self.assertEquals(result, expected_output)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/tmp/foo_gp_dump_20160701000000_increments\n/tmp/foo_gp_dump_20160801000000_increments\n", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_lines_from_file', return_value=['20160710000000', '20160720000000'])
    def test_get_full_timestamp_for_incremental_with_nbu_no_matching_increment(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        self.context.timestamp = '20160804000000'
        self.context.dump_prefix = 'foo'
        expected_output = None

        result = get_full_timestamp_for_incremental_with_nbu(self.context)
        self.assertEquals(result, expected_output)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/data/gp_dump_20160701000000.rpt\n", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_full_ts_from_report_file', return_value='20160701000000')
    def test_get_latest_full_ts_with_nbu_default(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        expected_output = '20160701000000'

        result = get_latest_full_ts_with_nbu(self.context)
        self.assertEquals(result, expected_output)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/data/gp_dump_20160701000000.rpt\n", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_full_ts_from_report_file', return_value=None)
    def test_get_latest_full_ts_with_nbu_no_full(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024

        with self.assertRaisesRegexp(Exception, 'No full backup found for given incremental on the specified NetBackup server'):
            get_latest_full_ts_with_nbu(self.context)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_full_ts_from_report_file', return_value=None)
    def test_get_latest_full_ts_with_nbu_no_report_file(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024

        with self.assertRaisesRegexp(Exception, 'No full backup found for given incremental on the specified NetBackup server'):
            get_latest_full_ts_with_nbu(self.context)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/tmp/gp_dump_20160701000000.rpt\n/tmp/gp_dump_20160720000000.rpt", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_full_ts_from_report_file', return_value=None)
    def test_get_latest_full_ts_with_nbu_empty_report_file(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024

        with self.assertRaisesRegexp(Exception, 'No full backup found for given incremental on the specified NetBackup server'):
            get_latest_full_ts_with_nbu(self.context)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/tmp/gp_dump_20160701000000.rpt\n/tmp/gp_dump_20160720000000.rpt", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_full_ts_from_report_file', return_value='20160701000000')
    def test_get_latest_full_ts_with_nbu_default(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        expected_output = '20160701000000'

        result = get_latest_full_ts_with_nbu(self.context)
        self.assertEquals(result, expected_output)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "/tmp/gp_dump_20160701000000.rpt\n", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_full_ts_from_report_file', return_value=None)
    def test_get_latest_full_ts_with_nbu_with_prefix(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        self.context.dump_prefix = 'foo'
        expected_output = None

        with self.assertRaisesRegexp(Exception, 'No full backup found for given incremental on the specified NetBackup server'):
            get_latest_full_ts_with_nbu(self.context)

    @patch('gppylib.operations.backup_utils.Command.run')
    @patch('gppylib.operations.backup_utils.Command.get_results', return_value=CommandResult(0, "No object matched the specified predicate\n", "", True, False))
    @patch('gppylib.operations.backup_utils.restore_file_with_nbu')
    @patch('gppylib.operations.backup_utils.get_full_ts_from_report_file', return_value=None)
    def test_get_latest_full_ts_with_nbu_no_object(self, mock1, mock2, mock3, mock4):
        self.context.netbackup_block_size = 1024
        expected_output = None

        output = get_latest_full_ts_with_nbu(self.context)
        self.assertEquals(output, expected_output)

    # Yes, this is hackish, but mocking os.environ.get doesn't work.
    def test_init_context_with_no_mdd(self):
        old_mdd = os.environ.get('MASTER_DATA_DIRECTORY')
        try:
            os.environ['MASTER_DATA_DIRECTORY'] = ""
            with self.assertRaisesRegexp(Exception, 'Environment Variable MASTER_DATA_DIRECTORY not set!'):
                context = Context()
        finally:
            os.environ['MASTER_DATA_DIRECTORY'] = old_mdd

    def test_tablename_list_to_tuple_list_default(self):
        table_list = ['public.foo', 'public.bar']
        expected = [('public', 'foo'), ('public', 'bar')]
        results = tablename_list_to_tuple_list(table_list)
        self.assertEqual(expected, results)

    def test_tablename_list_to_tuple_list_no_schemas(self):
        table_list = ['foo', 'public.bar']
        with self.assertRaisesRegexp(Exception, "not in the format schema.table"):
            results = tablename_list_to_tuple_list(table_list)

    def test_tablename_list_to_tuple_list_too_many_tokens(self):
        table_list = ['testdb.public.foo', 'public.bar']
        with self.assertRaisesRegexp(Exception, "not in the format schema.table"):
            results = tablename_list_to_tuple_list(table_list)

    def test_tablename_list_to_tuple_list_bad_format(self):
        table_list = ['public,foo', 'public.bar']
        with self.assertRaisesRegexp(Exception, "not in the format schema.table"):
            results = tablename_list_to_tuple_list(table_list)

    def test_tablename_list_to_tuple_list_empty_list(self):
        results = tablename_list_to_tuple_list([])
        self.assertEqual([], results)

    def test_tablename_list_to_tuple_list_special_chars(self):
        table_list = ['public."foo!$""\t\n,."', 'public.bar']
        expected = [('public', 'foo!$"\t\n,.'), ('public', 'bar')]
        results = tablename_list_to_tuple_list(table_list)
        self.assertEqual(expected, results)

    def test_list_to_csv_string_default(self):
        table_list = ['public', 'foo']
        expected = 'public,foo\n'
        results = list_to_csv_string(table_list)
        self.assertEqual(expected, results)

    def test_list_to_csv_string_tuple(self):
        table_list = ('public', 'foo')
        expected = 'public,foo\n'
        results = list_to_csv_string(table_list)
        self.assertEqual(expected, results)

    def test_list_to_csv_string_more_items(self):
        table_list = ['testdb', 'public', 'foo']
        expected = 'testdb,public,foo\n'
        results = list_to_csv_string(table_list)
        self.assertEqual(expected, results)

    def test_list_to_csv_string_different_delimiter(self):
        table_list = ['public', 'foo']
        expected = 'public.foo\n'
        results = list_to_csv_string(table_list, delimiter='.')
        self.assertEqual(expected, results)

    def test_list_to_csv_string_different_terminator(self):
        table_list = ['public', 'foo']
        expected = 'public,foo'
        results = list_to_csv_string(table_list, terminator='')
        self.assertEqual(expected, results)

    def test_list_to_csv_string_empty_list(self):
        results = list_to_csv_string([])
        self.assertEqual('\n', results)

    def test_list_to_csv_string_special_chars(self):
        table_list = ['public', 'foo!$"\t\n,.']
        expected = 'public,"foo!$""\t\n,."\n'
        results = list_to_csv_string(table_list)
        self.assertEqual(expected, results)

    def test_list_to_csv_string_special_chars_no_delimiter_or_terminator_or_quotechar(self):
        table_list = ['public', 'foo!$\t.']
        expected = 'public,foo!$\t.\n'
        results = list_to_csv_string(table_list)
        self.assertEqual(expected, results)

    def test_csv_string_to_tuple_default(self):
        csv_string = 'public,foo\n'
        expected = ('public', 'foo')
        results = csv_string_to_tuple(csv_string)
        self.assertEqual(expected, results)

    def test_csv_string_to_tuple_more_tokens(self):
        csv_string = 'testdb,public,foo\n'
        expected = ('testdb', 'public', 'foo')
        results = csv_string_to_tuple(csv_string)
        self.assertEqual(expected, results)

    def test_csv_string_to_tuple_wrong_delimiter(self):
        csv_string = 'public.foo\n'
        expected = ('public.foo', )
        results = csv_string_to_tuple(csv_string)
        self.assertEqual(expected, results)

    def test_csv_string_to_tuple_no_terminator(self):
        csv_string = 'public,foo'
        expected = ('public', 'foo')
        results = csv_string_to_tuple(csv_string)
        self.assertEqual(expected, results)

    def test_csv_string_to_tuple_empty_string(self):
        results = csv_string_to_tuple('')
        self.assertEqual((), results)

    def test_csv_string_to_tuple_special_chars(self):
        csv_string = 'public,"foo!$""\t\n,."\n'
        expected = ('public', 'foo!$"\t\n,.')
        results = csv_string_to_tuple(csv_string)
        self.assertEqual(expected, results)

    def test_csv_string_to_tuple_special_chars_dot_delimiter(self):
        csv_string = '" S`~@#$%^&*()-+[{]}|\;: \'""/?><1 "." ao_T`~@#$%^&*()-+[{]}|\;: \'""/?><1 "'
        expected = (' S`~@#$%^&*()-+[{]}|\;: \'"/?><1 ',' ao_T`~@#$%^&*()-+[{]}|\;: \'"/?><1 ')
        results = csv_string_to_tuple(csv_string,delimiter='.',terminator='')
        self.assertEqual(expected, results)

    def test_list_to_csv_string_special_chars_no_delimiter_or_terminator_or_quotechar(self):
        csv_string = 'public,foo!$\t.\n'
        expected = ('public', 'foo!$\t.')
        results = csv_string_to_tuple(csv_string)
        self.assertEqual(expected, results)

    def test_tuple_to_tablename_default(self):
        tuple = ['public', 'foo']
        expected = 'public.foo'
        results = tuple_to_tablename(tuple)
        self.assertEqual(expected, results)

    def test_tuple_to_tablename_with_tuple(self):
        tuple = ('public', 'foo')
        expected = 'public.foo'
        results = tuple_to_tablename(tuple)
        self.assertEqual(expected, results)

    def test_tuple_to_tablename_no_schemas(self):
        tuple = ['foo']
        with self.assertRaisesRegexp(Exception, 'not in the format schema.table'):
            results = tuple_to_tablename(tuple)

    def test_tuple_to_tablename_too_long(self):
        tuple = ['testdb', 'public', 'foo']
        with self.assertRaisesRegexp(Exception, 'not in the format schema.table'):
            results = tuple_to_tablename(tuple)

    def test_tuple_to_tablename_empty_list(self):
        tuple = []
        with self.assertRaisesRegexp(Exception, 'not in the format schema.table'):
            results = tuple_to_tablename(tuple)

    def test_tuple_to_tablename_special_chars(self):
        tuple = ['public', 'foo!$\t\n,.']
        expected = 'public."foo!$\t\n,."'
        results = tuple_to_tablename(tuple)
        self.assertEqual(expected, results)

    def test_tablename_to_tuple_default(self):
        tablename = 'public.foo'
        expected = ('public', 'foo')
        results = tablename_to_tuple(tablename)
        self.assertEqual(expected, results)

    def test_tablename_to_tuple_no_schemas(self):
        tablename = 'foo'
        with self.assertRaisesRegexp(Exception, 'not in the format schema.table'):
            results = tablename_to_tuple(tablename)

    def test_tablename_to_tuple_bad_format(self):
        tablename = 'public,foo'
        with self.assertRaisesRegexp(Exception, 'not in the format schema.table'):
            results = tablename_to_tuple(tablename)

    def test_tablename_to_tuple_empty_string(self):
        tablename = ''
        with self.assertRaisesRegexp(Exception, 'not in the format schema.table'):
            results = tablename_to_tuple(tablename)

    def test_tablename_to_tuple_special_chars(self):
        tablename = 'public."foo!$\t\n,."'
        expected = ('public', 'foo!$\t\n,.')
        results = tablename_to_tuple(tablename)
        self.assertEqual(expected, results)

    def test_get_lines_from_csv_file_default(self):
        lines = ['public.foo\n', 'public.bar\n']
        filename = '/tmp/testfile'
        write_lines_to_file(filename, lines)
        expected = [('public', 'foo'), ('public', 'bar')]
        results = get_lines_from_csv_file(filename)
        self.assertEqual(expected, results)
        os.remove(filename)

    def test_get_lines_from_csv_file_different_delimiter(self):
        lines = ['public,foo\n', 'public,bar\n']
        filename = '/tmp/testfile'
        write_lines_to_file(filename, lines)
        expected = [('public', 'foo'), ('public','bar')]
        results = get_lines_from_csv_file(filename, delimiter=',')
        self.assertEqual(expected, results)
        os.remove(filename)

    def test_get_lines_from_csv_file_empty_file(self):
        filename = '/tmp/testfile'
        open(filename, 'a').close()
        results = get_lines_from_csv_file(filename)
        self.assertEqual([], results)
        os.remove(filename)

    def test_get_lines_from_csv_file_nonexistent_file(self):
        filename = '/tmp/testfile'
        with self.assertRaisesRegexp(Exception, "No such file or directory"):
            results = get_lines_from_csv_file(filename)

    def test_get_lines_from_csv_file_special_chars(self):
        lines = ['public."foo!$\t\n,."\n', 'public.bar\n']
        filename = '/tmp/testfile'
        write_lines_to_file(filename, lines)
        expected = [('public', 'foo!$\t\n,.'), ('public', 'bar')]
        results = get_lines_from_csv_file(filename)
        self.assertEqual(expected, results)
        os.remove(filename)

    @patch('gppylib.operations.backup_utils.get_lines_from_dd_file', return_value=['public.foo', 'public.bar'])
    def test_get_lines_from_csv_file_ddboost(self, mock1):
        fake_context = Mock()
        fake_context.ddboost = True
        filename = '/tmp/testfile'
        expected = [('public', 'foo'), ('public', 'bar')]
        results = get_lines_from_csv_file(filename, fake_context)
        self.assertEqual(expected, results)

    def test_write_lines_to_csv_file_default(self):
        lines= [['public', 'foo'], ['public', 'bar']]
        filename = '/tmp/testfile'
        m = mock_open()
        with patch('__builtin__.open', m, create=True):
            write_lines_to_csv_file(filename, lines)
            result = m()
            self.assertEqual(len(lines), len(result.write.call_args_list))
            for i in range(len(lines)):
                table = "%s.%s\n" % tuple(lines[i])
                self.assertEqual(call(table), result.write.call_args_list[i])

    def test_write_lines_to_csv_file_different_delimiter(self):
        lines= [['public', 'foo'], ['public', 'bar']]
        filename = '/tmp/testfile'
        m = mock_open()
        with patch('__builtin__.open', m, create=True):
            write_lines_to_csv_file(filename, lines, delimiter=',')
            result = m()
            self.assertEqual(len(lines), len(result.write.call_args_list))
            for i in range(len(lines)):
                table = "%s,%s\n" % tuple(lines[i])
                self.assertEqual(call(table), result.write.call_args_list[i])

    def test_write_lines_to_csv_file_empty_list(self):
        lines= []
        filename = '/tmp/testfile'
        m = mock_open()
        with patch('__builtin__.open', m, create=True):
            write_lines_to_csv_file(filename, lines)
            result = m()
            self.assertEqual(0, len(result.write.call_args_list))

    def test_write_lines_to_csv_file_special_chars(self):
        lines= [['public', 'foo!$\t\n,.'], ['public', 'bar']]
        expected = [call('public."foo!$\t\n,."\n'), call('public.bar\n')]
        filename = '/tmp/testfile'
        m = mock_open()
        with patch('__builtin__.open', m, create=True):
            write_lines_to_csv_file(filename, lines)
            result = m()
            self.assertEqual(len(lines), len(result.write.call_args_list))
            self.assertEqual(expected, result.write.call_args_list)

    def test_write_lines_to_csv_file_always_quote(self):
        lines= [['public', 'foo!$\t\n,.'], ['public', 'bar']]
        filename = '/tmp/testfile'
        m = mock_open()
        with patch('__builtin__.open', m, create=True):
            write_lines_to_csv_file(filename, lines, alwaysquote=True)
            result = m()
            self.assertEqual(len(lines), len(result.write.call_args_list))
            for i in range(len(lines)):
                table = '"%s"."%s"\n' % tuple(lines[i])
                self.assertEqual(call(table), result.write.call_args_list[i])

    def test_convert_list_of_list_to_list_of_tuples_all_list(self):
        lol = [['a','b'], ['b','c'], ['e','f']]
        expected = [('a','b'), ('b','c'), ('e','f')]
        result = convert_list_of_list_to_list_of_tuples(lol)
        self.assertEqual(result, expected)

    def test_convert_list_of_list_to_list_of_tuples_list_and_tuples(self):
        lot = [['a','b'], ('b','c'), ['e','f']]
        expected = [('a','b'), ('b','c'), ('e','f')]
        result = convert_list_of_list_to_list_of_tuples(lot)
        self.assertEqual(result, expected)


    def test_convert_list_of_list_to_set_of_tuples_all_list(self):
        lol = [['a','b'], ['b','c'], ['e','f']]
        expected = set([('a','b'), ('b','c'), ('e','f')])
        result = convert_list_of_list_to_set_of_tuples(lol)
        self.assertEqual(result, expected)

    def test_convert_list_of_list_to_set_of_tuples_list_and_tuples(self):
        lot = [['a','b'], ('b','c'), ['e','f']]
        expected = set([('a','b'), ('b','c'), ('e','f')])
        result = convert_list_of_list_to_set_of_tuples(lot)
        self.assertEqual(result, expected)
