#!/usr/bin/env python3

import unittest
import gppylib
from mock import Mock, patch
from gppylib.operations.getSegmentTablespaceDir import GetTablespaces

class GetTablespaceDirTestCase(unittest.TestCase):

    def setUp(self):
        self.tablespace = GetTablespaces()
        patch('gppylib.operations.getSegmentTablespaceDir.dbconn.connect')
        gppylib.operations.getSegmentTablespaceDir.dbconn.query = Mock(return_value=[])

    def test_validate_no_tablespace_oids(self):
        self.assertEqual([], self.tablespace.get_tablespace_oids())

    def test_validate_with_tablespace_oids(self):
        gppylib.operations.getSegmentTablespaceDir.dbconn.query = Mock(return_value=[[('1234')],[('2345')]])
        expected = [('1234'),('2345')]
        self.assertEqual(expected, self.tablespace.get_tablespace_oids())

    def test_validate_empty_with_allhost_fetch_tablespace_dirs(self):
        tablespace_oids = ['1234']
        self.assertEqual([], self.tablespace.fetch_tablespace_dirs(True,tablespace_oids))

    def test_validate_empty_with_no_allhost_fetch_tablespace_dirs(self):
        tablespace_oids = ['1234']
        data_directory = '/data/primary/seg1'
        self.assertEqual([], self.tablespace.fetch_tablespace_dirs(False, tablespace_oids, data_directory))

    def test_validate_data_with_host_fetch_tablespace_dirs(self):
        gppylib.operations.getSegmentTablespaceDir.dbconn.query = Mock(side_effect=[[('sdw1','/data/tblsp1')]])
        expected = [('sdw1','/data/tblsp1')]
        tablespace_oids = ['1234']
        self.assertEqual(expected, self.tablespace.fetch_tablespace_dirs(True, tablespace_oids, None))

    def test_validate_data_with_datadir_fetch_tablespace_dirs(self):
        gppylib.operations.getSegmentTablespaceDir.dbconn.query = Mock(side_effect=[[('sdw1',1,'/data/tblsp1')]])
        expected = [('sdw1',1,'/data/tblsp1')]
        data_directory = '/data/tblsp1'
        tablespace_oids = ['1234']
        self.assertEqual(expected, self.tablespace.fetch_tablespace_dirs(False, tablespace_oids, data_directory))
