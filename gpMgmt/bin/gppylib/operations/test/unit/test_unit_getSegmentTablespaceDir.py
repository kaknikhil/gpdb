#!/usr/bin/env python3

import unittest
import gppylib
from mock import Mock, patch
from gppylib.operations.getSegmentTablespaceDir import GetTablespaces
from test.unit.gp_unittest import GpTestCase

class GetTablespaceDirTestCase(GpTestCase):
    def __init__(self, arg):
        super().__init__(arg)
        self.tablespace = GetTablespaces()
        self.mock_query = Mock(return_value=[])

    def setUp(self):
        self.apply_patches([
            patch('gppylib.operations.getSegmentTablespaceDir.dbconn.connect'),
            patch('gppylib.operations.getSegmentTablespaceDir.dbconn.query', self.mock_query)
        ])

    def tearDown(self):
        super(GetTablespaceDirTestCase, self).tearDown()

    def test_validate_no_tablespace_oids(self):
        self.assertEqual([], self.tablespace.get_tablespace_oids())

    def test_validate_with_tablespace_oids(self):
        self.mock_query.return_value = [[('1234')],[('2345')]]
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
        self.mock_query.side_effect =[[('sdw1','/data/tblsp1')]]
        expected = [('sdw1','/data/tblsp1')]
        tablespace_oids = ['1234']
        self.assertEqual(expected, self.tablespace.fetch_tablespace_dirs(True, tablespace_oids, None))

    def test_validate_data_with_datadir_fetch_tablespace_dirs(self):
        self.mock_query.side_effect =[[('sdw1',1,'/data/tblsp1')]]
        expected = [('sdw1',1,'/data/tblsp1')]
        data_directory = '/data/tblsp1'
        tablespace_oids = ['1234']
        self.assertEqual(expected, self.tablespace.fetch_tablespace_dirs(False, tablespace_oids, data_directory))
