#!/usr/bin/env python3

import copy
import io
from mock import Mock, patch, MagicMock
import tempfile

import gppylib
from gparray import Segment, GpArray
from gppylib.programs import clsRecoverSegment_triples
from gppylib.programs.clsRecoverSegment_triples import ConfigFileMirrorBuilder, MirrorBuilderFactory, RecoverTriplet
from test.unit.gp_unittest import GpTestCase

class MirrorBuilderFactoryTestCase(GpTestCase):
    def setUp(self):
        # Set maxDiff to None to see the entire diff on the console in case of failure
        self.maxDiff = None

        # we always mock ping.local to be a no-op. Once we refactor -i and -p to
        # use the same code path, this mock will no longer be needed
        clsRecoverSegment_triples.unix.Ping.local = Mock()

    def run_single_ConfigFile_test(self, test):
        with tempfile.NamedTemporaryFile() as f:
            f.write(test["config"].encode("utf-8"))
            f.flush()
            return self._run_single_GpArrayMirrorBuilder_test(test["gparray"], f.name, None, None, test.get("unreachable_existing_hosts"))

    def run_single_GpArray_test(self, test):
        return self._run_single_GpArrayMirrorBuilder_test(test["gparray"], None, test["new_hosts"], test.get("unreachable_hosts"),
                                     test.get("unreachable_existing_hosts"))

    #TODO: do we want new hosts here?  We do not officially support new hosts with "-i"
    def test_ConfigFileMirrorBuilder_getMirrorTriples_should_pass(self):
        tests = [
            {
                "name": "blank_config_file",
                "gparray": self.all_up_gparray_str,
                "config": "",
                "expected": [],
            },
            {
                "name": "single_old_and_new",
                "gparray": self.all_up_gparray_str,
                "config": "sdw2|21000|/mirror/gpseg0 sdw3|41000|/mirror/gpseg5_new",
                "expected": [self._triplet('10|0|m|m|s|u|sdw2|sdw2|21000|/mirror/gpseg0',
                                           '2|0|p|p|s|u|sdw1|sdw1|20000|/primary/gpseg0',
                                           '10|0|m|m|s|u|sdw3|sdw3|41000|/mirror/gpseg5_new')]
            },
            {
                "name": "multiple_old_and_new",
                "gparray": self.all_up_gparray_str,
                "config": """sdw2|21000|/mirror/gpseg0 sdw3|41001|/mirror/gpseg4
                             sdw2|21001|/mirror/gpseg1 sdw2|41001|/mirror/gpseg_new
                             sdw4|21000|/mirror/gpseg4
                             sdw1|21000|/mirror/gpseg6 sdw4|41000|/mirror/gpseg7""",
                "expected": [self._triplet('10|0|m|m|s|u|sdw2|sdw2|21000|/mirror/gpseg0',
                                           '2|0|p|p|s|u|sdw1|sdw1|20000|/primary/gpseg0',
                                           '10|0|m|m|s|u|sdw3|sdw3|41001|/mirror/gpseg4'),
                             self._triplet('11|1|m|m|s|u|sdw2|sdw2|21001|/mirror/gpseg1',
                                           '3|1|p|p|s|u|sdw1|sdw1|20001|/primary/gpseg1',
                                           '11|1|m|m|s|u|sdw2|sdw2|41001|/mirror/gpseg_new'),
                             self._triplet('14|4|m|m|s|u|sdw4|sdw4|21000|/mirror/gpseg4',
                                           '6|4|p|p|s|u|sdw3|sdw3|20000|/primary/gpseg4',
                                           None),
                             self._triplet('16|6|m|m|s|u|sdw1|sdw1|21000|/mirror/gpseg6',
                                           '8|6|p|p|s|u|sdw4|sdw4|20000|/primary/gpseg6',
                                           '16|6|m|m|s|u|sdw4|sdw4|41000|/mirror/gpseg7')]
            },
            # TODO: this should work but the failedSegment in the gparray is mutated but should not be...
            # {
            #     "name": "multiple_old_with_unreachable_existing_hosts",
            #     "gparray": self.all_up_gparray_str,
            #     "config": """sdw2|21000|/mirror/gpseg0 sdw3|41001|/mirror/gpseg4
            #                  sdw4|21000|/mirror/gpseg4
            #                  sdw1|21000|/mirror/gpseg6 sdw4|41000|/mirror/gpseg7""",
            #     "unreachable_existing_hosts": ['sdw2'],
            #     "expected": [self._triplet('14|4|m|m|s|u|sdw4|sdw4|21000|/mirror/gpseg4',
            #                                '6|4|p|p|s|u|sdw3|sdw3|20000|/primary/gpseg4',
            #                                None),
            #                  self._triplet('16|6|m|m|s|u|sdw1|sdw1|21000|/mirror/gpseg6',
            #                                '8|6|p|p|s|u|sdw4|sdw4|20000|/primary/gpseg6',
            #                                '16|6|m|m|s|u|sdw4|sdw4|41000|/mirror/gpseg7')]
            # },
            {
                "name": "in_place_1_part",
                "gparray": self.all_up_gparray_str,
                "config": "sdw2|21000|/mirror/gpseg0",
                "expected": [self._triplet('10|0|m|m|s|u|sdw2|sdw2|21000|/mirror/gpseg0',
                                           '2|0|p|p|s|u|sdw1|sdw1|20000|/primary/gpseg0',
                                           None)]
            },
            {
                "name": "in_place_2_parts",
                "gparray": self.all_up_gparray_str,
                "config": "sdw2|21000|/mirror/gpseg0 sdw2|21000|/mirror/gpseg0",
                "expected": [self._triplet('10|0|m|m|s|u|sdw2|sdw2|21000|/mirror/gpseg0',
                                           '2|0|p|p|s|u|sdw1|sdw1|20000|/primary/gpseg0',
                                           '10|0|m|m|s|u|sdw2|sdw2|21000|/mirror/gpseg0')]
            },
            {
                "name": "in_place_1_part_unreachable_exising_host",
                "gparray": self.all_up_gparray_str,
                "config": "sdw2|21000|/mirror/gpseg0",
                "unreachable_existing_hosts": ['sdw2'],
                "expected": []
            }
        ]
        self.run_pass_tests(tests, self.run_single_ConfigFile_test)

    def test_ConfigFileMirrorBuilder_getMirrorTriples_should_fail(self):
        tests = [
            {
                "name": "invalid_failed_address",
                "gparray": self.three_failedover_segs_gparray_str,
                "config": "seg_does_not_exist|20000|/primary/gpseg0 sdw3|20001|/primary/gpseg5",
                "expected": "segment to recover was not found in configuration.*described by.*seg_does_not_exist"
            },
            {
                "name": "invalid_failed_port1",
                "gparray": self.three_failedover_segs_gparray_str,
                "config": "sdw1|99999|/primary/gpseg0 sdw3|20001|/primary/gpseg5",
                "expected": "segment to recover was not found in configuration.*described by.*99999"
            },
            {
                "name": "invalid_failed_port2",
                "gparray": self.three_failedover_segs_gparray_str,
                "config": "sdw1|port_does_not_exist|/primary/gpseg0 sdw3|20001|/primary/gpseg5",
                "expected": "Invalid port"
            },
            {
                "name": "invalid_failed_data_dir",
                "gparray": self.three_failedover_segs_gparray_str,
                "config": "sdw1|20000|/does/not/exist sdw3|20001|/primary/gpseg5",
                "expected": "segment to recover was not found in configuration.*described by.*exist"
            },
            {
                "name": "invalid_failed_and_new_address",
                "gparray": self.three_failedover_segs_gparray_str,
                "config": "seg_does_not_exist_old|20000|/primary/gpseg0 seg_does_not_exist_new|20001|/primary/gpseg5",
                "expected": "segment to recover was not found in configuration.*described by.*seg_does_not_exist_old"
            },
            {
                "name": "invalid_recovery_port1",
                "gparray": self.three_failedover_segs_gparray_str,
                "config": "sdw1|20000|/primary/gpseg0 sdw3|port_does_not_exist|/primary/gpseg5",
                "expected": "Invalid port"
            },
            {
                "name": "no_peer_for_failed_seg",
                "gparray": self.content0_no_peer_gparray_str,
                "config": "sdw1|20000|/primary/gpseg0 sdw3|20000|/primary/gpseg5",
                "expected": "No peer found for dbid 2"
            },
            {
                "name": "both_peers_down",
                "gparray": self.content0_mirror_and_its_peer_down_gparray_str,
                "config": "sdw1|20000|/primary/gpseg0 sdw3|20000|/primary/gpseg5",
                "expected": "Both segments for content 0 are down. Try restarting"
            },
            #
            #
            # TODO: these should fail, but right now do not.  For recovery port and host, we should detect them here.
            #   For recovery data directory, it is not clear where the check should go.
            # {
            #     "name": "invalid_recovery_address",
            #     "config": "sdw1|20000|/primary/gpseg0 host_does_not_exist|20001|/primary/gpseg5",
            # },
            # {
            #     "name": "invalid_recovery_port",
            #     "config": "sdw1|20000|/primary/gpseg0 sdw3|99999|/primary/gpseg5",
            # },
            # {
            #     "name": "invalid_recovery_data_dir",
            #     "config": "sdw1|20000|/primary/gpseg0 sdw3|20001|/does/not/exist",
            # },
        ]
        self.run_fail_tests(tests, self.run_single_ConfigFile_test)

    def test_GpArrayMirrorBuilder_getMirrorTriples_should_pass(self):
        tests = [{
                "name": "no_new_hosts",
                "gparray": self.three_failedover_segs_gparray_str,
                "new_hosts": [],
                "expected": [self._triplet('2|0|m|p|s|d|sdw1|sdw1|20000|/primary/gpseg0',
                                           '6|0|p|m|s|u|sdw2|sdw2|21000|/mirror/gpseg0',
                                           None),
                             self._triplet('3|1|m|p|s|d|sdw1|sdw1|20001|/primary/gpseg1',
                                           '7|1|p|m|s|u|sdw2|sdw2|21001|/mirror/gpseg1',
                                           None),
                             self._triplet('4|2|m|p|s|d|sdw2|sdw2|20000|/primary/gpseg2',
                                           '8|2|p|m|s|u|sdw3|sdw3|21000|/mirror/gpseg2',
                                           None)]
            },
            {
                "name": "one_existing_host_down",
                "gparray": self.three_failedover_segs_gparray_str,
                "new_hosts": [],
                "unreachable_existing_hosts": ['sdw1'],
                "expected": [self._triplet('4|2|m|p|s|d|sdw2|sdw2|20000|/primary/gpseg2',
                                           '8|2|p|m|s|u|sdw3|sdw3|21000|/mirror/gpseg2',
                                           None)]
            },
            {
                "name": "all_relevant_existing_hosts_down",
                "gparray": self.three_failedover_segs_gparray_str,
                "new_hosts": [],
                "unreachable_existing_hosts": ['sdw1', 'sdw2'],
                "expected": []
            },
            {
                "name": "no_segs_to_recover",
                "gparray": self.all_up_gparray_str,
                "new_hosts": ['new_1'],
                "expected": []
            },
            {
                "name": "enough_hosts",
                "gparray": self.three_failedover_segs_gparray_str,
                "new_hosts": ['new_1', 'new_2'],
                "expected": [self._triplet('2|0|m|p|s|d|sdw1|sdw1|20000|/primary/gpseg0',
                                           '6|0|p|m|s|u|sdw2|sdw2|21000|/mirror/gpseg0',
                                           '2|0|m|p|s|d|new_1|new_1|20000|/primary/gpseg0'),
                             self._triplet('3|1|m|p|s|d|sdw1|sdw1|20001|/primary/gpseg1',
                                           '7|1|p|m|s|u|sdw2|sdw2|21001|/mirror/gpseg1',
                                           '3|1|m|p|s|d|new_1|new_1|20001|/primary/gpseg1'),
                             self._triplet('4|2|m|p|s|d|sdw2|sdw2|20000|/primary/gpseg2',
                                           '8|2|p|m|s|u|sdw3|sdw3|21000|/mirror/gpseg2',
                                           '4|2|m|p|s|d|new_2|new_2|20000|/primary/gpseg2')]
            },
            {
                "name": "more_hosts",
                "gparray": self.three_failedover_segs_gparray_str,
                "new_hosts": ['new_1', 'new_2', 'new_3'], # This test assumes there is only one extra host
                "expected": [self._triplet('2|0|m|p|s|d|sdw1|sdw1|20000|/primary/gpseg0',
                                           '6|0|p|m|s|u|sdw2|sdw2|21000|/mirror/gpseg0',
                                           '2|0|m|p|s|d|new_1|new_1|20000|/primary/gpseg0'),
                             self._triplet('3|1|m|p|s|d|sdw1|sdw1|20001|/primary/gpseg1',
                                           '7|1|p|m|s|u|sdw2|sdw2|21001|/mirror/gpseg1',
                                           '3|1|m|p|s|d|new_1|new_1|20001|/primary/gpseg1'),
                             self._triplet('4|2|m|p|s|d|sdw2|sdw2|20000|/primary/gpseg2',
                                           '8|2|p|m|s|u|sdw3|sdw3|21000|/mirror/gpseg2',
                                           '4|2|m|p|s|d|new_2|new_2|20000|/primary/gpseg2')]
            }
        ]

        self.run_pass_tests(tests, self.run_single_GpArray_test)

    def test_GpArrayMirrorBuilder_getMirrorTriples_should_fail(self):
        tests = [
            {
                "name": "not_enough_hosts",
                "gparray": self.three_failedover_segs_gparray_str,
                "new_hosts": ['new_1'],
                "unreachable_hosts": [],
                "expected": "Not enough new recovery hosts given for recovery."
            },
            {
                "name": "all_hosts_unreachable1",
                "gparray": self.three_failedover_segs_gparray_str,
                "new_hosts": ['new_1'],
                "unreachable_hosts": ['new_1'],
                "expected": "Not enough new recovery hosts given for recovery."
            },
            {
                "name": "all_hosts_unreachable2",
                "gparray": self.three_failedover_segs_gparray_str,
                "new_hosts": ['new_1', 'new_2'],
                "unreachable_hosts": ['new_1', 'new_2'],
                "expected": "Cannot recover. The following recovery target hosts are unreachable: \['new_1', 'new_2'\]"
            },
            {
                "name": "some_hosts_unreachable",
                "gparray": self.three_failedover_segs_gparray_str,
                "new_hosts": ['new_1', 'new_2'],
                "unreachable_hosts": ['new_2'],
                "expected": "Cannot recover. The following recovery target hosts are unreachable: \['new_2'\]"
            },
            {
                "name": "no_peer_for_failed_seg",
                "gparray": self.content0_no_peer_gparray_str,
                "new_hosts": ['new_1'],
                "unreachable_hosts": [],
                "expected": "No peer found for dbid 2"
            },
            {
                "name": "both_peers_down",
                "gparray": self.content0_mirror_and_its_peer_down_gparray_str,
                "new_hosts": ['new_1'],
                "unreachable_hosts": [],
                "expected": "Both segments for content 0 are down. Try restarting"
            }
        ]
        self.run_fail_tests(tests, self.run_single_GpArray_test)

    def run_pass_tests(self, tests, fn_to_test):
        for test in tests:
            with self.subTest(msg=test["name"]):

                initial_gparray, actual_gparray, actual = fn_to_test(test)
                self.assertEqual(test["expected"], actual,
                                 msg="\n\nTest {} failed.\n\nexpected:\n{}\n\ngot:\n{}".format(
                                     test["name"], test["expected"], actual))

                expected_gparray = self.get_expected_gparray(initial_gparray, test["expected"])
                self.assertEqual(self.get_gparray_for_cmp(expected_gparray), self.get_gparray_for_cmp(actual_gparray),
                                 msg="\n\nTest {} failed.\n\ninitial:\n{}\n\nexpected:\n{}\n\ngot:\n{}".format(
                                     test["name"],
                                     self.get_gparray_for_cmp(initial_gparray),
                                     self.get_gparray_for_cmp(expected_gparray),
                                     self.get_gparray_for_cmp(actual_gparray)))

    def run_fail_tests(self, tests, fn_to_test):
        for test in tests:
            with self.subTest(msg=test["name"]):
                # make sure test does not pass trivially("" will pass assertRaisesRegex)
                self.assertTrue(test["expected"].strip() != "")
                # TODO it is possible to match partial strings with regex that might be a typo. Should we instead not
                #  use Regex and type out the exact error message ?
                with self.assertRaisesRegex(Exception, test["expected"]):
                    fn_to_test(test)

    def __init__(self, arg):
        super().__init__(arg)

        # It's possible to have no down segments, as gpmovemirrors also calls gprecoverseg.
        self.all_up_gparray_str = '''1|-1|p|p|n|u|mdw|mdw|5432|/master/gpseg-1
                                  2|0|p|p|s|u|sdw1|sdw1|20000|/primary/gpseg0
                                  3|1|p|p|s|u|sdw1|sdw1|20001|/primary/gpseg1
                                  16|6|m|m|s|u|sdw1|sdw1|21000|/mirror/gpseg6
                                  17|7|m|m|s|u|sdw1|sdw1|21001|/mirror/gpseg7
                                  10|0|m|m|s|u|sdw2|sdw2|21000|/mirror/gpseg0
                                  11|1|m|m|s|u|sdw2|sdw2|21001|/mirror/gpseg1
                                  4|2|p|p|s|u|sdw2|sdw2|20000|/primary/gpseg2
                                  5|3|p|p|s|u|sdw2|sdw2|20001|/primary/gpseg3
                                  12|2|m|m|s|u|sdw3|sdw3|21000|/mirror/gpseg2
                                  13|3|m|m|s|u|sdw3|sdw3|21001|/mirror/gpseg3
                                  6|4|p|p|s|u|sdw3|sdw3|20000|/primary/gpseg4
                                  7|5|p|p|s|u|sdw3|sdw3|20001|/primary/gpseg5
                                  14|4|m|m|s|u|sdw4|sdw4|21000|/mirror/gpseg4
                                  15|5|m|m|s|u|sdw4|sdw4|21001|/mirror/gpseg5
                                  8|6|p|p|s|u|sdw4|sdw4|20000|/primary/gpseg6
                                  9|7|p|p|s|u|sdw4|sdw4|20001|/primary/gpseg7'''

        # We include down segments, so that gprecoverseg can find them automatically
        self.three_failedover_segs_gparray_str = '''1|-1|p|p|n|u|mdw|mdw|5432|/master/gpseg-1
                                  2|0|m|p|s|d|sdw1|sdw1|20000|/primary/gpseg0
                                  3|1|m|p|s|d|sdw1|sdw1|20001|/primary/gpseg1
                                  8|2|p|m|s|u|sdw3|sdw3|21000|/mirror/gpseg2
                                  9|3|m|m|s|u|sdw3|sdw3|21001|/mirror/gpseg3
                                  6|0|p|m|s|u|sdw2|sdw2|21000|/mirror/gpseg0
                                  7|1|p|m|s|u|sdw2|sdw2|21001|/mirror/gpseg1
                                  4|2|m|p|s|d|sdw2|sdw2|20000|/primary/gpseg2
                                  5|3|p|p|s|u|sdw2|sdw2|20001|/primary/gpseg3'''

        self.content0_no_peer_gparray_str = '''1|-1|p|p|n|u|mdw|mdw|5432|/master/gpseg-1
                                  2|0|m|p|s|d|sdw1|sdw1|20000|/primary/gpseg0
                                  3|1|m|p|s|d|sdw1|sdw1|20001|/primary/gpseg1
                                  8|2|p|m|s|u|sdw3|sdw3|21000|/mirror/gpseg2
                                  9|3|m|m|s|u|sdw3|sdw3|21001|/mirror/gpseg3
                                  7|1|p|m|s|u|sdw2|sdw2|21001|/mirror/gpseg1
                                  4|2|m|p|s|d|sdw2|sdw2|20000|/primary/gpseg2
                                  5|3|p|p|s|u|sdw2|sdw2|20001|/primary/gpseg3'''

        self.content0_mirror_and_its_peer_down_gparray_str = '''1|-1|p|p|n|u|mdw|mdw|5432|/master/gpseg-1
                                  2|0|m|p|s|d|sdw1|sdw1|20000|/primary/gpseg0
                                  3|1|p|p|s|u|sdw1|sdw1|20001|/primary/gpseg1
                                  8|2|m|m|s|u|sdw3|sdw3|21000|/mirror/gpseg2
                                  9|3|m|m|s|u|sdw3|sdw3|21001|/mirror/gpseg3
                                  6|0|p|m|s|d|sdw2|sdw2|21000|/mirror/gpseg0
                                  7|1|m|m|s|u|sdw2|sdw2|21001|/mirror/gpseg1
                                  4|2|m|p|s|d|sdw2|sdw2|20000|/primary/gpseg2
                                  5|3|p|p|s|u|sdw2|sdw2|20001|/primary/gpseg3'''

    def _run_single_GpArrayMirrorBuilder_test(self, gparray_str, config_file, new_hosts, unreachable_hosts,
                                              unreachable_existing_hosts=None):
        unreachable_hosts = unreachable_hosts if unreachable_hosts else []
        gppylib.programs.clsRecoverSegment_triples.get_unreachable_segment_hosts = Mock(return_value=unreachable_hosts)

        initial_gparray = self.get_gp_array(gparray_str, unreachable_existing_hosts)
        mutated_gparray = self.get_gp_array(gparray_str, unreachable_existing_hosts)
        i = MirrorBuilderFactory.instance(mutated_gparray,
                                          config_file=config_file,
                                          new_hosts=new_hosts)
        triples = i.getMirrorTriples()

        warnings = i.getInterfaceHostnameWarnings()
        if warnings:
            # TODO Currently we do not assert if warnings should have been populated.
            expected = ["The following recovery hosts were not needed:", "\t{}".format(new_hosts[-1])]
            self.assertEqual(expected, warnings)

        return initial_gparray, mutated_gparray, triples

    @staticmethod
    def _triplet(failed, live, failover):
        return RecoverTriplet(Segment.initFromString(failed),
                              Segment.initFromString(live),
                              Segment.initFromString(failover) if failover else None)

    @staticmethod
    def get_gp_array(gparray_str, unreachable_existing_hosts=None):
        with tempfile.NamedTemporaryFile() as f:
            f.write(gparray_str.encode('utf-8'))
            f.flush()

            gparray = GpArray.initFromFile(f.name)
            if not unreachable_existing_hosts:
                return gparray

            # the caller of the function under test sets to True the "unreachable"
            # member on any segment on an unreachable host; we emulate that here.
            for seg in gparray.getSegDbList():
                if seg.getSegmentHostName() in unreachable_existing_hosts:
                    seg.unreachable = True
            return gparray

    # apply the "recover_triplets" to the "gparray" such that the "result"ing gparray
    # will be the result of a successful gprecoverseg.
    @staticmethod
    def get_expected_gparray(gparray, recover_triplets):
        result = copy.copy(gparray)
        segMap = result.getSegDbMap()

        for t in recover_triplets:
            failed, failover = t.failed, t.failover

            if not failover:
                continue

            if not failed.getSegmentDbId() in segMap:
                continue

            # these are the only fields we can change in the failed segment
            segMap[failed.getSegmentDbId()].address = failover.address
            segMap[failed.getSegmentDbId()].hostname = failover.hostname
            segMap[failed.getSegmentDbId()].port = failover.port
            segMap[failed.getSegmentDbId()].datadir = failover.datadir

        return result

    def get_gparray_for_cmp(self, gparray):
        """
        TODO This should ideally be in the __repr__ function in GpArray class.

        Get a string representation of gparray for comparison in our tests.
        We cannot rely on str(gparray) for comparison since the __str__ function for the Segment class
        does not include the port and address.
        """
        segMap = gparray.getSegDbMap()
        gparray_str = io.StringIO()
        for seg in sorted(segMap.keys()):
            gparray_str.write(repr(segMap[seg]))
            gparray_str.write('\n')

        return gparray_str.getvalue()


class MirrorBuilderConfigFileParserTestCase(GpTestCase):

    @staticmethod
    def run_single_parser_test(test):
        with tempfile.NamedTemporaryFile() as f:
            f.write(test["config"].encode("utf-8"))
            f.flush()
            return ConfigFileMirrorBuilder._parseConfigFile(f.name)

    passing_tests = [
        {
            "name": "simple_pass",
            "config": """sdw1|20000|/primary/gpseg0 sdw3|20001|/primary/gpseg5
                      sdw1|20001|/primary/gpseg1 sdw1|40001|/primary/gpseg_new
                      sdw3|20000|/primary/gpseg4
                      sdw4|20000|/primary/gpseg6 sdw4|20000|/primary/gpseg6"""
        },
        {
            "name": "6X_web_doc",
            "config": """sdw2|50000|/data2/mirror/gpseg0 sdw3|50000|/data/mirror/gpseg0
                     sdw2|50001|/data2/mirror/gpseg1 sdw4|50001|/data/mirror/gpseg1
                     sdw3|50002|/data2/mirror/gpseg2 sdw1|50002|/data/mirror/gpseg2"""},
        {
            "name": "old_to_new_new_to_other",
            "config": """sdw1|20000|/primary/gpseg0 sdw3|20001|/primary/gpseg5
                      sdw3|20001|/primary/gpseg5 sdw4|20000|/primary/gpseg6"""
        },
        {
            "name": "old_to_new_new_to_old",
            "config": """sdw1|20000|/primary/gpseg0 sdw3|20001|/primary/gpseg5
                      sdw3|20001|/primary/gpseg5 sdw1|20000|/primary/gpseg0"""
        }
        ]

    def test_parsing_should_pass(self):
        for test in self.passing_tests:
            with self.subTest(msg=test["name"]):

                rows = self.run_single_parser_test(test)
                self.assertEqual(self.parse_config(test["config"]), rows)

    failing_tests = [
        {
            "name":
                "too_many_groups",
            "config":
                """sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5 sdw3|20001|/mirror/gpseg5""",
            "expected":
                "line 1 of file .*: expected 1 or 2 groups but found 3"
        },
        {
            "name":
                "too_few_parts_in_group_1",
            "config":
                """sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5
                   sdw1|20000 sdw3|20001|/mirror/gpseg5""",
            "expected":
                "line 2 of file .*: expected 3 parts on failed segment group, obtained 2"
        },
        {
            "name":
                "too_few_parts_in_group_2",
            "config": """sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5
                         sdw2|50001|/data2/mirror/gpseg1 sdw4|50001""",
            "expected":
                "line 2 of file .*: expected 3 parts on new segment group, obtained 2"
        },
        {
            "name":
                "old_inplace_and_old_to_old",
            "config":
                """sdw1|20000|/mirror/gpseg0
                   sdw1|20000|/mirror/gpseg0 sdw1|20000|/mirror/gpseg0""",
            "expected":
                "config file lines 1 and 2 conflict: Cannot recover the same failed segment sdw1 and data "
                "directory /mirror/gpseg0 twice"
        },
        {
            "name": "old_inplace_and_old_to_old",
            "config": """sdw1|20000|/mirror/gpseg0
                         sdw1|20000|/mirror/gpseg0 sdw1|20000|/mirror/gpseg0""",
            "expected": "config file lines 1 and 2 conflict: Cannot recover the same failed segment sdw1 and data "
                        "directory /mirror/gpseg0 twice"

        },
        {
            "name": "old_inplace_and_old_inplace",
            "config": """sdw1|20000|/mirror/gpseg0
                         sdw1|20000|/mirror/gpseg0""",
            "expected": "config file lines 1 and 2 conflict: Cannot recover the same failed segment sdw1 and data "
                        "directory /mirror/gpseg0 twice"

        },
        {
            "name": "old1_to_new_and_old2_to_new",
            "config": """sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5
                         sdw2|20001|/mirror/gpseg3 sdw3|20001|/mirror/gpseg5""",
            "expected": "config file lines 1 and 2 conflict: Cannot recover to the same segment sdw3 and data directory "
                        "/mirror/gpseg5 twice"
        },
        {
            "name": "old_to_old_and_old_to_old",
            "config": """sdw1|20000|/mirror/gpseg0 sdw1|20000|/mirror/gpseg0
                         sdw1|20000|/mirror/gpseg0 sdw1|20000|/mirror/gpseg0""",
            "expected": "config file lines 1 and 2 conflict: Cannot recover the same failed segment sdw1 and data "
                        "directory /mirror/gpseg0 twice"
        },
        {
            "name": "old_to_new_and_old_inplace",
            "config": """sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5
                         sdw1|20000|/mirror/gpseg0""",
            "expected": "config file lines 1 and 2 conflict: Cannot recover the same failed segment sdw1 and data "
                        "directory /mirror/gpseg0 twice"

        },
        {
            "name": "old_to_new_and_old_to_old",
            "config": """sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5
                         sdw1|20000|/mirror/gpseg0 sdw1|20000|/mirror/gpseg0""",
            "expected": "config file lines 1 and 2 conflict: Cannot recover the same failed segment sdw1 and data "
                        "directory /mirror/gpseg0 twice"

        },
        {
            "name": "old_to_new1_and_old_to_new2",
            "config": """sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5
                         sdw1|20000|/mirror/gpseg0 sdw2|20001|/mirror/gpseg3""",
            "expected": "config file lines 1 and 2 conflict: Cannot recover the same failed segment sdw1 and data "
                        "directory /mirror/gpseg0 twice"
        },
        {
            "name": "old_to_new_and_new_to_new",
            "config": """sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5
                         sdw3|20001|/mirror/gpseg5 sdw3|20001|/mirror/gpseg5""",
            "expected": "config file lines 1 and 2 conflict: Cannot recover to the same segment sdw3 and data directory "
                        "/mirror/gpseg5 twice"
        },
        {
            "name": "old_to_new_and_old_to_new",
            "config": """sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5
                         sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5""",
            "expected": "config file lines 1 and 2 conflict: Cannot recover the same failed segment sdw1 and data "
                        "directory /mirror/gpseg0 twice"
        },
        {
            "name": "old_to_new_and_new_inplace",
            "config": """sdw1|20000|/mirror/gpseg0 sdw3|20001|/mirror/gpseg5
                         sdw3|20001|/mirror/gpseg5""",
            "expected": "config file lines 1 and 2 conflict: Cannot recover segment sdw3 with data directory "
                        "/mirror/gpseg5 in place if it is used as a recovery segment"
        }
    ]

    def test_parsing_should_fail(self):
        for test in self.failing_tests:
            with self.subTest(msg=test["name"]):
                # make sure test does not pass trivially("" will pass assertRaisesRegex)
                self.assertTrue(test["expected"].strip() != "")

                with self.assertRaisesRegex(Exception, test["expected"]):
                    self.run_single_parser_test(test)

    @staticmethod
    def parse_config(config_str):
        rows = []
        lineno = 0

        for line in config_str.splitlines():
            lineno += 1
            groups = line.split()

            address, port, datadir = groups[0].split('|')
            row = {
                'failedAddress': address,
                'failedPort': port,
                'failedDataDirectory': datadir,
                'lineno': lineno
            }

            if len(groups) > 1:
                address, port, datadir = groups[1].split('|')
                row.update({
                    'newAddress': address,
                    'newPort': port,
                    'newDataDirectory': datadir
                })

            rows.append(row)

        return rows
