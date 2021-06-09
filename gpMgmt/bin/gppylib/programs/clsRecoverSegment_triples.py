import abc
from typing import List

from gppylib.commands import unix
from gppylib.mainUtils import ExceptionNoStackTraceNeeded
from gppylib.operations.detect_unreachable_hosts import get_unreachable_segment_hosts
from gppylib.parseutils import line_reader, check_values, canonicalize_address
from gppylib.utils import checkNotNone, normalizeAndValidateInputPath
from gppylib.gparray import GpArray, Segment


class RecoverTriplet:
    """
    Represents the segments needed to perform a recovery on a given segment.
    failed   = acting mirror that needs to be recovered
    live     = acting primary to use to recover that failed segment
    failover = failed segment will be recovered here
    """
    def __init__(self, failed: Segment, live: Segment, failover: Segment):
        self.failed = failed
        self.live = live
        self.failover = failover
        self.validate(failed, live, failover)

    def __repr__(self):
        return "Failed: {0} Live: {1} Failover: {2}".format(self.failed, self.live, self.failover)

    def __eq__(self, other):
        return self.failed == other.failed and self.live == other.live and self.failover == other.failover

    @staticmethod
    def validate(failed, live, failover):

        msg = "liveSegment" if not failed else "No peer found for dbid {}. liveSegment".format(failed.getSegmentDbId())
        checkNotNone(msg, live)

        if failed is None and failover is None:
            raise Exception("No mirror passed to recovery triplet")

        if not live.isSegmentQE():
            raise ExceptionNoStackTraceNeeded("Segment to recover from for content %s is not a correct segment "
                                              "(it is a coordinator or standby coordinator)" % live.getSegmentContentId())
        if not live.isSegmentPrimary(True):
            raise ExceptionNoStackTraceNeeded(
                "Segment to recover from for content %s is not a primary" % live.getSegmentContentId())
        if not live.isSegmentUp():
            raise ExceptionNoStackTraceNeeded(
                "Primary segment is not up for content %s" % live.getSegmentContentId())
        if live.unreachable:
            raise ExceptionNoStackTraceNeeded(
                "The recovery source segment %s (content %s) is unreachable." % (live.getSegmentHostName(),
                                                                                 live.getSegmentContentId()))

        if failed is not None:
            if failed.getSegmentContentId() != live.getSegmentContentId():
                raise ExceptionNoStackTraceNeeded(
                    "The primary is not of the same content as the failed mirror.  Primary content %d, "
                    "mirror content %d" % (live.getSegmentContentId(), failed.getSegmentContentId()))
            if failed.getSegmentDbId() == live.getSegmentDbId():
                raise ExceptionNoStackTraceNeeded("For content %d, the dbid values are the same.  "
                                                  "A segment may not be recovered from itself" %
                                                  live.getSegmentDbId())

        if failover is not None:
            if failover.getSegmentContentId() != live.getSegmentContentId():
                raise ExceptionNoStackTraceNeeded(
                    "The primary is not of the same content as the mirror.  Primary content %d, "
                    "mirror content %d" % (live.getSegmentContentId(), failover.getSegmentContentId()))
            if failover.getSegmentDbId() == live.getSegmentDbId():
                raise ExceptionNoStackTraceNeeded("For content %d, the dbid values are the same.  "
                                                  "A segment may not be built from itself"
                                                  % live.getSegmentDbId())
            if failover.unreachable:
                raise ExceptionNoStackTraceNeeded(
                    "The recovery target segment %s (content %s) is unreachable." % (failover.getSegmentHostName(),
                                                                                     failover.getSegmentContentId()))

        if failed is not None and failover is not None:
            # for now, we require the code to have produced this -- even when moving the segment to another
            #  location, we preserve the directory
            assert failed.getSegmentDbId() == failover.getSegmentDbId()


class MirrorBuilderFactory:
    """
    parser
    ~~validate datadir is not relative normalizeAndValidateInputPath for both old and new~~
    ~~remove Exception('found twice in configuration')~~
    ~~add test for non integer port and remove from getMirrorTriples~~

    -p or not -i (inplace)
    read from gparray all down segments
    create equivalent of parsed rows
    -i code for parsed rows

    -i
    parse rows from config file
    for each parsed row:
        we get the list of failed, failover and live segment
        find the failed segment in gparray
        if not inplace:
            host should be reachable
            copy failover segment from the failed segment (this is weird since it mutates gparray)
        add to recovery triplet: failed, peerforfailed, failover


    3 cases:
       -i        (gprecoverseg -i file.txt) has failover_address or not(in place)
       -p        (gprecoverseg -p new1)     has failover_address which is new
       ! -i ! -p (gprecoverseg)             has no failover_address(in place

BOTH:  self.__options.outputSampleConfigFile  gparray is mutated for all triples, even if unreachable
    """
    @staticmethod
    def instance(gpArray, config_file=None, new_hosts=[], logger=None):
        """

        :param gpArray: The variable gpArray may get mutated when the getMirrorTriples function is called on this instance.
        :param config_file:
        :param new_hosts:
        :param logger:
        :return:
        """
        if config_file:
            return ConfigFileMirrorBuilder(gpArray, config_file)
        if not new_hosts:
            return GpArrayNoNewHostsMirrorBuilder(gpArray, logger)

        return GpArrayMirrorBuilder(gpArray, new_hosts, logger)


class MirrorBuilder(abc.ABC):
    def __init__(self, gpArray):
        """

        :param gpArray: Needs to be a shallow copy since we may return a mutated gpArray
        """
        self.gpArray = gpArray
        self.recoveryTriples = []
        self.interfaceHostnameWarnings = []

    def getInterfaceHostnameWarnings(self):
        return self.interfaceHostnameWarnings

    def _common_code(self, requests):
        dbIdToPeerMap = self.gpArray.getDbIdToPeerMap()
        for req in requests:
            # TODO: These 2 cases have different behavior which might be confusing to the user.
            # "failed_address>|<port>|<data_dir> <recovery_address>|<port>|<data_dir>" does full recovery
            # "failed_address>|<port>|<data_dir>" does incremental recovery
            failoverSegment = None
            if req.failover_host:
                """
                When the second set was passed, the caller is going to tell us to where we need to failover, so
                  build a failover segment
                """
                # these two lines make it so that failoverSegment points to the object that is registered in gparray
                failoverSegment = req.failed
                req.failed = failoverSegment.copy()

                # now update values in failover segment
                failoverSegment.setSegmentAddress(req.failover_host)
                failoverSegment.setSegmentHostName(req.failover_host)
                failoverSegment.setSegmentPort(int(req.failover_port))
                failoverSegment.setSegmentDataDirectory(req.failover_datadir)
                failoverSegment.unreachable = False if req.is_new_host else failoverSegment.unreachable

            # this must come AFTER the if check above because failedSegment can be adjusted to
            #   point to a different object
            if req.failed.unreachable:
                continue
            peer = dbIdToPeerMap.get(req.failed.getSegmentDbId())
            self.recoveryTriples.append(RecoverTriplet(req.failed, peer, failoverSegment))

        return self.recoveryTriples

    @abc.abstractmethod
    def getMirrorTriples(self) -> List[RecoverTriplet]:
        """

        :return: Returns a list of tuples that describes the following
        Failed Segment:
        Live Segment
        Failover Segment
        This function ignores the status (i.e. u or d) of the segment because this function is used by gpaddmirrors,
        gpmovemirrors and gprecoverseg. For gpaddmirrors and gpmovemirrors, the segment to be moved should not
        be marked as down whereas for gprecoverseg the segment to be recovered needs to marked as down.
        """
        pass


class GpArrayNoNewHostsMirrorBuilder(MirrorBuilder):
    def __init__(self, gpArray, logger):
        super().__init__(gpArray)
        self.logger = logger

    def getMirrorTriples(self):
        segments = self.gpArray.getSegDbList()
        #TODO only get failed mirrors ?
        failedSegments = [seg for seg in segments if seg.isSegmentDown()]

        requests = []
        for failedSeg in failedSegments:
            req = RecoveryRequest(failedSeg)
            requests.append(req)
        return self._common_code(requests)


class GpArrayMirrorBuilder(MirrorBuilder):
    def __init__(self, gpArray, newHosts, logger):
        super().__init__(gpArray)
        self.newHosts = []
        if newHosts:
            self.newHosts = newHosts[:]
        self.logger = logger

    #TODO add code to check if more hosts than needed are passed and skip new hosts that are unreachable
    #TODO either use failedSeg or failed
    def getMirrorTriples(self):

        failedSegments = [seg for seg in self.gpArray.getSegDbList() if seg.isSegmentDown()]
        failedSegmentsByHost = GpArray.getSegmentsByHostName(failedSegments)
        recoverHostMap = {k:v for k,v in zip(list(failedSegmentsByHost.keys()), self.newHosts)}

        """
        map the failed segments hosts to new recovery hosts
        key: failedSegHost
        value: newSegHost
        iterate over the failedseghosts
            get recoveryhost
            iterate over each seg on the host
        """
        portAssigner = _PortAssigner(self.gpArray)

        if len(self.newHosts) != len(failedSegmentsByHost):

            if len(self.newHosts) < len(failedSegmentsByHost):
                raise Exception('Not enough new recovery hosts given for recovery.')
            if len(self.newHosts) > len(failedSegmentsByHost):
                self.interfaceHostnameWarnings.append("The following recovery hosts were not needed:")
                for h in self.newHosts[len(failedSegmentsByHost):]:
                    self.interfaceHostnameWarnings.append("\t%s" % h)

        unreachable_hosts = get_unreachable_segment_hosts(self.newHosts[:len(failedSegments)], len(failedSegments))
        if unreachable_hosts:
            raise ExceptionNoStackTraceNeeded("Cannot recover. The following recovery target hosts are "
                                              "unreachable: %s" % unreachable_hosts)
        requests = []

        for failedHost, failoverHost in recoverHostMap.items():
            for failedSeg in failedSegmentsByHost[failedHost]:
                failoverPort = portAssigner.findAndReservePort(failoverHost, failoverHost)
                req = RecoveryRequest(failedSeg, failoverHost, failoverPort, failedSeg.getSegmentDataDirectory(), True)#TODO
                requests.append(req)

        return self._common_code(requests)

class RecoveryRequest:
    def __init__(self, failed, failover_host=None, failover_port=None, failover_datadir=None, is_new_host=False):
        # self.failed_host = failed_host
        # self.failed_port = failed_port
        # self.failed_datadir = failed_datadir
        self.failed = failed

        self.failover_host = failover_host
        self.failover_port = failover_port
        self.failover_datadir = failover_datadir
        self.is_new_host = is_new_host

"""

# -i case new host unreachable --> skip
# -p case new hsot unreachable andused --> Exception

if new_hosts:
   requests = assign_failover_info(requests, new_hosts)  // assign host, address, ports, datadir)  (-p case)

triples = []
for request in requests:
    failed = lookup(lhs)
    
    failover = calculate(failed)
    live = lookup(failed)
    
    new_hosts = new_hosts(requests)
    unreachable_new_hosts = get_unreachable_hosts(new_hosts)
    
    if not failover:
       # in place
       if failed.reachable:
           triples.append((failed, live, failover)
    else:
       if isNew(failover.host):
            if failover.reachable:
                triples.append((failed, live, failover)
            else:
                # Exception or skip?
       else:
            if failover.reachable:
                triples.append((failed, live, failover))         
    
return triples

!-p and !-i
don't care about new_hosts
get all the failed segs
failover is none
populate req

"""
class ConfigFileMirrorBuilder(MirrorBuilder):
    def __init__(self, gpArray, config_file):
        super().__init__(gpArray)
        self.config_file = config_file
        self.rows = self._parseConfigFile(self.config_file)

    def getMirrorTriples(self):
        requests = []
        for row in self.rows:
            # find the failed segment
            failedSegment = None
            for segment in self.gpArray.getDbList():
                if (segment.getSegmentAddress() == row['failedAddress']
                        and str(segment.getSegmentPort()) == row['failedPort']
                        and segment.getSegmentDataDirectory() == row['failedDataDirectory']):
                    failedSegment = segment
                    break

            if failedSegment is None:
                #FIXME: we deleted lineno from the error message. adding this to make sure it doesn't break anything
                raise Exception("A segment to recover was not found in configuration.  " \
                                "This segment is described by address|port|directory '%s|%s|%s'" %
                                (row['failedAddress'], row['failedPort'], row['failedDataDirectory']))
            req = RecoveryRequest(failedSegment, row.get('newAddress'), row.get('newPort'), row.get('newDataDirectory'))
            requests.append(req)
        return self._common_code(requests)


    @staticmethod
    def _parseConfigFile(config_file):
        """
        Parse the config file
        :param config_file:
        :return: List of dictionaries with each dictionary containing the failed and failover information??
        """
        rows = []
        with open(config_file) as f:
            for lineno, line in line_reader(f):

                groups = line.split()  # NOT line.split(' ') due to MPP-15675
                if len(groups) not in [1, 2]:
                    msg = "line %d of file %s: expected 1 or 2 groups but found %d" % (lineno, config_file, len(groups))
                    raise ExceptionNoStackTraceNeeded(msg)
                parts = groups[0].split('|')
                if len(parts) != 3:
                    msg = "line %d of file %s: expected 3 parts on failed segment group, obtained %d" % (
                        lineno, config_file, len(parts))
                    raise ExceptionNoStackTraceNeeded(msg)
                address, port, datadir = parts
                check_values(lineno, address=address, port=port, datadir=datadir)
                datadir = normalizeAndValidateInputPath(datadir, f.name, lineno)

                row = {
                    'failedAddress': address,
                    'failedPort': port,
                    'failedDataDirectory': datadir,
                    'lineno': lineno
                }
                if len(groups) == 2:
                    parts2 = groups[1].split('|')
                    if len(parts2) != 3:
                        msg = "line %d of file %s: expected 3 parts on new segment group, obtained %d" % (
                            lineno, config_file, len(parts2))
                        raise ExceptionNoStackTraceNeeded(msg)
                    address2, port2, datadir2 = parts2
                    check_values(lineno, address=address2, port=port2, datadir=datadir2)
                    datadir2 = normalizeAndValidateInputPath(datadir2, f.name, lineno)

                    row.update({
                        'newAddress': address2,
                        'newPort': port2,
                        'newDataDirectory': datadir2
                    })

                rows.append(row)

        ConfigFileMirrorBuilder._validate(rows)

        return rows

    @staticmethod
    #TODO rename validate
    def _validate(rows):
        """
        Runs checks for making sure all the rows are consistent
        :param rows:
        :return:
        """
        failed = {}
        new = {}
        for row in rows:
            address, port, datadir, lineno = \
                row['failedAddress'], row['failedPort'], row['failedDataDirectory'], row['lineno']

            if address+datadir in failed:
                msg = 'config file lines {0} and {1} conflict: ' \
                      'Cannot recover the same failed segment {2} and data directory {3} twice.' \
                    .format(failed[address+datadir], lineno, address, datadir)
                raise ExceptionNoStackTraceNeeded(msg)

            failed[address+datadir] = lineno

            if 'newAddress' not in row:
                if address+datadir in new:
                    msg = 'config file lines {0} and {1} conflict: ' \
                          'Cannot recover segment {2} with data directory {3} in place if it is used as a recovery segment.' \
                        .format(new[address+datadir], lineno, address, datadir)
                    raise ExceptionNoStackTraceNeeded(msg)

                continue

            address2, port2, datadir2 = row['newAddress'], row['newPort'], row['newDataDirectory']

            if address2+datadir2 in new:
                msg = 'config file lines {0} and {1} conflict: ' \
                      'Cannot recover to the same segment {2} and data directory {3} twice.' \
                    .format(new[address2+datadir2], lineno, address2, datadir2)
                raise ExceptionNoStackTraceNeeded(msg)

            new[address2+datadir2] = lineno


#FIXME DO we still need this ?
def _findAndValidatePeersForFailedSegments(gpArray, failedSegments):
    dbIdToPeerMap = gpArray.getDbIdToPeerMap()
    peersForFailedSegments = [dbIdToPeerMap.get(seg.getSegmentDbId()) for seg in failedSegments]

    for i in range(len(failedSegments)):
        peer = peersForFailedSegments[i]
        if peer is None:
            raise Exception("No peer found for dbid %s" % failedSegments[i].getSegmentDbId())
        elif peer.isSegmentDown():
            raise Exception(
                "Both segments for content %s are down; Try restarting Greenplum DB and running the program again." %
                (peer.getSegmentContentId()))
    return peersForFailedSegments

class _PortAssigner:
    """
    Used to assign new ports to segments on a host

    Note that this could be improved so that we re-use ports for segments that are being recovered but this
      does not seem necessary.

    """

    MAX_PORT_EXCLUSIVE = 65536

    def __init__(self, gpArray):
        #
        # determine port information for recovering to a new host --
        #   we need to know the ports that are in use and the valid range of ports
        #
        segments = gpArray.getDbList()
        ports = [seg.getSegmentPort() for seg in segments if seg.isSegmentQE()]
        if len(ports) > 0:
            self.__minPort = min(ports)
        else:
            raise Exception("No segment ports found in array.")
        self.__usedPortsByHostName = {}

        byHost = GpArray.getSegmentsByHostName(segments)
        for hostName, segments in byHost.items():
            usedPorts = self.__usedPortsByHostName[hostName] = {}
            for seg in segments:
                usedPorts[seg.getSegmentPort()] = True

    def findAndReservePort(self, hostName, address):
        """
        Find a port not used by any postmaster process.
        When found, add an entry:  usedPorts[port] = True   and return the port found
        Otherwise raise an exception labeled with the given address
        """
        if hostName not in self.__usedPortsByHostName:
            self.__usedPortsByHostName[hostName] = {}
        usedPorts = self.__usedPortsByHostName[hostName]

        minPort = self.__minPort
        for port in range(minPort, _PortAssigner.MAX_PORT_EXCLUSIVE):
            if port not in usedPorts:
                usedPorts[port] = True
                return port
        raise Exception("Unable to assign port on %s" % address)
