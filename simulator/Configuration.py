import ConfigParser
import os
from math import ceil, floor
from random import random
from simulator.utils import splitMethod, splitIntMethod, splitFloatMethod, extractDRS
from simulator.Log import info_logger
from simulator.drs.Handler import getDRSHandler

BASE_PATH = r"/root/CR-SIM/"
CONF_PATH = BASE_PATH + "conf/"


def getConfParser(conf_file_path):
    conf = ConfigParser.ConfigParser()
    conf.read(conf_file_path)
    return conf


class Configuration(object):
    path = CONF_PATH + "cr-sim.conf"

    def __init__(self, path=None):
        if path is None:
            conf_path = Configuration.path
        else:
            conf_path = path
        self.conf = getConfParser(conf_path)

        try:
            d = self.conf.defaults()
        except ConfigParser.NoSectionError:
            raise Exception("No Default Section!")

        self.total_time = int(d["total_time"])
        # total active storage in PBs
        self.total_active_storage = float(d["total_active_storage"])

        self.chunk_size = int(d["chunk_size"])
        self.disk_capacity = float(d["disk_capacity"])
        # tanslate TB of manufacturrer(10^12 bytes) into GBs(2^30 bytes)
        self.actual_disk_capacity = self.disk_capacity * pow(10, 12)/ pow(2, 30)
        self.max_chunks_per_disk = int(floor(self.actual_disk_capacity*1024/self.chunk_size))

        self.disks_per_machine = int(d["disks_per_machine"])
        self.machines_per_rack = int(d["machines_per_rack"])
        self.rack_count = int(d["rack_count"])

        self.datacenters = int(d.pop("datacenters", 1))

        self.event_file = d.pop("event_file", None)

        # If n <= 15 in each stripe, no two chunks are on the same rack.
        self.num_chunks_diff_racks = 15

        self.data_placement = d["data_placement"]
        if self.data_placement.lower() == "copyset":
            self.scatter_width = int(d["scatter_width"])

        self.data_redundancy = d.pop("data_redundancy")
        data_redundancy = extractDRS(self.data_redundancy)

        # True means auto repair; False means manual repair
        self.auto_repair = self._bool(d.pop("auto_repair", "true"))
        self.overall_calculation = self._bool(d.pop("overall_calculation", "true"))

        self.lazy_recovery = self._bool(d.pop("lazy_recovery", "false"))
        self.lazy_only_available = self._bool(d.pop("lazy_only_available", "true"))
        self.recovery_bandwidth_cross_rack = int(d["recovery_bandwidth_cross_rack"])
        self.queue_disable = self._bool(d.pop("queue_disable", "true"))
        self.bandwidth_contention = d["bandwidth_contention"]
        self.node_bandwidth = int(d["node_bandwidth"])

        self.hierarchical = self._bool(d.pop("hierarchical", "false"))
        if self.hierarchical:
            self.distinct_racks = int(d.pop("distinct_racks"))
            self.recovery_bandwidth_intra_rack = int(d["recovery_bandwidth_intra_rack"])

        self.parallel_repair = self._bool(d.pop("parallel_repair", "false"))

        self.availability_counts_for_recovery = self._bool(d[
            "availability_counts_for_recovery"])

        self.availability_to_durability_threshold = splitIntMethod(d["availability_to_durability_threshold"])
        self.recovery_probability = splitIntMethod(d["recovery_probability"])
        self.max_degraded_slices = float(d["max_degraded_slices"])
        self.installment_size = int(d["installment_size"])

        self.outputs = splitMethod(d["outputs"])

        detect_intervals = d.pop("detect_intervals", None)
        if detect_intervals is None:
            self.rafi_recovery = False
            self.detect_intervals = detect_intervals
        else:
            self.rafi_recovery = True
            self.detect_intervals = splitFloatMethod(detect_intervals)

        self.drs_handler = getDRSHandler(data_redundancy[0], data_redundancy[1:])
        if not self.lazy_recovery:
            self.recovery_threshold = self.drs_handler.n - 1
        else:
            self.recovery_threshold = int(d.pop("recovery_threshold"))

        # total slices calculate like this only without heterogeneous redundancy
        self.total_slices = int(ceil(self.total_active_storage*pow(2,30)/(self.drs_handler.k*self.chunk_size)))

        self.chunk_repair_time, self.disk_repair_time, self.node_repair_time = self._repairTime()

    def _bool(self, string):
        if string.lower() == "true":
            return True
        elif string.lower() == "false":
            return False
        else:
            raise Exception("String must be 'true' or 'false'!")

    def _checkSpace(self):
        total_system_capacity = self.actual_disk_capacity * self.disks_per_machine * self.machines_per_rack * self.rack_count
        min_necess_capacity = float(self.total_active_storage) * self.drs_handler.n / self.drs_handler.k
        if min_necess_capacity >= total_system_capacity:
            raise Exception("Not have enough space!")

    def _getSections(self, section_start):
        sections = []
        all_sections = self.conf.sections()
        for item in all_sections:
            if item.startswith(section_start):
                sections.append(item)
        return sections

    def _getRepairTraffic(self):
        # repair traffic in chunks
        if not self.hierarchical:
            repair_traffic = self.drs_handler.repairTraffic()
        else:
            repair_traffic = self.drs_handler.repairTraffic(self.hierarchical, self.distinct_racks)
        return repair_traffic

    def _repairTime(self):
        if not self.hierarchical and self.data_placement == "sss":
            r = self.rack_count
        elif not self.hierarchical and self.data_placement == "pss":
            r = min(self.rack_count, self.drs_handler.k)
        elif not self.hierarchical and self.data_placement == "copyset":
            r = min(self.rack_count, self.scatter_width)
        elif self.hierarchical and self.data_placement == "sss":
            r = self.rack_count
        elif self.hierarchical and self.data_placement == "pss":
            r = min(self.rack_count, self.drs_handler.k, self.distinct_racks)
        elif self.hierarchical and self.data_placement == "copyset":
            r = min(self.rack_count, self.scatter_width, self.distinct_racks)
        else:
            raise Exception("Incorrect data placement")

        # in MB/s
        aggregate_bandwidth = self.recovery_bandwidth_cross_rack * r
        aggregate_bandwidth_for_single_block = self.recovery_bandwidth_cross_rack * min(r, self.drs_handler.k)

        repair_traffic = self._getRepairTraffic()
        # used disk space in MBs
        used_disk_space = self.drs_handler.SO*self.total_active_storage*pow(2,30)/(self.rack_count*self.machines_per_rack*self.disks_per_machine)

        # repair time in hours
        chunk_repair_time = round(repair_traffic*self.chunk_size/aggregate_bandwidth_for_single_block, 5)
        disk_repair_time = round(repair_traffic*used_disk_space/aggregate_bandwidth, 5)
        node_repair_time = disk_repair_time*self.disks_per_machine

        return chunk_repair_time, disk_repair_time, node_repair_time

    def getDCCount(self):
        return self.datacenters

    def getRackCount(self):
        return self.rack_count

    def getMachinesPerRack(self):
        return self.machines_per_rack

    def getDisksPerMachine(self):
        return self.disks_per_machine

    # "True" means events record to file, and vice versa.
    def eventToFile(self):
        return self.event_file is not None

    def DRSHandler(self):
        return self.drs_handler

    def getAvailableLazyThreshold(self, time_since_failed):
        threshold_gap = self.drs_handler.n - 1 - self.recovery_threshold
        length = len(self.availability_to_durability_threshold)
        index = 0
        for i in xrange(length-1):
            if self.availability_to_durability_threshold[i] < \
               time_since_failed and \
               self.availability_to_durability_threshold[i+1] >= \
               time_since_failed:
                if i > 0:
                    index = i
                break
        threshold_increment = threshold_gap * \
            (1 if random() < self.recovery_probability[i] else 0)
        return self.recovery_threshold + threshold_increment

    def returnSliceSize(self):
        return self.chunk_size * self.drs_handler.n

    def returnAll(self):
        d = {"total_time": self.total_time,
             "total_active_storage": self.total_active_storage,
             "chunk_size": self.chunk_size,
             "disk_capacity": self.disk_capacity,
             "disks_per_machine": self.disks_per_machine,
             "machines_per_rack": self.machines_per_rack,
             "datacenters": self.datacenters,
             "event_file": self.event_file,
             "recovery_threshold": self.recovery_threshold,
             "lazy_only_available": self.lazy_only_available,
             "data_redundancy": self.data_redundancy,
             "outputs": self.outputs,
             "recovery_bandwidth_cross_rack": self.recovery_bandwidth_cross_rack,
             "installment_size": self.installment_size,
             "availability_counts_for_recovery":
             self.availability_counts_for_recovery,
             "parallel_repair": self.parallel_repair,
             "lazy_recovery": self.lazy_recovery,
             "rafi_recovery": self.rafi_recovery}

        if self.rafi_recovery:
            d["detect_intervals"] = self.detect_intervals

        return d

    def printTest(self):
        d = self.returnAll()
        keys = d.keys()
        for key in keys:
            print key, d[key]

    def printAll(self):
        default_infos = "Default Configurations: \t total_time: " + str(self.total_time) + \
                        ", disk capacity: " + str(self.disk_capacity) + "TB" + \
                        ", disks per machine: " + str(self.disks_per_machine) + \
                        ", machines per rack: " + str(self.machines_per_rack) + \
                        ", rack count: " + str(self.rack_count) + \
                        ", chunk size: " + str(self.chunk_size) + "MB" + \
                        ", total active storage: " + str(self.total_active_storage) + "PB" +\
                        ", data redundancy: " + self.data_redundancy + \
                        ", data placement: " + self.data_placement + \
                        ", recovery bandwidth cross rack: " + str(self.recovery_bandwidth_cross_rack) + \
                        ", installment size: " + str(self.installment_size) + \
                        ", event file path: " + self.event_file + \
                        ", outputs: " + str(self.outputs) + \
                        ", auto repair: " + str(self.auto_repair) + \
                        ", hierarchical: " + str(self.hierarchical) + \
                        ", parallel repair: " + str(self.parallel_repair) + \
                        ", lazy recovery flag: " + str(self.lazy_recovery) + \
                        ", lazy only available: " + str(self.lazy_only_available) + \
                        ", recovery threshold: " + str(self.recovery_threshold) + \
                        ", rafi recovery flag: " + str(self.rafi_recovery)

        if self.data_placement == "copyset":
            default_infos += ", scatter width: " + str(self.scatter_width)
        if self.hierarchical:
            default_infos += ", distinct racks: " + str(self.distinct_racks)

        info_logger.info(default_infos)

        recovery_infos = ""
        if self.rafi_recovery:
            recovery_infos += " detect intervals: " + str(self.detect_intervals)
        if self.lazy_recovery or self.rafi_recovery:
            info_logger.info(recovery_infos)


if __name__ == "__main__":
    conf = Configuration("/root/CR-SIM/conf/samples/pss_hier_lazy_rafi/RS.conf")
    drs_handler = conf.DRSHandler()
    print conf.total_slices
    print conf.rack_count
    conf.printTest()
    conf.printAll()
    print conf.detect_intervals
    print conf.chunk_repair_time, conf.disk_repair_time, conf.node_repair_time
