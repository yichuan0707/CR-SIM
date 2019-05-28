import ConfigParser
import os
from math import ceil
from random import random
from simulator.utils import splitMethod, splitIntMethod, extractDRS, extractHeteDRSFromDiffMedium
from simulator.Log import info_logger
from simulator.drs.Handler import getDRSHandler

BASE_PATH = r"/root/CR-SIM/"
CONF_PATH = BASE_PATH + "conf/"


def getConfParser(conf_file_path):
    conf = ConfigParser.ConfigParser()
    conf.read(conf_file_path)
    return conf


class Configuration(object):

    def __init__(self, conf_path=CONF_PATH+"base-cr-sim.conf"):
        self.conf = getConfParser(conf_path)

        try:
            d = self.conf.defaults()
        except ConfigParser.NoSectionError:
            raise Exception("No Default Section!")

        self.total_time = int(d["total_time"])
        # total active storage in PBs
        self.total_active_storage = float(d["total_active_storage"])

        self.chunk_size = int(d["chunk_size"])
        self.chunks_per_disk = int(d["chunks_per_disk"])
        self.disks_per_machine = int(d["disks_per_machine"])
        self.machines_per_rack = int(d["machines_per_rack"])

        self.datacenters = int(d.pop("datacenters", 1))

        self.event_file = d.pop("event_file", None)

        # If n <= 15 in each stripe, no two chunks are on the same rack.
        self.num_chunks_diff_racks = 15

        self.data_placement = d["data_placement"]
        data_redundancy = d.pop("data_redundancy", None)
        if data_redundancy is not None:
            self.data_redundancy = extractDRS(data_redundancy)

        self.lazy_only_available = True
        self.recovery_bandwidth_gap = int(d["recovery_bandwidth_gap"])
        self.installment_size = int(d["installment_size"])
        self.availability_counts_for_recovery = self._bool(d[
            "availability_counts_for_recovery"])

        self.outputs = splitMethod(d["outputs"])

        self.lazy_recovery = self._bool(d.pop("lazy_recovery", "false"))
        if self.lazy_recovery:
            if not self.conf.has_section("Lazy Recovery"):
                raise Exception("Lack of Lazy Recovery Section in configuration file!")
            options = dict(self.conf.items("Lazy Recovery"))

            self.recovery_threshold = int(options["recovery_threshold"])
            self.availability_to_durability_threshold = splitIntMethod(options["availability_to_durability_threshold"])
            self.recovery_probability = splitIntMethod(options["recovery_probability"])
            self.max_degraded_slices = float(options["max_degraded_slices"])

        self.rafi_recovery = self._bool(d.pop("rafi_recovery", "false"))
        if self.rafi_recovery:
            if not self.conf.has_section("RAFI Recovery"):
                raise Exception("Lack of RAFI Recovery Section in configuration file")
            options = dict(self.conf.items("RAFI Recovery"))

            self.detect_interval1 = float(options["detect_interval1"])
            self.detect_interval2 = float(options["detect_interval2"])

        self.heterogeneous_redundancy = self._bool(d.pop(
            "heterogeneous_redundancy", "false"))
        self.heterogeneous_medium = self._bool(d.pop(
            "heterogeneous_medium", "false"))
        # can not define heterogeneous_medium and heterogeneous_redundancy at same time
        if self.heterogeneous_medium and self.heterogeneous_redundancy:
            pass

        if self.heterogeneous_redundancy:
            if not self.conf.has_section("Heterogeneous Redundancy"):
                raise Exception("Lack of Heterogeneous Redundancy Section in configuration file")
            options = dict(self.conf.items("Heterogeneous Redundancy"))

            self.data_redundancy = []
            self.data_redundancy.append(extractDRS(options.pop("first_redundancy")))
            self.data_redundancy.append(extractDRS(options.pop("second_redundancy")))
            self.hr_policy = options.pop("hr_policy")
            self.file_size_distribution = options.pop("file_size_distribution")
            self.hotness_prob = float(options.pop("hotness_prob"))

        if self.heterogeneous_medium:
            if not self.conf.has_section("Heterogeneous Medium"):
                raise Exception("Lack of Heterogeneous Medium Section in configuration file")
            options = dict(self.conf.items("Heterogeneous Medium"))

            self.mediums = options["mediums"]
            self.data_redundancy = extractHeteDRSFromDiffMedium(options["redundancies"])
            self.transform_policy = options["transform_policy"]

        self.drs_handler = self.DRSHandler()
        if not self.lazy_recovery:
            self.recovery_threshold = self.drs_handler.n - 1

        self.system_scaling = self._bool(d.pop("system_scaling", "false"))
        if self.system_scaling:
            sections = self._getSections("System Scaling")
            if sections == []:
                raise Exception("No System Scaling section in configuration file")
            self.system_scaling_infos = []
            for section in sections:
                self.system_scaling_infos.append(self.parserScalingSettings(section))

        self.system_upgrade = self._bool(d.pop("system_upgrade", "false"))
        if self.system_upgrade:
            sections = self._getSections("System Upgrade")
            if sections == []:
                raise Exception("No System Upgrade section in configuration file")
            self.system_upgrade_infos = []
            for section in sections:
                self.system_upgrade_infos.append(self.parserUpgradeSettings(section))

        self.correlated_failures = self._bool(d.pop("correlated_failures", "false"))
        if self.correlated_failures:
            sections = self._getSections("Correlated Failures")
            if sections == []:
                raise Exception("No Correlated Failures section in configuration file")
            self.correlated_failures_infos = []
            for section in sections:
                self.correlated_failures_infos.append(self.parserCorrelatedSetting(section))

        self.block_failure = self._bool(d.pop("block_failure", "false"))
        if self.block_failure:
            if not self.conf.has_section("Block Failure"):
                raise Exception("Lack of Block Failure Section in configuration file")
            self.block_failure_prob = self.conf.getfloat("Block Failure", "block_failure_prob")

        # total slices calculate like this only without heterogeneous redundancy
        self.total_slices = int(ceil(self.total_active_storage*1024*1024*1024/(self.drs_handler.k*self.chunk_size)))
        # calculation!!!
        self.rack_count = int(ceil((self.total_slices * self.drs_handler.n)/(self.chunks_per_disk*self.disks_per_machine*self.machines_per_rack*5.0/6.0)))


    def _bool(self, string):
        if string.lower() == "true":
            return True
        elif string.lower() == "false":
            return False
        else:
            raise Exception("String must be 'true' or 'false'!")

    def _getSections(self, section_start):
        sections = []
        all_sections = self.conf.sections()
        for item in all_sections:
            if item.startswith(section_start):
                sections.append(item)
        return sections

    # "True" means events record to file, and vice versa.
    def eventToFile(self):
        return self.event_file is not None

    def DRSHandler(self):
        if self.heterogeneous_redundancy and self.heterogeneous_medium:
            pass
        elif self.heterogeneous_redundancy and not self.heterogeneous_medium:
            pass
        elif not self.heterogeneous_redundancy and self.heterogeneous_medium:
            pass
        else:
            drs_handler = getDRSHandler(self.data_redundancy[0], self.data_redundancy[1:])

        return drs_handler

    def parserScalingSettings(self, section_name):
        scaling_start = self.conf.getint(section_name, "scaling_start")
        style = self.conf.getint(section_name, "style")
        inc_capacity = self.conf.getfloat(section_name, "inc_capacity")
        inc_slices = self.conf.getint(section_name, "inc_slices")
        add_slice_start = self.conf.getint(section_name, "add_slice_start")
        slice_rate = self.conf.getfloat(section_name, "slice_rate")
        load_balancing = self.conf.getboolean(section_name, "load_balancing")

        new_chunks_per_disk = None
        if style == 0:
            new_chunks_per_disk = self.conf.getint(section_name, "new_chunks_per_disk")
        try:
            disk_failure_generator = self.conf.get(section_name, "disk_failure_generator")
        except ConfigParser.NoOptionError:
            disk_failure_generator = None
        try:
            disk_recovery_generator = self.conf.get(section_name, "disk_recovery_generator")
        except ConfigParser.NoOptionError:
            disk_recovery_generator = None

        return [scaling_start, style, inc_capacity, inc_slices, add_slice_start, slice_rate, load_balancing,
                new_chunks_per_disk, disk_failure_generator, disk_recovery_generator]

    def parserUpgradeSettings(self, section_name):
        upgrade_start = self.conf.getint(section_name, "upgrade_start")
        upgrade_concurrence = self.conf.getint(section_name, "upgrade_concurrence")
        upgrade_interval = self.conf.getfloat(section_name, "upgrade_interval")
        downtime = self.conf.getfloat(section_name, "downtime")

        return (upgrade_start, upgrade_concurrence, upgrade_interval, downtime)

    def parserCorrelatedSetting(self, section_name):
        occurrence_timestamp = self.conf.getint(section_name, "occurrence_timestamp")
        una_scope = self.conf.get(section_name, "una_scope")
        una_downtime = self.conf.getfloat(section_name, "una_downtime")
        try:
            dl_scope = self.conf.get(section_name, "dl_scope")
            dl_downtime = self.conf.getfloat(section_name, "dl_downtime")
        except ConfigParser.NoOptionError:
            dl_scope = None
            dl_downtime = None
        try:
            choose_from_una = self.conf.getboolean(section_name, "choose_from_una")
        except ConfigParser.NoOptionError:
            choose_from_una = False

        return (occurrence_timestamp, una_scope, una_downtime, dl_scope, dl_downtime, choose_from_una)

    # Time table for total slices changes
    def tableForTotalSlice(self):
        time_table = []
        total_slices = self.total_slices
        if not self.system_scaling:
            time_table.append([0, self.total_time, total_slices, 0])
        else:
            start_time = 0
            end_time = 0
            for i, info in enumerate(self.system_scaling_infos):
                end_time = info[0] + info[4]
                time_table.append([start_time, end_time, total_slices, 0])
                start_time = end_time
                end_time += float(info[3])/info[5]
                time_table.append([start_time, end_time, total_slices, info[5]])
                total_slices += info[3]

            time_table.append([end_time, self.total_time, total_slices, 0])

        return time_table

    def getAvailableLazyThreshold(self, time_since_failed):
        threshold_gap = self.drs_handler.n - 1 - self.recovery_threshold
        length = len(self.availability_to_durability_threshold)
        index = 0
        for i in xrange(length):
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
             "chunks_per_disk": self.chunks_per_disk,
             "disks_per_machine": self.disks_per_machine,
             "machines_per_rack": self.machines_per_rack,
             "datacenters": self.datacenters,
             "event_file": self.event_file,
             "recovery_threshold": self.recovery_threshold,
             "data_redundancy": self.data_redundancy,
             "outputs": self.outputs,
             "recovery_bandwidth_gap": self.recovery_bandwidth_gap,
             "installment_size": self.installment_size,
             "availability_counts_for_recovery":
             self.availability_counts_for_recovery,
             "lazy_recovery": self.lazy_recovery,
             "rafi_recovery": self.rafi_recovery,
             "heterogeneous_redundancy": self.heterogeneous_redundancy,
             "heterogeneous_medium": self.heterogeneous_medium,
             "system_scaling": self.system_scaling,
             "system_upgrade": self.system_upgrade,
             "correlated_failures": self.correlated_failures,
             "block_failure": self.block_failure}

        if self.lazy_recovery:
            d["recovery_threshold"] = self.recovery_threshold
            d["max_degraded_slices"] = self.max_degraded_slices
            d["availability_to_durability_threshold"] \
                = self.availability_to_durability_threshold
            d["recovery_probability"] = self.recovery_probability
        if self.rafi_recovery:
            d["detect_interval1"] = self.detect_interval1
            d["detect_interval2"] = self.detect_interval2
        if self.heterogeneous_redundancy:
            d["hr_policy"] = self.hr_policy
            d["file_size_distribution"] = self.file_size_distribution
            d["hotness_prob"] = self.hotness_prob
        if self.heterogeneous_medium:
            d["heterogeneous_mediums"] = self.mediums
            d["transform_policy"] = self.transform_policy
        if self.block_failure:
            d["block_failure_prob"] = self.block_failure_prob
        if self.system_scaling:
            d["system_scaling_infos"] = self.system_scaling_infos
        if self.system_upgrade:
            d["system_upgrade_infos"] = self.system_upgrade_infos
        if self.correlated_failures:
            d["correlated_failures"] = self.correlated_failures_infos

        return d

    def printTest(self):
        d = self.returnAll()
        keys = d.keys()
        for key in keys:
            print key, d[key]

    def printAll(self):
        default_infos = "Default Configurations: \t total_time: " + str(self.total_time) + \
                        ", chunks per disk: " + str(self.chunks_per_disk) + \
                        ", disks per machine: " + str(self.disks_per_machine) + \
                        ", machines per rack: " + str(self.machines_per_rack) + \
                        ", rack count: " + str(self.rack_count) + \
                        ", chunk size: " + str(self.chunk_size) + "MB" + \
                        ", total active storage: " + str(self.total_active_storage) + \
                        ", data redundancy: " + str(self.data_redundancy) + \
                        ", data placement: " + self.data_placement + \
                        ", recovery bandwidth cap: " + str(self.recovery_bandwidth_gap) + \
                        ", installment size: " + str(self.installment_size) + \
                        ", event file path: " + self.event_file + \
                        ", outputs: " + str(self.outputs) + \
                        ", lazy recovery flag: " + str(self.lazy_recovery) + \
                        ", rafi recovery flag: " + str(self.rafi_recovery) + \
                        ", heterogeneous redundancy flag: " + str(self.heterogeneous_redundancy) + \
                        ", heterogeneous medium flag: " + str(self.heterogeneous_medium) + \
                        ", system Scaling flag: " + str(self.system_scaling) + \
                        ", system upgrade flag: " + str(self.system_upgrade) + \
                        ", correlated failures flag: " + str(self.correlated_failures)

        if self.block_failure:
            default_infos += ", block failure prob: " + str(self.block_failure_prob)

        info_logger.info(default_infos)

        recovery_infos = "(Recovery Configurations) "

        if self.lazy_recovery:
            recovery_infos += "recovery shreshold: " + str(self.recovery_threshold) + \
                              ", max degraded slices: " + str(self.max_degraded_slices)
            recovery_infos += "\t"
        if self.rafi_recovery:
            recovery_infos += " detect interval 1: " + str(self.detect_interval1) + \
                              ", detect interval 2: " + str(self.detect_interval2)
        if self.lazy_recovery or self.rafi_recovery:
            info_logger.info(recovery_infos)

        heterogeneous_infos = "(Heterogeneous Configurations) "
        if self.heterogeneous_redundancy:
            heterogeneous_infos += "heterogeneous data redundancy: " + str(self.data_redundancy) + \
                                   ", transform policy for hetero-redundancy: " + str(self.hr_policy)
            if self.hr_policy == "size":
                heterogeneous_infos += ", file size distribution: " + str(self.file_size_distribution)
            if self.hr_policy == "hotness":
                heterogeneous_infos += ", hotness probability: " + str(self.hotness_prob)
            heterogeneous_infos += "\t"
        if self.heterogeneous_medium:
            heterogeneous_infos += " heterogeneous storage mediums: " + str(self.mediums) + \
                                   ", heterogeneous data redundancy: " + str(self.data_redundancy) + \
                                   ", transform policy for hetero-mediums: " + str(self.transform_policy)
        if self.heterogeneous_redundancy or self.heterogeneous_medium:
            info_logger.info(heterogeneous_infos)

        if self.system_scaling:
            info_logger.info("System scaling Configurations: " + str(self.system_scaling_infos))

        if self.system_upgrade:
            info_logger.info("System upgrade Configurations: " + str(self.system_upgrade_infos))

        if self.correlated_failures:
            info_logger.info("Correlated Failures Configurations: " + str(self.correlated_failures_infos))



if __name__ == "__main__":
    conf = Configuration("/root/CR-SIM/conf/cr-sim.conf")
    drs_handler = conf.DRSHandler()
    print conf.tableForTotalSlice()
    # print conf.total_slices
    # print conf.rack_count
    # conf.printTest()
    # conf.printAll()
