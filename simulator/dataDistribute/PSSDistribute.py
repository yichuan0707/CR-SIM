from random import randint, choice, sample
from time import strftime, time

from simulator.Configuration import Configuration
from simulator.XMLParser import XMLParser
from simulator.Log import error_logger
from simulator.dataDistribute.base import DataDistribute


class PSSDistribute(DataDistribute):
    """
    PSS: Partitioned placement Strategy System.
    All stripes of one file spread across n fixed disks. All disks organized as subsets
    which have n disks.
    """
    def divideDisksIntoGroups(self, disks):
        full_disk_count = 0
        groups = []

        while len(disks) >= self.n:
            group = []

            # if len(disks) < self.n:
            #     break

            # why execution blocked here sometimes?
            chosen_racks = sample(disks, self.n)
            for rack_disks in chosen_racks:
                disk_index_in_rack = randint(0, len(rack_disks)-1)
                group.append(rack_disks.pop(disk_index_in_rack))
                if len(rack_disks) == 0:
                    disks.remove(rack_disks)
            groups.append(group)

        self.groups = groups
        return groups

    def distributeSlices(self, root, increase_slices):
        disks = []

        self.getAllDisks(root, disks)
        self.total_slices += increase_slices
        groups = self.divideDisksIntoGroups(disks)

        full_disk_count = 0
        for i in xrange(self.total_slices - increase_slices, self.total_slices):
            group = choice(groups)
            self.slice_locations.append(group)
            for disk in group:
                if len(disk.getChildren()) > self.conf.max_chunks_per_disk:
                    full_disk_count += self.n
                    groups.remove(group)
                    error_logger.error("A Partition is completely full, full disk count is " + str(full_disk_count))
                    break
                disk.addChild(i)

            self._my_assert(len(self.slice_locations[i]) == self.n)

        self._my_assert(len(self.slice_locations) == self.total_slices)

    def printGroupsToFile(self, file_path=r"/root/CR-SIM/log/groups"):
        ts = strftime("%Y%m%d.%H.%M.%S")
        file_path += '-' + ts
        with open(file_path, 'w') as fp:
            for i, item in enumerate(self.groups):
                info = "group " + str(i) + ": "
                for d in item:
                    info += d.toString() + ", "
                info += "\n"
                fp.write(info)


class HierPSSDistribute(PSSDistribute):
    """
    Hierarchical + PSS dataplacement
    """
    def __init__(self, xml):
        super(HierPSSDistribute, self).__init__(xml)
        self.r = self.conf.distinct_racks
        slice_chunks_on_each_rack = self.n/self.r
        if slice_chunks_on_each_rack == 0:
            raise Exception("Distinct_racks is too large")
        self.slices_chunks_on_racks = [slice_chunks_on_each_rack] * self.r
        if self.n % self.r:
            for i in xrange(self.n % self.r):
                self.slices_chunks_on_racks[i] += 1

    def divideDisksIntoGroups(self, disks):
        retry_count = 0
        groups = []

        while True:
            group = []
            if len(disks) < self.r:
                break

            chosen_racks = sample(disks, self.r)
            for i, rack_disks in enumerate(chosen_racks):
                if len(rack_disks) < self.slices_chunks_on_racks[i]:
                    retry_count += 1
                    break
                if retry_count > 100:
                    error_logger.error("Divide disks into groups failed")
                    raise Exception("Divide disks into groups failed")

                diskes = sample(rack_disks, self.slices_chunks_on_racks[i])
                for disk in diskes:
                    group.append(disk)
                    rack_disks.remove(disk)
                if len(rack_disks) == 0:
                    disks.remove(rack_disks)
            if len(group) == self.n:
                groups.append(group)

        self.groups = groups
        return groups



if __name__ == "__main__":
    conf = Configuration()
    xml = XMLParser(conf)
    distribute = PSSDistribute(xml)

    disks = []
    distribute.getAllDisks(distribute.getRoot(), disks)
    groups = distribute.divideDisksIntoGroups(disks)
    for i, group in enumerate(groups):
        format_output = "group " + str(i) + ": "
        for disk in group:
            format_output += "  " + disk.toString()
        # print format_output


    total_slices = 419431
    distribute.distributeSlices(distribute.getRoot(), total_slices)
    for i in xrange(total_slices):
        format_output = "slice index " + str(i) + ": "
        for disk in distribute.slice_locations[i]:
            format_output += "  " + disk.toString()
        print format_output
