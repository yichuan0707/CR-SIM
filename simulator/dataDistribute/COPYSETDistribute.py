from math import ceil, floor
from time import strftime
from random import randint, choice, sample

from simulator.Configuration import Configuration
from simulator.XMLParser import XMLParser
from simulator.Log import error_logger
from simulator.dataDistribute.base import DataDistribute


class COPYSETDistribute(DataDistribute):
    """
    CopySet(Cidon et al. 2013)
    We expand the CopySet idea to erasure codes, scatter width s.
    n-1 <= s <= total_nodes - 1, when s = n-1, CopySet becomes PSS; when s = total_nodes -1, CopySet becomes SSS.

    """
    def _permutation(self, available_racks, copy_sets_num):
        copy_sets = []
        rack_count = self.conf.rack_count
        disk_per_rack = self.conf.machines_per_rack * self.conf.disks_per_machine

        # remove the disks have been selected from counts, make sure each disk appears only once in each permutation
        counts = [[i for i in xrange(disk_per_rack)] for j in xrange(rack_count)]
        rack_indexes = [i for i in xrange(rack_count)]

        for i in xrange(copy_sets_num):
            copy_set = []

            if len(rack_indexes) < self.n:
                break

            chosen_rack_indexes = sample(rack_indexes, self.n)
            for rack_index in chosen_rack_indexes:
                rack = available_racks[rack_index]
                disk_index = choice(counts[rack_index])
                copy_set.append(rack[disk_index])
                counts[rack_index].remove(disk_index)
                if counts[rack_index] == []:
                    rack_indexes.remove(rack_index)
            copy_sets.append(copy_set)
        return copy_sets

    def divideDisksIntoSets(self, disks):
        full_disk_count = 0
        copy_sets = []
        s = self.conf.scatter_width
        N = self.returnTotalDiskCount()
        p = int(ceil(float(s)/(self.n-1)))
        copy_sets_for_each_permutation = int(floor(float(N)/self.n))

        # for each permutation
        for i in xrange(p):
            copy_sets += self._permutation(disks, copy_sets_for_each_permutation)
        self.groups = copy_sets
        return copy_sets

    def distributeSlices(self, root, increase_slices):
        disks = []

        self.getAllDisks(root, disks)
        self.total_slices += increase_slices
        copy_sets = self.divideDisksIntoSets(disks)

        full_disk_count = 0
        for i in xrange(self.total_slices - increase_slices, self.total_slices):
            copy_set = choice(copy_sets)
            self.slice_locations.append(copy_set)
            for disk in copy_set:
                if len(disk.getChildren()) > self.conf.max_chunks_per_disk:
                    full_disk_count += self.n
                    copy_sets.remove(copy_set)
                    error_logger.error("A CopySet is completely full, full disk count is " + str(full_disk_count))
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


class HierCOPYSETDistribute(COPYSETDistribute):
    """
    Except CopySet(Cidon et al. 2013), we also adopt hierarchical data placement.

    """
    def __init__(self, xml):
        super(HierCOPYSETDistribute, self).__init__(xml)
        self.r = self.conf.distinct_racks
        slice_chunks_on_each_rack = self.n/self.r
        if slice_chunks_on_each_rack == 0:
            raise Exception("Distinct_racks is too large")
        self.slices_chunks_on_racks = [slice_chunks_on_each_rack] * self.r
        if self.n % self.r:
            for i in xrange(self.n % self.r):
                self.slices_chunks_on_racks[i] += 1

    def _permutation(self, available_racks, copy_sets_num):
        copy_sets = []
        rack_count = self.conf.rack_count
        disk_per_rack = self.conf.machines_per_rack * self.conf.disks_per_machine

        # remove the disks have been selected from counts, make sure each disk appears only once in each permutation
        counts = [[i for i in xrange(disk_per_rack)] for j in xrange(rack_count)]
        rack_indexes = [i for i in xrange(rack_count)]

        for i in xrange(copy_sets_num):
            copy_set = []

            if len(rack_indexes) < self.r:
                break

            # make sure each slice spread across self.r(<self.n) different racks
            chosen_rack_indexes = sample(rack_indexes, self.r)
            for i, rack_index in enumerate(chosen_rack_indexes):
                rack = available_racks[rack_index]
                if len(counts[rack_index]) < self.slices_chunks_on_racks[i]:
                    break
                disk_indexes = sample(counts[rack_index], self.slices_chunks_on_racks[i])
                for disk_index in disk_indexes:
                    copy_set.append(rack[disk_index])
                    counts[rack_index].remove(disk_index)
                if counts[rack_index] == []:
                    rack_indexes.remove(rack_index)
            if len(copy_set) == self.n:
                copy_sets.append(copy_set)
        return copy_sets


if __name__ == "__main__":
    conf = Configuration("/root/CR-SIM/conf/cr-sim.conf")
    xml = XMLParser(conf)
    distribute = HierCOPYSETDistribute(xml)

    disks = []
    distribute.getAllDisks(distribute.getRoot(), disks)
    disks_set = [[0 for i in xrange(len(disks[0]))] for j in xrange(len(disks))]
    copy_sets = distribute.divideDisksIntoSets(disks)
    for i, copy_set in enumerate(copy_sets):
        format_output = "copy set " + str(i) + ": "
        for disk in copy_set:
            format_output += "  " + disk.toString()
            for j, rack_disks in enumerate(disks):
                if disk in rack_disks:
                    disk_index = rack_disks.index(disk)
                    disks_set[j][disk_index] += 1
        print format_output

    # print disks_set
"""
    total_slices = 419431
    distribute.distributeSlices(distribute.getRoot(), total_slices)
    for i in xrange(total_slices):
        format_output = "slice index " + str(i) + ": "
        for disk in distribute.slice_locations[i]:
            format_output += "  " + disk.toString()
        print format_output"""
