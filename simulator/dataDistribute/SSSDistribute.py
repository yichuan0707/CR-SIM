from math import floor
from random import randint, sample, choice

from simulator.Configuration import Configuration
from simulator.XMLParser import XMLParser
from simulator.dataDistribute.base import DataDistribute
from simulator.Log import error_logger


class SSSDistribute(DataDistribute):
    """
    SSS: Spread placement Strategy System.
    All stripes of one file randomly spread, so the file spreads more than n disks.
    """
    def distributeSlices(self, root, increase_slices):
        disks = []

        self.getAllDisks(root, disks)
        self.total_slices += increase_slices
        for i in xrange(self.total_slices - increase_slices, self.total_slices):
            self.slice_locations.append([])
            tmp_racks = [item for item in disks]
            for j in xrange(self.n):
                if j < self.num_chunks_diff_racks:
                    self.distributeSliceToDisk(i, disks, tmp_racks, True)
                else:
                    self.distributeSliceToDisk(i, disks, tmp_racks, False)

            self._my_assert(len(tmp_racks) == (self.returnRackCount() - self.n))
            self._my_assert(len(self.slice_locations[i]) == self.n)

        self._my_assert(len(self.slice_locations) == self.total_slices)

    def distributeSliceToDisk(self, slice_index, disks, available_racks, separate_racks):
        retry_count = 0
        same_rack_count = 0
        same_disk_count = 0
        full_disk_count = 0

        while True:
            retry_count += 1
            # choose disk from the right rack
            if len(available_racks) == 0:
                raise Exception("No racks left")
            prev_racks_index = randint(0, len(available_racks)-1)
            rack_disks = available_racks[prev_racks_index]

            disk_index_in_rack = randint(0, len(rack_disks)-1)
            disk = rack_disks[disk_index_in_rack]
            slice_count = len(disk.getChildren())
            if slice_count >= self.conf.max_chunks_per_disk:
                full_disk_count += 1
                rack_disks.remove(disk)

                if len(rack_disks) == 0:
                    error_logger.error("One rack is completely full" + str(disk.getParent().getParent().getID()))
                    available_racks.remove(rack_disks)
                    disks.remove(rack_disks)
                if retry_count > 100:
                    error_logger.error("Unable to distribute slice " + str(slice_index) + "; picked full disk " +
                                       str(full_disk_count) + " times, same rack " + str(same_rack_count) +
                                       " times, and same disk " + str(same_disk_count) + " times")
                    raise Exception("Disk distribution failed")
                continue

            available_racks.remove(rack_disks)

            # LZR
            self.slice_locations[slice_index].append(disk)

            # add slice indexs to children list of disks
            disk.addChild(slice_index)
            break


class HierSSSDistribute(SSSDistribute):
    """
    Hierarchical + SSS dataplacement
    """
    def __init__(self, xml):
        super(HierSSSDistribute, self).__init__(xml)
        self.r = self.conf.distinct_racks
        slice_chunks_on_each_rack = self.n/self.r
        if slice_chunks_on_each_rack == 0:
            raise Exception("Distinct_racks is too large")
        self.slices_chunks_on_racks = [slice_chunks_on_each_rack] * self.r
        if self.n % self.r:
            for i in xrange(self.n % self.r):
                self.slices_chunks_on_racks[i] += 1

    def distributeSlices(self, root, increase_slices):
        full_disk_count = 0
        full_machine_count = 0

        self.total_slices += increase_slices
        disks_per_machine = self.conf.disks_per_machine
        machines_per_rack = self.conf.machines_per_rack
        rack_count = self.conf.rack_count

        machines = self.getAllMachines()

        disk_indexes = [[[a for a in xrange(disks_per_machine)] for i in xrange(machines_per_rack)] for j in xrange(rack_count)]
        machine_indexes = [[i for i in xrange(machines_per_rack)] for j in xrange(rack_count)]
        rack_indexes = [i for i in xrange(rack_count)]

        for slice_index in xrange(self.total_slices - increase_slices, self.total_slices):
            retry_count = 0
            location = []

            # (rack_index, machine_index)
            rack_machine_indexes = []
            while retry_count <= 100:
                flag = False
                chosen_rack_indexes = sample(rack_indexes, self.r)
                for i, item in enumerate(chosen_rack_indexes):
                    if len(machine_indexes[item]) < self.slices_chunks_on_racks[i]:
                        flag = True
                if flag:
                    retry_count += 1
                    continue
                for i, rack_index in enumerate(chosen_rack_indexes):
                    chosen_machine_indexes = sample(machine_indexes[rack_index], self.slices_chunks_on_racks[i])
                    for m_index in chosen_machine_indexes:
                        rack_machine_indexes.append((rack_index, m_index))
                break

            for rack_index, m_index in rack_machine_indexes:
                disk_index = choice(disk_indexes[rack_index][m_index])
                disk = machines[rack_index][m_index].getChildren()[disk_index]
                disk.addChild(slice_index)
                location.append(disk)

                if len(disk.getChildren()) >= self.conf.max_chunks_per_disk:
                    full_disk_count += 1
                    error_logger.info("One disk is completely full " + str(disk.toString()))
                    disk_indexes[rack_index][m_index].remove(disk_index)
                    if disk_indexes[rack_index][m_index] == []:
                        machine_indexes[rack_index].remove(m_index)
                        if machine_indexes[rack_index] == []:
                            rack_indexes.remove(rack_index)

            self.slice_locations.append(location)

            self._my_assert(len(self.slice_locations[slice_index]) == self.n)
        self._my_assert(len(self.slice_locations) == self.total_slices)


if __name__ == "__main__":
    conf = Configuration()
    xml = XMLParser(conf)
    sss = HierSSSDistribute(xml)
    print "disk usage is: ", sss.diskUsage()
    sss.start()
    sss.printToFile()
    print "disk usage is: ", sss.diskUsage()

