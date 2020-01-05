from random import randint, sample, choice

from simulator.Configuration import Configuration
from simulator.XMLParser import XMLParser
from simulator.dataDistribute.base import DataDistribute
from simulator.Log import error_logger


class RandomDistribute(DataDistribute):

    # both old and new disk are disk instance, slice_index means the slice
    # which the blocks belongs to.
    def _blockMoving(self, old_disk, new_disk, slice_index):
        old_disk.removeChild(slice_index)
        new_disk.addChild(slice_index)
        for i, disk in enumerate(self.slice_locations[slice_index]):
            if disk == old_disk:
                self.slice_locations[slice_index][i] = new_disk

    def _additionSpaceInBlocks(self, style, additions):
        chunks_per_disk = self.returnChunksPerDisk()
        disks_per_machine = self.returnDisksPerMachine()
        machines_per_rack = self.returnMachinesPerRack()
        rack_count = self.returnRackCount()

        addition_space = additions
        if style == 0:
            addition_space *= disks_per_machine * machines_per_rack * rack_count
        elif style == 1:
            addition_space *= chunks_per_disk * machines_per_rack * rack_count
        elif style == 2:
            addition_space *= chunks_per_disk * disks_per_machine * rack_count
        elif style == 3:
            addition_space *= chunks_per_disk * disks_per_machine * machines_per_rack
        else:
            raise Exception("Incorrect style")

        return addition_space

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
            if disk.getMetadata().slice_count >= self.conf.chunks_per_disk:
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

            m = disk.getMetadata()
            if m.slices == []:
                m.slices = [-10] * self.conf.chunks_per_disk

            # LZR
            self.slice_locations[slice_index].append(disk)
            # add slice indexs to children list of disks
            disk.addChild(slice_index)
            m.slices[m.slice_count] = slice_index
            m.slice_count += 1
            break

    def loadBalancing(self, style, additions):
        bandwidth_cost = 0

        if style not in [1, 2, 3]:
            raise Exception("Incorrect style for load balancing")
        elif style == 1:
            disks_per_machine = self.returnDisksPerMachine()
            self._my_assert(disks_per_machine > additions)
            pre_disks_per_machine = disks_per_machine - additions

            machines = self.getAllMachines()
            for machine in machines:
                pre_disks_in_machine = machine.getChildren()[:pre_disks_per_machine]
                new_disks_in_machine = machine.getChildren()[pre_disks_per_machine:]
                for disk in pre_disks_in_machine:
                    blocks = disk.getChildren()
                    moving_amount_per_disk = int(round(len(blocks) * additions / disks_per_machine))
                    moving_blocks_per_disk = sample(blocks, moving_amount_per_disk)
                    for slice_index in moving_blocks_per_disk:
                        new_disk_for_block = choice(new_disks_in_machine)
                        self._blockMoving(disk, new_disk_for_block, slice_index)
                        bandwidth_cost += 1
        elif style == 2:
            machines_per_rack = self.returnMachinesPerRack()
            self._my_assert(machines_per_rack > additions)
            pre_machines_per_rack = machines_per_rack - additions

            racks = self.getAllRacks()
            for rack in racks:
                pre_machines_in_rack = rack.getChildren()[:pre_machines_per_rack]
                new_machines_in_rack = rack.getChildren()[pre_machines_per_rack:]
                new_disks = []
                for machine in new_machines_in_rack:
                    new_disks += machine.getChildren()
                for machine in pre_machines_in_rack:
                    disks = machine.getChildren()
                    for disk in disks:
                        blocks = disk.getChildren()
                        moving_amount_per_disk = int(round(len(blocks) * additions / machines_per_rack))
                        moving_blocks_per_disk = sample(blocks, moving_amount_per_disk)
                        for slice_index in moving_blocks_per_disk:
                            new_disk_for_block = choice(new_disks)
                            self._blockMoving(disk, new_disk_for_block, slice_index)
                            bandwidth_cost += 1
        else:  # style == 3
            rack_count = self.returnRackCount()
            self._my_assert(rack_count > additions)
            pre_rack_count = rack_count - additions

            racks = self.getAllRacks()
            pre_racks = racks[:pre_rack_count]
            new_racks = racks[pre_rack_count:]

            new_disks = []
            for rack in new_racks:
                disks = []
                self.getAllDisksInRack(rack, disks)
                new_disks += disks
            for rack in pre_racks:
                machines = rack.getChildren()
                for machine in machines:
                    disks = machine.getChildren()
                    for disk in disks:
                        blocks = disk.getChildren()
                        moving_amount_per_disk = int(round(len(blocks)* additions / rack_count))
                        moving_blocks_per_disk = sample(blocks, moving_amount_per_disk)
                        for slice_index in moving_blocks_per_disk:
                            new_disk_for_block = choice(new_disks)
                            self._blockMoving(disk, new_disk_for_block, slice_index)
                            bandwidth_cost += 1

        return bandwidth_cost

    def systemScaling(self, ts, inc_capcity, inc_slices, style, new_chunks_per_disk=None, load_balancing=False, e_generators={}):
        d_generators = []
        m_generators = []
        r_generators = []
        new_generators_names = e_generators.keys()

        if "d_generators" in new_generators_names:
            d_generators = e_generators["d_generators"]
        if "m_generators" in new_generators_names:
            m_generators = e_generators["m_generators"]
        if "r_generators" in new_generators_names:
            r_generators = e_generators["r_generators"]

        self._my_assert(self.slice_locations != [])

        additions = self.xml.systemDiskChanges(self.root, ts, inc_capcity, style, new_chunks_per_disk, d_generators, m_generators, r_generators)
        self._my_assert(inc_slices * self.n <= self._additionSpaceInBlocks(style, additions))

        if style in [0, 1, 2, 3]:
            if load_balancing and style != 0:
                load_banlancing_cost = self.loadBalancing(style, additions)
            self.distributeSlices(self.root, inc_slices)
        else:
            raise Exception("Incorrect style!")

        return

    def printTest(self):
        print "current rack count: ", self.returnRackCount()
        print "current machins per rack: ", self.returnMachinesPerRack()
        print "current disks per machines: ", self.returnDisksPerMachine()
        print "current total slices: ", self.total_slices
        print "chunks_per_disk: ", self.root.getChildren()[0].getChildren()[0].getChildren()[0].getChildren()[0].chunks_per_disk


class RandomDistributeSSS(RandomDistribute):
    """
    SSS: Spread placement Strategy System.
    All stripes of one file randomly spread, so the file spreads more than n disks.
    """
    pass


class RandomDistributePSS(RandomDistribute):
    """
    PSS: Partitioned placement Strategy System.
    All stripes of one file spread across n fixed disks. All disks organized as subsets
    which have n disks.
    """
    pass


class RandomDistributeSET(RandomDistribute):
    """
    CopySet(Cidon et al. 2013)
    """
    pass


class RandomDistributeSameRack(RandomDistribute):
    """
    HDFS: three replicas, two of them on different machines of same rack, the third one on a
        different rack
    QFS: n blocks, several of them on same rack
    """


class RandomDistributeDRC(RandomDistribute):
    """
    DRC: Double Regenerating Codes.(Hu et al. 2017)
    """
    pass


if __name__ == "__main__":
    conf = Configuration()
    xml = XMLParser(conf)
    sss = RandomDistributeSSS(xml)
    sss.start()
    sss.printTest()
    sss.printToFile()
    sss.systemScaling(1000, 0.1, 20000, 3, 9000, True)
    sss.printTest()
    sss.printToFile()
