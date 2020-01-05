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
    def __init__(self, xml):
        super(COPYSETDistribute, self).__init__(xml)
        self.s = self.conf.scatter_width

    # return n usable machine indexes
    def _getMachinesFromCopyset(self, copy_set, full_machine_indexes):
        if len(copy_set) - len(full_machine_indexes) < self.n:
            return None
        usable_machine_indexes = [i for i in xrange(len(copy_set))]
        for i in full_machine_indexes:
            usable_machine_indexes.remove(i)
        chosen_index = sample(usable_machine_indexes, self.n)
        return chosen_index

    def divideMachinesIntoSets(self, machines):
        copy_sets = []

        machine_indexes = [[i for i in xrange(self.conf.machines_per_rack)] for j in xrange(self.conf.rack_count)]
        rack_indexes = [i for i in xrange(self.conf.rack_count)]

        while len(rack_indexes) >= self.s:
            copy_set = []
            chosen_rack_indexes = sample(rack_indexes, self.s)
            for rack_index in chosen_rack_indexes:
                rack = machines[rack_index]
                machine_index = choice(machine_indexes[rack_index])
                copy_set.append(rack[machine_index])
                machine_indexes[rack_index].remove(machine_index)
                if machine_indexes[rack_index] == []:
                    rack_indexes.remove(rack_index)
            copy_sets.append(copy_set)
        self.groups = copy_sets

        """ Print copy sets"""
        machines_set = [[0 for i in xrange(len(machines[0]))] for j in xrange(len(machines))]
        for i, copy_set in enumerate(copy_sets):
            format_output = "copy set " + str(i) + ": "
            for machine in copy_set:
                format_output += "  " + machine.toString()
                for j, rack_machines in enumerate(machines):
                    if machine in rack_machines:
                        machine_index = rack_machines.index(machine)
                        machines_set[j][machine_index] += 1

        return copy_sets

    def distributeSlices(self, root, increase_slices):
        full_disk_count = 0
        full_machine_count = 0
        disks_per_machine = self.returnDisksPerMachine()
        self.total_slices += increase_slices
        machines_in_racks = self.getAllMachines()
        copy_sets = self.divideMachinesIntoSets(machines_in_racks)

        full_disk_indexes = [[[] for i in xrange(self.s)] for j in xrange(len(copy_sets))]
        full_machine_indexes = [[] for j in xrange(len(copy_sets))]
        copysets_index = [i for i in xrange(len(copy_sets))]
        for i in xrange(self.total_slices - increase_slices, self.total_slices):
            locations = []
            retry_count = 0
            while retry_count <= 100:
                copy_set_index = choice(copysets_index)
                copy_set = copy_sets[copy_set_index]
                machine_indexes = self._getMachinesFromCopyset(copy_set, full_machine_indexes[copy_set_index])
                if machine_indexes is None:
                    copysets_index.remove(copysets_index.remove(copy_set_index))
                    retry_count += 1
                    continue
                else:
                    break

            for machine_index in machine_indexes:
                machine = copy_set[machine_index]

                disk_indexes = [j for j in xrange(self.conf.disks_per_machine)]
                for i in full_disk_indexes[copy_set_index][machine_index]:
                    disk_indexes.remove(i)
                try:
                    disk_index = choice(disk_indexes)
                except IndexError:
                    raise Exception("full machine is " + machine.toString())
                disk = machine.getChildren()[disk_index]
                disk.addChild(i)
                locations.append(disk)

                if len(disk.getChildren()) >= self.conf.max_chunks_per_disk:
                    full_disk_count += 1
                    full_disk_indexes[copy_set_index][machine_index].append(disk_index)
                    error_logger.error("A disk is completely full, full disk is " + disk.toString())
                    if len(full_disk_indexes[copy_set_index][machine_index]) == disks_per_machine:
                        full_machine_count += 1
                        full_machine_indexes[copy_set_index].append(machine_index)
                        error_logger.error("A machine is completely full, full machine is " + machine.toString())

            self.slice_locations.append(locations)

            self._my_assert(len(self.slice_locations[i]) == self.n)
        self._my_assert(len(self.slice_locations) == self.total_slices)

    def printGroupsToFile(self, file_path=r"/root/CR-SIM/log/groups"):
        ts = strftime("%Y%m%d.%H.%M.%S")
        file_path += '-' + ts
        with open(file_path, 'w') as fp:
            for i, item in enumerate(self.groups):
                info = "slices " + str(i) + ": "
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
        # self.copy_sets_in_racks = []
        slice_chunks_on_each_rack = self.s/self.r
        if slice_chunks_on_each_rack == 0:
            raise Exception("Distinct_racks is too large")
        self.slices_chunks_on_racks = [slice_chunks_on_each_rack] * self.r
        if self.s % self.r:
            for i in xrange(self.s % self.r):
                self.slices_chunks_on_racks[i] += 1

    # return n machine indexes, make sure these machines spread across r racks
    def _getMachinesFromCopyset(self, copy_set, full_machine_indexes):
        if len(copy_set) - len(full_machine_indexes) < self.n:
            return None
        usable_machine_indexes = [i for i in xrange(self.s)]
        for item in full_machine_indexes:
            usable_machine_indexes.remove(item)

        machine_indexes = [[] for i in xrange(self.r)]
        for index in usable_machine_indexes:
            machine = copy_set[index]
            rack = machine.getParent()
            for item in machine_indexes:
                if item == []:
                    item.append(index)
                    break
                else:
                    if copy_set[item[0]].getParent().toString() == rack.toString():
                        item.append(index)
                        break
        chunks_on_racks = [self.n/self.r] * self.r
        if self.n % self.r:
            for i in xrange(self.n % self.r):
                chunks_on_racks[i] += 1

        chosen_index = []
        for i, rack in enumerate(machine_indexes):
            try:
                chosen_index += sample(machine_indexes[i], chunks_on_racks[i])
            except ValueError:
                break
        if len(chosen_index) < self.n:
            chosen_index = sample(usable_machine_indexes, self.n)
        return chosen_index

    def divideMachinesIntoSets(self, machines):
        copy_sets = []
        retry_count = 0
        machine_indexes = [[i for i in xrange(self.conf.machines_per_rack)] for j in xrange(self.conf.rack_count)]
        rack_indexes = [i for i in xrange(self.conf.rack_count)]

        while retry_count < 100:
            copy_set = []
            try:
                retry_count += 1
                chosen_rack_indexes = sample(rack_indexes, self.r)
            except ValueError:
                continue

            flag = True
            for i, rack_index in enumerate(chosen_rack_indexes):
                if len(machine_indexes[rack_index]) < self.slices_chunks_on_racks[i]:
                    flag = False
            if not flag:
                continue

            for i, rack_index in enumerate(chosen_rack_indexes):
                rack = machines[rack_index]

                m_indexes = sample(machine_indexes[rack_index], self.slices_chunks_on_racks[i])
                for m_index in m_indexes:
                    copy_set.append(rack[m_index])
                    machine_indexes[rack_index].remove(m_index)
                if len(machine_indexes[rack_index]) < self.s/self.r:
                    rack_indexes.remove(rack_index)
            copy_sets.append(copy_set)
        self.groups = copy_sets

        machines_set = [[0 for i in xrange(len(machines[0]))] for j in xrange(len(machines))]
        for i, copy_set in enumerate(copy_sets):
            format_output = "copy set " + str(i) + ": "
            for machine in copy_set:
                format_output += "  " + machine.toString()
                for j, rack_machines in enumerate(machines):
                    if machine in rack_machines:
                        machine_index = rack_machines.index(machine)
                        machines_set[j][machine_index] += 1
        return copy_sets


if __name__ == "__main__":
    conf = Configuration("/root/CR-SIM/conf/cr-sim.conf")
    xml = XMLParser(conf)
    distribute = COPYSETDistribute(xml)

    machines = distribute.getAllMachines()
    distribute.distributeSlices(distribute.getRoot(), distribute.conf.total_slices)
    distribute.printToFile()

"""
    total_slices = 419431
    distribute.distributeSlices(distribute.getRoot(), total_slices)
    for i in xrange(total_slices):
        format_output = "slice index " + str(i) + ": "
        for disk in distribute.slice_locations[i]:
            format_output += "  " + disk.toString()
        print format_output"""
