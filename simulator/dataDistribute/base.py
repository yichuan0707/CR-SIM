from time import strftime, time
from simulator.Configuration import Configuration
from simulator.XMLParser import XMLParser
from simulator.unit.Rack import Rack


class DataDistribute(object):

    def __init__(self, xml):
        self.xml = xml
        self.conf = self.xml.config
        self.n = self.conf.drs_handler.n
        self.k = self.conf.drs_handler.k
        self.num_chunks_diff_racks = self.conf.num_chunks_diff_racks

        # scaling up and heteogeneous layer need XMLParser module supports rebuild.
        units = self.xml.readFile()
        self.root = units[0]

        self.slice_locations = []
        # slice_infos format: slice_index:[(DRC, size, medium, locations, start, end),...]
        self.slice_infos = {}

        self.total_slices = 0
        self.conf.printAll()

    def _my_assert(self, expression):
        if not expression:
            raise Exception("Assertion Failed!")
        return True

    def returnConf(self):
        return self.conf

    def returnCodingParameters(self):
        return (self.n, self.k)

    def returnSliceLocations(self):
        return self.slice_locations

    def getRoot(self):
        return self.root

    def returnTotalSliceCount(self):
        return self.total_slices

    def returnRackCount(self):
        return len(self.root.getChildren()[0].getChildren())

    def returnMachinesPerRack(self):
        return len(self.root.getChildren()[0].getChildren()[0].getChildren())

    def returnDisksPerMachine(self):
        return len(self.root.getChildren()[0].getChildren()[0].getChildren()[0].getChildren())

    def returnChunksPerDisk(self):
        return self.root.getChildren()[0].getChildren()[0].getChildren()[0].getChildren()[0].chunks_per_disk

    def returnTotalDiskCount(self):
        rack_count = self.returnRackCount()
        machines_per_rack = self.returnMachinesPerRack()
        disks_per_machine = self.returnDisksPerMachine()
        return rack_count * machines_per_rack * disks_per_machine

    # scaling gives additional slices, not include in total_active_storage
    # def returnSliceCount(self):
    #    return self.conf.total_active_storage/(self.conf.block_size * self.n)

    def distributeSlices(self, root, total_slices):
        pass

    def loadBalancing(self, style, additions):
        pass

    def scalingDistributeSlices(self, style, additions):
        """
        System scaling up. Add a couple of disks, and old slices may need remapping.
        Add a couple of slices, the joining time of these slices are uncertain.
        """
        pass

    def distributeSliceToDisk(self):
        """
        Distribute one slice to one disk.
        """
        pass

    def start(self):
        self.distributeSlices(self.root, self.conf.total_slices)

    def end(self):
        print "Results"
        pass

    def getAllRacks(self):
        r = self.root
        if not isinstance(r.getChildren()[0], Rack):
            r = r.getChildren()[0]

        return r.getChildren()

    def getAllMachines(self):
        machines = []

        racks = self.getAllRacks()
        for rack in racks:
            rack_machines = rack.getChildren()
            for m in rack_machines:
                machines.append(m)

        return machines

    def getAllDisks(self, u, disks):
        for tmp in u.getChildren():
            if isinstance(tmp, Rack):
                rack_disks = []
                self.getAllDisksInRack(tmp, rack_disks)
                disks.append(rack_disks)
            else:
                self.getAllDisks(tmp, disks)

    def getAllDisksInRack(self, u, disks):
        for tmp in u.getChildren():
            for m in tmp.getChildren():
                disks.append(m)

    def printToFile(self, file_path=r"/root/CR-SIM/log/slices_distribution"):
        ts = strftime("%Y%m%d.%H.%M.%S")
        file_path += '-' + ts
        with open(file_path, 'w') as fp:
            for i, item in enumerate(self.slice_locations):
                info = "slice " + str(i) + ": "
                for d in item:
                    info += d.toString() + ", "
                info += "\n"
                fp.write(info)


class DataDistributeDynamic(object):
    pass


if __name__ == "__main__":
    pass

