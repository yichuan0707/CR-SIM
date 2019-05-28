import os
import xml.etree.ElementTree as ET

from math import ceil
from copy import deepcopy

from simulator.unit.Layer import Layer
from simulator.unit.DataCenter import DataCenter
from simulator.unit.Rack import Rack
from simulator.unit.Machine import Machine
from simulator.unit.Disk import Disk
from simulator.unit.DiskWithScrubbing import DiskWithScrubbing

from simulator.failure.WeibullGenerator import WeibullGenerator
from simulator.failure.Constant import Constant
from simulator.failure.GaussianGenerator import GaussianGenerator
from simulator.failure.Uniform import Uniform
from simulator.failure.NoFailure import NoFailure
from simulator.failure.GFSAvailability import GFSAvailability
from simulator.failure.GFSAvailability2 import GFSAvailability2

from simulator.Configuration import Configuration, CONF_PATH


class XMLParser(object):

    def __init__(self, conf):
        layer_path = CONF_PATH + os.sep + "layer.xml"
        self.tree = ET.parse(layer_path)
        self.root = self.tree.getroot()
        self.conf = conf

    def _component_class(self, class_name):
        name = class_name.split(".")[-1]
        name.strip()

        if name.lower() == "layer":
            return Layer
        elif name.lower() == "datacenter":
            return DataCenter
        elif name.lower() == "rack":
            return Rack
        elif name.lower() == "machine":
            return Machine
        elif name.lower() == "disk":
            return Disk
        elif name.lower() == "diskwithscrubbing":
            return DiskWithScrubbing
        else:
            raise Exception("Invalid component class name")

    def _event_class(self, class_name):
        name = class_name.split(".")[-1]
        name.strip()
        if name.lower() == "weibullgenerator":
            return WeibullGenerator
        elif name.lower() == "gaussiangenerator":
            return GaussianGenerator
        elif name.lower() == "constant":
            return Constant
        elif name.lower() == "uniform":
            return Uniform
        elif name.lower() == "nofailure":
            return NoFailure
        elif name.lower() == "gfsavailability":
            return GFSAvailability
        elif name.lower() == "gfsavailability2":
            return GFSAvailability2
        else:
            raise Exception("Invalid event class name")

    @property
    def config(self):
        return self.conf

    def readFile(self):
        return self.readComponent(self.root, None)

    def readComponent(self, node, parent):
        # self.i += 1
        # print "*"*50
        # print "this is the %d times of executing readComponent" % self.i
        name = None
        class_name = None
        next_component = None
        count = 1
        attributes = {}
        component = node
        if component is not None:
            for child in component:
                # print child.tag, child.text
                if child.tag == "name":
                    name = child.text
                elif child.tag == "count":
                    count = child.text
                elif child.tag == "class":
                    class_name = child.text
                elif child.tag == "component":
                    pass
                elif child.tag == "eventGenerator":
                    pass
                else:
                    attributes[child.tag] = child.text

        if name is None:
            raise Exception("no name for " + node.tag)
        if class_name is None:
            raise Exception("no class name for " + node.tag)

        if name.lower() == "rack":
            count = self.conf.rack_count
        elif name.lower() == "machine":
            count = self.conf.machines_per_rack
        elif name.lower() == "disk":
            count = self.conf.disks_per_machine
        elif name.lower() == "datacenter":
            count = self.conf.datacenters
        else:
            pass

        units = []
        for i in xrange(count):
            # print "class_name:" + class_name
            unit_class = self._component_class(class_name)
            units.append(unit_class(name+str(i), parent, attributes))
            # layer is logic, has not failure and recovery events
            if name.lower() != "layer":
                e_generators = component.iterfind("eventGenerator")
                # if name.lower() == "disk":
                #     print "e generators:", e_generators
                if e_generators is not None:
                    for event in e_generators:
                        units[i].addEventGenerator(
                            self.readEventGenerator(event))

        if component is not None:
            next_component = component.find("component")
            if next_component is not None:
                for i in xrange(count):
                    children = self.readComponent(next_component, units[i])
                    for j in xrange(len(children)):
                        units[i].addChild(children[j])
        return units

    def readEventGenerator(self, node):
        name = None
        class_name = None
        attributes = {}

        for child in node:
            if child.tag == "name":
                name = child.text
            elif child.tag == "class":
                class_name = child.text
            else:
                attributes[child.tag] = child.text

        if name is None:
            raise Exception("no name for " + node.tag)
        if class_name is None:
            raise Exception("no class name for " + node.tag)

        generator_class = self._event_class(class_name)
        return generator_class(name, attributes)

    def systemDiskChanges(self, root, ts, inc_capacity, style=1, new_chunks_per_disk=None, d_generators=[], m_generators=[], r_generators=[]):
        """
        Upgrading disks for high speed/high reliability/high capacity, or adding new disks into the system
        for system capacity enlargement.
        Parameters:
            root: tree root of all components' instances in the system;
            ts: start time of disk changes;
            inc_capacity: system capacity incremental, in PBs;
            style: how to scale? "0" means replacing the old disks with high-capacity disks,
                                 "1" means adding new disks on each node,
                                 "2" means adding new nodes on each rack,
                                 "3" means adding new racks in the system;
            new_chunks_per_disk: new chunks per disk for style=0, typically 3000 chunks for each 1TB space.
            d_generators: the event generators for new disks,
                          [failure_generator, recovery_generator, LSE_generator, scrub_generator]
            m_generators/r_generators: event generators for new machines or new racks, respectively;
                                       [failure_generator, recovery_generator]
        """
        dc_unit = root.getChildren()[0]
        rack_units = dc_unit.getChildren()
        # Obtain the real-time rack count, machines per rack, disks per machine from SYSTEM Tree.
        # Because many times scaling up will make the data in Configuration class obsolete.
        rack_count = len(rack_units)
        machines_per_rack = len(rack_units[0].getChildren())
        disks_per_machine = len(rack_units[0].getChildren()[0].getChildren())
        chunks_per_disk = rack_units[0].getChildren()[0].getChildren()[0].chunks_per_disk

        total_disk_count = rack_count * machines_per_rack * disks_per_machine
        # system capacity in blocks
        system_capacity = total_disk_count * chunks_per_disk

        # incremental capacity in blocks
        inc_capacity = (float(inc_capacity) * 1024 * 1024 * 1024)/ self.conf.chunk_size
        if style == 0:
            if new_chunks_per_disk is None:
                raise Exception("New chunks per disk is not given!")
            if chunks_per_disk + inc_capacity/total_disk_count > new_chunks_per_disk:
                raise Exception("Disk Upgrading can not provide enough capacity")

            for rack in rack_units:
                for machine in rack.getChildren():
                    if d_generators == []:
                        for disk in machine.getChildren():
                            disk.setDiskCapacity(new_chunks_per_disk)
                    else:
                        new_disk_units = []
                        count = machine.removeAllChildren()
                        for i in xrange(count):
                            new_disk = DiskWithScrubbing("disk"+str(i), machine, {})
                            new_disk.setDiskCapacity(new_chunks_per_disk)
                            for d_generator in d_generators:
                                new_disk.addEventGenerator(deepcopy(d_generator))
                            machine.addChild(new_disk)
            return (new_chunks_per_disk - chunks_per_disk)
        elif style == 1:  # Added disks on each machine
            add_disks = int(ceil((inc_capacity/chunks_per_disk)/
                                 (machines_per_rack*rack_count)))
            for rack in rack_units:
                for machine in rack.getChildren():
                    count = len(machine.getChildren())
                    for i in xrange(count, count+add_disks):
                        new_disk = DiskWithScrubbing("disk"+str(i), machine, {})
                        new_disk.setStartTime(ts)
                        if new_chunks_per_disk is not None:
                            new_disk.setDiskCapacity(new_chunks_per_disk)
                        if d_generators == []:
                            d_generators = machine.getChildren()[0].getEventGenerators()
                        for d_generator in d_generators:
                            new_disk.addEventGenerator(deepcopy(d_generator))
                        machine.addChild(new_disk)
            return add_disks
        elif style == 2:  # Added machines on each rack
            add_machines = int(ceil((inc_capacity/(chunks_per_disk*disks_per_machine))/
                                    rack_count))
            for rack in rack_units:
                count = len(rack.getChildren())
                for i in xrange(count, count+add_machines):
                    new_machine = Machine("machine"+str(i), rack, {})
                    if m_generators == []:
                        m_generators = rack.getChildren()[0].getEventGenerators()
                    for m_generator in m_generators:
                        new_machine.addEventGenerator(deepcopy(m_generator))
                    for j in xrange(disks_per_machine):
                        disk = DiskWithScrubbing("disk"+str(j), new_machine, {})
                        if new_chunks_per_disk is not None:
                            disk.setDiskCapacity(new_chunks_per_disk)
                        if d_generators == []:
                            d_generators = rack.getChildren()[0].getChildren()[0].getEventGenerators()
                        for d_generator in d_generators:
                            disk.addEventGenerator(deepcopy(d_generator))
                        new_machine.addChild(disk)
                    new_machine.setStartTime(ts)
                    rack.addChild(new_machine)
            return add_machines
        elif style == 3:  # Added racks
            add_racks = int(ceil(inc_capacity/(chunks_per_disk*disks_per_machine*machines_per_rack)))
            count = len(rack_units)
            for i in xrange(count, count+add_racks):
                new_rack = Rack("rack"+str(i), dc_unit, {})
                if r_generators == []:
                    e_generators = rack_units[0].getEventGenerators()
                for r_generator in r_generators:
                    new_rack.addEventGenerator(deepcopy(r_generator))
                for j in xrange(machines_per_rack):
                    machine = Machine("machine"+str(j), new_rack, {})
                    if m_generators == []:
                        m_generators = rack_units[0].getChildren()[0].getEventGenerators()
                    for m_generator in m_generators:
                        machine.addEventGenerator(deepcopy(m_generator))
                    for k in xrange(disks_per_machine):
                        disk = DiskWithScrubbing("disk"+str(k), machine, {})
                        if new_chunks_per_disk is not None:
                            disk.setDiskCapacity(new_chunks_per_disk)
                        if d_generators == []:
                            d_generators = rack_units[0].getChildren()[0].getChildren()[0].getEventGenerators()
                        for d_generator in d_generators:
                            disk.addEventGenerator(deepcopy(d_generator))

                        machine.addChild(disk)
                    new_rack.addChild(machine)
                new_rack.setStartTime(ts)
                dc_unit.addChild(new_rack)
            return add_racks
        else:
            raise Exception("Wrong style")


if __name__ == "__main__":
    conf = Configuration()
    xml = XMLParser(conf)
    units = xml.readFile()
    # readComponent(xml.root, None)
    units[0].printAll()
    d_generators =[WeibullGenerator("failureGenerator", {"lamda":0, "beta":191, "gamma":1.1}),
                   WeibullGenerator("recoveryGenerator", {"lamda":0, "beta":192, "gamma":1.2}),
                   WeibullGenerator("latentErrorGenerator", {"lamda":0, "beta":193,"gamma":1.3}),
                   WeibullGenerator("scrubGenerator", {"lamda":0, "beta":194, "gamma":1.4})]
    m_generators =[WeibullGenerator("failureGenerator", {"lamda":1.0, "beta":291, "gamma":3.1}),
                   WeibullGenerator("recoveryGenerator", {"lamda":1.0, "beta":292, "gamma":3.2})]
    r_generators =[WeibullGenerator("failureGenerator", {"lamda":2.0, "beta":391, "gamma":4.1}),
                   WeibullGenerator("recoveryGenerator", {"lamda":2.0, "beta":392, "gamma":4.2})]
    for unit in units[0].getChildren()[0].getChildren()[0].getChildren()[0].getChildren():
        print unit.chunks_per_disk
        print unit.failure_generator.lamda, unit.failure_generator.gamma, unit.failure_generator.beta

    xml.systemDiskChanges(units[0], 1000, 0.1, 0, 7000, d_generators, m_generators, r_generators)
    units[0].printAll()
    for unit in units[0].getChildren()[0].getChildren()[-1].getChildren()[-1].getChildren():
        print unit.chunks_per_disk
        print unit.failure_generator.lamda, unit.failure_generator.gamma, unit.failure_generator.beta

    print type(xml.config)
