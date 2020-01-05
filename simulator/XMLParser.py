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
from simulator.failure.Real import Real
from simulator.failure.Period import Period
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
        elif name.lower() == "real":
            return Real
        elif name.lower() == "period":
            return Period
        elif name.lower() == "gfsavailability":
            return GFSAvailability
        elif name.lower() == "gfsavailability2":
            return GFSAvailability2
        else:
            raise Exception("Invalid event class name")

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
            count = self.conf.getRackCount()
        elif name.lower() == "machine":
            count = self.conf.getMachinesPerRack()
        elif name.lower() == "disk":
            count = self.conf.getDisksPerMachine()
        elif name.lower() == "datacenter":
            count = self.conf.getDCCount()
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


if __name__ == "__main__":
    conf = Configuration()
    xml = XMLParser(conf)
    units = xml.readFile()
    # readComponent(xml.root, None)
    units[0].printAll()
