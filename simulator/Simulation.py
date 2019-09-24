import os
import sys
import csv

from random import sample
from copy import deepcopy
from time import strftime

from simulator.Event import Event
from simulator.Result import Result
from simulator.utils import splitMethod
from simulator.EventQueue import EventQueue
from simulator.Log import info_logger, error_logger
from simulator.Configuration import Configuration
from simulator.XMLParser import XMLParser

from simulator.unit.Rack import Rack
from simulator.unit.Machine import Machine
from simulator.unit.Disk import Disk

from simulator.eventHandler.EventHandler import EventHandler
from simulator.eventHandler.RAFIEventHandler import RAFIEventHandler
from simulator.dataDistribute.SSSDistribute import SSSDistribute, HierSSSDistribute
from simulator.dataDistribute.PSSDistribute import PSSDistribute, HierPSSDistribute
from simulator.dataDistribute.COPYSETDistribute import COPYSETDistribute, HierCOPYSETDistribute

DEFAULT = r"/root/CR-SIM/conf/"
RESULT = r"/root/CR-SIM/log/result-"

def returnDistributer(data_placement, hier):
    if data_placement == "sss":
        if hier:
            return HierSSSDistribute
        else:
            return SSSDistribute
    elif data_placement == "pss":
        if hier:
            return HierPSSDistribute
        else:
            return PSSDistribute
    elif data_placement == "copyset":
        if hier:
            return HierCOPYSETDistribute
        else:
            return COPYSETDistribute
    else:
        raise Exception("Incorrect data placement")


class Simulation(object):

    def __init__(self, conf_path):
        self.conf_path = conf_path
        # self.distributer_class = distributer_class
        # self.event_handler = event_handler

        self.iteration_times = 1
        self.ts = strftime("%Y%m%d.%H.%M.%S")
        self.total_events_handled = 0

    def getDistributer(self):
        return self.distributer

    def writeToCSV(self, res_file_path, contents):
        with open(res_file_path, "w") as fp:
            writer = csv.writer(fp, lineterminator='\n')
            for item in contents:
                writer.writerow(item)

    def run(self):
        conf = Configuration(self.conf_path)
        xml = XMLParser(conf)
        distributer_class = returnDistributer(conf.data_placement, conf.hierarchical)
        self.distributer = distributer_class(xml)
        self.conf = self.distributer.returnConf()

        if self.conf.rafi_recovery:
            self.event_handler = RAFIEventHandler
        else:
            self.event_handler = EventHandler
        self.distributer.start()
        # self.distributer.printGroupsToFile()

        info_logger.info("disk usage is: " + str(self.distributer.diskUsage()*100) + "%\n")
        self.distributer.getRoot().printAll()

        events_handled = 0
        events = EventQueue()

        root = self.distributer.getRoot()
        root.generateEvents(events, 0, self.conf.total_time, True)

        # if False:
        if self.conf.event_file != None:
            events_file = self.conf.event_file + '-' + self.ts
            events.printAll(events_file, "Iteration number: "+str(self.iteration_times))
        self.iteration_times += 1

        handler = self.event_handler(self.distributer)

        print "total slices:", handler.total_slices
        e = events.removeFirst()
        while e is not None:
            handler.handleEvent(e, events)
            e = events.removeFirst()
            events_handled += 1

        self.total_events_handled += events_handled

        result = handler.end()
        info_logger.info(result.toString())
        return result

    def main(self, num_iterations):
        contents = []

        for i in xrange(num_iterations):
            result = self.run()
            contents.append([result.data_loss_prob, result.unavailable_prob, result.unavailable_prob1, result.unavailable_prob2, result.total_repair_transfers])

        res_file_path = RESULT + self.ts + ".csv"
        self.writeToCSV(res_file_path, contents)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise Exception("Usage: python Test.py conf_path num_iterations")
    path = sys.argv[1]
    num_iterations = int(sys.argv[2])

    if not os.path.isabs(path):
        conf_path = DEFAULT + path
    else:
        conf_path = path

    sim = Simulation(conf_path)
    sim.main(num_iterations)
