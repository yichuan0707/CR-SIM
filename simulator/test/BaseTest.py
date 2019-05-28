import sys
import os

from simulator.Log import error_logger, info_logger
from simulator.GenerateEvents import GenerateEvents
from simulator.eventHandler.NormalEventHandler import NormalDistributeEventHandler

DEFAULT = r"/root/CR-SIM/conf/"


class Test(object):

    def __init__(self):
        error_logger.error("Starting simulation ")

    def main(self, conf_path):
        events_handled = 0
        ge = GenerateEvents(conf_path)
        distributer = ge.getDistributer()
        events = ge.main()
        handler = NormalDistributeEventHandler(distributer)

        print "total slices:", handler.total_slices
        e = events.removeFirst()
        while e is not None:
            handler.handleEvent(e, events)
            e = events.removeFirst()
            events_handled += 1

        result = handler.end()
        info_logger.info(result.toString())
        info_logger.info("Events handled: %d" % events_handled)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise Exception("Usage: python Test.py conf_path num_iterations")
    conf_path = sys.argv[1]
    num_iterations = sys.argv[2]

    t = Test()
    t.main(conf_path)

