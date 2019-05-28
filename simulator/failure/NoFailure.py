from numpy import Inf

from simulator.failure.EventGenerator import EventGenerator


class NoFailure(EventGenerator):

    def __init__(self, name, parameters):
        pass

    def getName(self):
        return "NoFailure"

    def getCurrentTime(self):
        return Inf

    def generateNextEvent(self, current_time):
        return Inf
