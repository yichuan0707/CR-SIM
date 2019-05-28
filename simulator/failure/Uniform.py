from numpy.random import randint

from simulator.failure.EventGenerator import EventGenerator


class Uniform(EventGenerator):

    def __init__(self, name, parameters):
        self.name = name
        self.frequency = float(parameters['lamda'])

    def reset(self):
        pass

    def getName(self):
        return self.name

    def getCurrentTime(self):
        return 0

    def generateNextEvent(self, current_time):
        return current_time + float(self.frequency*randint()*1000)/1000.0
