from random import random
from copy import deepcopy

from simulator.failure.EventGenerator import EventGenerator


class Piecewise(EventGenerator):
    values = []
    intervals = []

    def __init__(self, name, parameters):
        self.name = name
        self.previous_event = 0

    def reset(self, current_time):
        self.previous_event = current_time

    def getName(self):
        return self.name

    def getCurrentTime(self):
        return 0

    def generateNextEvent(self, current_time):
        index = random()
        rang = random()

        for i in xrange(len(Piecewise.values)):
            if index >= Piecewise.intervals[i] and \
               index <= Piecewise.intervals[i + 1]:
                break

        assert (i < len(Piecewise.values))

        next_event = Piecewise.values[i] + rang*(Piecewise.values[i+1] -
                                                 Piecewise.values[i])
        if self.previous_event + next_event <= current_time:
            self.previous_event = current_time
            return self.previous_event
        return self.previous_event + next_event

    # I thought it should be implemented like this, but maybe wrong?
    @classmethod
    def Piecewise(cls, _intervals, _values):
        cls.intervals = deepcopy(_intervals)
        cls.values = deepcopy(_values)
        for i in xrange(len(cls.intervals)):
            cls.intervals[i] += cls.intervals[i-1]
