from math import ceil

from simulator.failure.EventGenerator import EventGenerator


class Period(EventGenerator):
    """
    System periodic check failures.
    """

    def __init__(self, name, parameters):
        self.name = name
        # check period
        self.gamma = float(parameters['gamma'])

    def reset(self, current_time):
        pass

    def getName(self):
        return self.name

    def getCurrentTime(self):
        return 0

    def generateNextEvent(self, current_time):
        return ceil(current_time/self.gamma) * self.gamma
