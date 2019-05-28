from numpy.random import randn

from simulator.failure.EventGenerator import EventGenerator


class GaussianGenerator(EventGenerator):

    def __init__(self, name, parameters):
        self.name = name
        self.start_time = 0
        self.mean = float(parameters['mean'])
        self.stddev = float(parameters['stddev'])
        self.minval = float(parameters['minval'])

        if self.mean < self.stddev:
            raise Exception("Mean is smaller than stddev")

        if self.minval > self.mean - self.stddev:
            raise Exception("Minval is too large and will slow down the \
                            simulation")

    def reset(self, current_time):
        self.current_time = current_time

    def generateNextEvent(self, current_time):
        if current_time < self.start_time:
            raise Exception("current time is less than the start time")

        next_val = 0.0
        while next_val < self.minval:
            next_val = randn()*self.stddev + self.mean

        if next_val < 0:
            raise Exception("Negative value generated!")

        if self.start_time + next_val < current_time:
            self.start_time = current_time
            return self.start_time

        return self.start_time + next_val

    def getName(self):
        return self.name

    def getCurrentTime(self):
        return self.start_time
