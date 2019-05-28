from simulator.failure.EventGenerator import EventGenerator


class Constant(EventGenerator):

    def __init__(self, name, parameters):
        self.frequency = float(parameters['freq'])
        self.name = name
        self.previous_event = 0

    def reset(self, current_time):
        self.previous_event = current_time

    def getName(self):
        return self.name

    def getCurrentTime(self):
        return self.previous_event

    def generateNextEvent(self, current_time):
        if self.previous_event + self.frequency < current_time:
            self.previous_event = current_time
            return self.previous_event

        return self.previous_event + self.frequency
