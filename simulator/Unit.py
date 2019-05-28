from abc import ABCMeta
from copy import deepcopy

from simulator.Metadata import Metadata
from simulator.Event import Event


class Unit:
    __metaclass__ = ABCMeta
    unit_count = 0

    def __init__(self, name, parent, parameters):
        self.children = []
        self.parent = parent
        self.name = name
        self.failure_generator = None
        self.recovery_generator = None
        self.meta = Metadata()

        self.id = Unit.unit_count
        self.start_time = 0
        self.end_time = None
        self.last_failure_time = 0
        self.last_bandwidth_need = 0
        self.failure_intervals = []

        Unit.unit_count += 1

    def setStartTime(self, ts):
        self.start_time = ts
        if self.children != []:
            for child in self.children:
                child.setStartTime(ts)

    def getStartTime(self):
        return self.start_time

    def setLastFailureTime(self, ts):
        self.last_failure_time = ts

    def getLastFailureTime(self):
        return self.last_failure_time

    def addFailureInterval(self, interval):
        self.failure_intervals.append(deepcopy(interval))

    def updateFailureInterval(self, interval, new_start_time):
        if interval not in self.failure_intervals:
            raise Exception("Wrong interval for update")
        index = self.failure_intervals.index(interval)
        self.failure_intervals[index][0] = new_start_time

    def removeFailureInterval(self, interval):
        self.failure_intervals.remove(interval)

    def setLastBandwidthNeed(self, bw):
        self.last_bandwidth_need = bw

    def getLastBandwidthNeed(self):
        return self.last_bandwidth_need

    # unit must be a instance? Really?
    def addChild(self, unit):
        self.children.append(unit)

    def removeChild(self, unit):
        self.children.remove(unit)

    # return the amount of children for replacement
    def removeAllChildren(self):
        count = len(self.children)
        self.children = []
        return count

    def getChildren(self):
        return self.children

    def getParent(self):
        return self.parent

    def getMetadata(self):
        return self.meta

    def getID(self):
        return self.id

    def addEventGenerator(self, generator):
        if generator.getName() == "failureGenerator":
            self.failure_generator = generator
        elif generator.getName() == "recoveryGenerator":
            self.recovery_generator = generator
        else:
            raise Exception("Unknown generator" + generator.getName())

    def getEventGenerators(self):
        return [self.failure_generator, self.recovery_generator]

    def addCorrelatedFailures(self, result_events, failure_time, recovery_time, lost_flag):
        fail_event = Event(Event.EventType.Failure, failure_time, self)
        recovery_event = Event(Event.EventType.Recovered, recovery_time, self)
        result_events.addEvent(fail_event)
        result_events.addEvent(recovery_event)

        if [failure_time, recovery_time, lost_flag] in self.failure_intervals:
            self.failure_intervals.remove([failure_time, recovery_time, lost_flag])

        return fail_event

    def generateEvents(self, result_events, start_time, end_time, reset):
        current_time = start_time
        last_recover_time = start_time

        if self.failure_generator is None:
            for unit in self.children:
                unit.generateEvents(result_events, start_time, end_time, reset)
            return

        while True:
            if reset:
                self.failure_generator.reset(current_time)

            failure_time = self.failure_generator.generateNextEvent(
                current_time)
            current_time = failure_time
            if current_time > end_time:
                for u in self.children:
                    u.generateEvents(result_events, last_recover_time,
                                     end_time, True)
                break
            fail_event = Event(Event.EventType.Failure, current_time, self)
            result_events.addEvent(fail_event)
            for u in self.children:
                u.generateEvents(result_events, last_recover_time,
                                 current_time, True)

            self.recovery_generator.reset(current_time)
            recovery_time = self.recovery_generator.generateNextEvent(
                current_time)
            assert (recovery_time > failure_time)
            current_time = recovery_time
            fail_event.next_recovery_time = recovery_time

            if current_time > end_time:
                break

            result_events.addEvent(Event(Event.EventType.Recovered,
                                         current_time, self))
            last_recover_time = current_time

    def toString(self):
        if self.parent is None:
            return self.name
        else:
            return self.parent.toString() + '.' + self.name

    def printAll(self, prefix="--"):
        print prefix + self.name + "  " + str(self.failure_intervals)
        prefix += "--"
        for unit in self.children:
            if isinstance(unit, int):
                break
            unit.printAll(prefix)

