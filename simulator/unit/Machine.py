from random import random

from simulator.Unit import Unit
from simulator.Event import Event
from simulator.failure.Trace import Trace


class Machine(Unit):
    id_counter = 0
    fail_fraction = 0.0

    def __init__(self, name, parent, parameters):
        self.my_id = Machine.id_counter
        Machine.id_counter += 1
        super(Machine, self).__init__(name, parent, parameters)

        # amount of time after which a machine failure is treated as permanent,
        # and eager disk recovery is begun, if eager_recovery_enabled is True.
        self.fail_timeout = -1
        if self.fail_timeout == -1:
            # Fraction of machine failures that are permanent.
            Machine.fail_fraction = float(parameters.get("fail_fraction", 0.008))
            self.fail_timeout = float(parameters.get("fail_timeout", 0.25))
            # If True, machine failure and recovery events will be generated
            # but ignored.
            self.fast_forward = bool(parameters.get("fast_forward"))
            self.eager_recovery_enabled = bool(parameters.get(
                "eager_recovery_enabled"))

    def getFailureGenerator(self):
        return self.failure_generator

    def addCorrelatedFailures(self, result_events, failure_time, recovery_time, lost_flag):
        super(Machine, self).addCorrelatedFailures(result_events, failure_time, recovery_time, lost_flag)
        if lost_flag:
            for u in self.getChildren():
                u.addCorrelatedFailures(result_events, failure_time, recovery_time, lost_flag)

    def generateEvents(self, result_events, start_time, end_time, reset):
        if start_time < self.start_time:
            start_time = self.start_time
        current_time = start_time
        last_recover_time = start_time

        if self.failure_generator is None:
            for [fail_time, recover_time, flag] in self.failure_intervals:
                self.addCorrelatedFailures(result_events, fail_time, recover_time, flag)
            for u in self.children:
                u.generateEvents(result_events, start_time, end_time, True)
            return

        if isinstance(self.failure_generator, Trace):
            self.failure_generator.setCurrentMachine(self.my_id)
        if isinstance(self.recovery_generator, Trace):
            self.recovery_generator.setCurrentMachine(self.my_id)

        while True:
            if reset:
                self.failure_generator.reset(current_time)

            if isinstance(self.failure_generator, Trace):
                # For the event start.
                self.failure_generator.setCurrentEventType(True)

            failure_time = self.failure_generator.generateNextEvent(
                current_time)
            current_time = failure_time
            if current_time > end_time:
                for [fail_time, recover_time, flag] in self.failure_intervals:
                    self.addCorrelatedFailures(result_events, fail_time, recover_time, flag)
                for u in self.children:
                    u.generateEvents(result_events, last_recover_time,
                                     end_time, True)
                break

            if isinstance(self.failure_generator, Trace):
                # For event start.
                self.failure_generator.eventAccepted()

            if isinstance(self.recovery_generator, Trace):
                self.recovery_generator.setCurrentEventType(False)
            self.recovery_generator.reset(current_time)
            recovery_time = self.recovery_generator.generateNextEvent(
                current_time)
            assert (recovery_time > failure_time)

            for [fail_time, recover_time, _bool] in self.failure_intervals:
                if recovery_time < fail_time:
                    break
                remove_flag = True
                # combine the correlated failure with component failure
                if fail_time < failure_time <= recover_time:
                    failure_time = fail_time
                    remove_flag = False
                if fail_time < recovery_time <= recover_time:
                    recovery_time = recover_time
                    remove_flag = False
                if remove_flag:
                    self.addCorrelatedFailures(result_events, fail_time, recover_time, _bool)
                else:
                    self.failure_intervals.remove([fail_time, recover_time, _bool])

            for u in self.children:
                u.generateEvents(result_events, last_recover_time,
                                 failure_time, True)

            if recovery_time > end_time - (1E-5):
                recovery_time = end_time - (1E-5)

            r = random()
            if not self.fast_forward:  # we will process failures
                if r < Machine.fail_fraction:
                    # failure type: tempAndShort=1, tempAndLong=2, permanent=3
                    failure_type = 3

                    # generate disk failures
                    max_recovery_time = recovery_time
                    for u in self.children:
                        # ensure machine fails before disk
                        disk_fail_time = failure_time + 1E-5
                        disk_fail_event = Event(Event.EventType.Failure,
                                                disk_fail_time, u)
                        result_events.addEvent(disk_fail_event)
                        disk_recovery_time = u.generateRecoveryEvent(
                            result_events, disk_fail_time, end_time-(1E-5))
                        disk_fail_event.next_recovery_time = disk_recovery_time
                        # machine recovery must coincide with last disk recovery
                        if disk_recovery_time > max_recovery_time:
                            max_recovery_time = disk_recovery_time
                    recovery_time = max_recovery_time + (1E-5)
                else:
                    if recovery_time - failure_time <= self.fail_timeout:
                        # transient failure and come back very soon
                        failure_type = 1
                    else:
                        # transient failure, but last long.
                        failure_type = 2
                        if self.eager_recovery_enabled:
                            eager_recovery_start_time = failure_time + \
                                                        self.fail_timeout
                            eager_recovery_start_event = Event(
                                Event.EventType.EagerRecoveryStart,
                                eager_recovery_start_time, self)
                            eager_recovery_start_event.next_recovery_time = \
                                recovery_time
                            result_events.addEvent(eager_recovery_start_event)
                            # Ensure machine recovery happens after last eager
                            # recovery installment
                            recovery_time += 1E-5

            if isinstance(self.failure_generator, Trace):
                self.failure_generator.eventAccepted()

            if self.fast_forward:
                result_events.addEvent(Event(Event.EventType.Failure,
                                             failure_time, self, True))
                result_events.addEvent(Event(Event.EventType.Recovered,
                                             recovery_time, self, True))
            else:
                result_events.addEvent(Event(Event.EventType.Failure,
                                             failure_time, self, failure_type))
                result_events.addEvent(Event(Event.EventType.Recovered,
                                             recovery_time, self,
                                             failure_type))

            current_time = recovery_time
            last_recover_time = current_time
            if current_time >= end_time - (1E-5):
                break


    # def toString(self):
    #     full_name = super(Machine, self).toString()
    #     parts = full_name.split(".")
    #     return ".".join(parts[2:])
