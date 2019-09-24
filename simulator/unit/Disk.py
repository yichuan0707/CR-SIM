from simulator.unit.Unit import Unit
from numpy import isnan, isinf
from simulator.Event import Event
from simulator.Configuration import Configuration


class Disk(Unit):

    def __init__(self, name, parent, parameters):
        super(Disk, self).__init__(name, parent, parameters)
        conf = Configuration()
        self.disk_capacity = conf.disk_capacity
        self.disk_repair_time = conf.disk_repair_time
        self.chunk_repair_time = conf.chunk_repair_time
        self.slices_hit_by_LSE = []
        self.latent_error_generator = None
        self.scrub_generator = None

    def setDiskCapacity(self, disk_capacity):
        self.disk_capacity = disk_capacity

    def getDiskCapacity(self):
        return self.disk_capacity

    def getSlicesHitByLSE(self):
        return self.slices_hit_by_LSE

    def addEventGenerator(self, generator):
        if generator.getName() == "latentErrorGenerator":
            self.latent_error_generator = generator
        else:
            super(Disk, self).addEventGenerator(generator)

    def generateEvents(self, result_events, start_time, end_time, reset):
        if start_time < self.start_time:
            start_time = self.start_time
        current_time = start_time
        last_recover_time = start_time

        while True:
            self.failure_generator.reset(current_time)
            failure_time = self.failure_generator.generateNextEvent(
                current_time)
            current_time = failure_time
            if current_time > end_time:
                for [fail_time, recover_time, flag] in self.failure_intervals:
                    self.addCorrelatedFailures(result_events, fail_time, recover_time, flag)
                if self.latent_error_generator is None:
                    break
                self.generateLatentErrors(result_events, last_recover_time,
                                          end_time)
                break

            self.recovery_generator.reset(current_time)
            recovery_time = self.recovery_generator.generateNextEvent(
                current_time)
            assert (recovery_time > failure_time)
            # for disk repair, detection and identification have been given by recovery generator, so we add data transferring time here.
            recovery_time += self.disk_repair_time

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
                    disk_fail_event = Event(Event.EventType.Failure, fail_time, self)
                    disk_fail_event.next_recovery_time = recover_time
                    result_events.addEvent(disk_fail_event)
                    result_events.addEvent(Event(Event.EventType.Recovered, recover_time, self))
                self.failure_intervals.remove([fail_time, recover_time, _bool])

            current_time = failure_time
            fail_event = Event(Event.EventType.Failure, current_time, self)
            result_events.addEvent(fail_event)

            if self.latent_error_generator is not None:
                self.generateLatentErrors(result_events, last_recover_time,
                                          current_time)
            fail_event.next_recovery_time = recovery_time
            current_time = recovery_time
            if current_time > end_time:
                result_events.addEvent(Event(Event.EventType.Recovered,
                                             current_time, self))
                break
            result_events.addEvent(Event(Event.EventType.Recovered,
                                         current_time, self))
            last_recover_time = current_time

    def generateRecoveryEvent(self, result_events, failure_time, end_time):
        if end_time < 0 or failure_time < 0:
            raise Exception("end time or failure time is negative")
        if isinf(failure_time) or isnan(failure_time):
            raise Exception("start time = Inf or NAN")
        if isinf(end_time) or isnan(end_time):
            raise Exception("end time = Inf or NaN")

        self.recovery_generator.reset(failure_time)
        recovery_time = self.recovery_generator.generateNextEvent(failure_time)

        # if recovery falls in one correlated failure interval, combines it with
        # this interval
        for [fail_time, recover_time, _bool] in self.failure_intervals:
            if fail_time <= recovery_time <= recover_time:
                recovery_time = recover_time

        # if recovery falls later than the end time (which is the time of the
        # next failure of the higher-level component we just co-locate the
        # recovery with the failure because the data will remain unavailable
        # in either case)
        if recovery_time > end_time:
            recovery_time = end_time
        self.last_recovery_time = recovery_time
        if self.last_recovery_time < 0:
            raise Exception("recovery time is negative")
        result_events.addEvent(Event(Event.EventType.Recovered, recovery_time,
                                     self))
        return recovery_time

    def generateLatentErrors(self, result_events, start_time, end_time):
        self.latent_error_generator.reset(start_time)
        current_time = start_time
        while True:
            latent_error_time = self.latent_error_generator.generateNextEvent(
                current_time)
            current_time = latent_error_time
            if current_time > end_time:
                break
            result_events.addEvent(Event(Event.EventType.LatentDefect,
                                         current_time, self))

    # def toString(self):
    #     full_name = super(Disk, self).toString()
    #     parts = full_name.split(".")
    #     return ".".join(parts[2:])
