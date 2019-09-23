from numpy import isnan, isinf, ceil

from simulator.Event import Event
from simulator.unit.Disk import Disk


class DiskWithScrubbing(Disk):

    def __init__(self, name, parent, parameters):
        super(DiskWithScrubbing, self).__init__(name, parent, parameters)
        self.last_recovery_time = 0.0
        self.last_scrub_start = 0.0
        # Every 2 weeks = 336 hours scan the whole system.
        self.scan_period = 336

    def setLastScrubStart(self, last_scrub_start):
        self.last_scrub_start = last_scrub_start

    def getLastScrubStart(self):
        return self.last_scrub_start

    def addEventGenerator(self, generator):
        if generator.getName() == "latentErrorGenerator":
            self.latent_error_generator = generator
        elif generator.getName() == "scrubGenerator":
            self.scrub_generator = generator
        else:
            super(DiskWithScrubbing, self).addEventGenerator(generator)

    def getEventGenerators(self):
        return [self.failure_generator, self.recovery_generator, self.latent_error_generator, self.scrub_generator]

    def generateEvents(self, result_events, start_time, end_time, reset):
        if start_time < self.start_time:
            start_time = self.start_time
        if isnan(start_time) or isinf(start_time):
            raise Exception("start_time = Inf or NAN")
        if isnan(end_time) or isinf(end_time):
            raise Exception("end_time = Inf or NAN")

        current_time = start_time

        if start_time == 0:
            self.last_recovery_time = 0
            self.latent_error_generator.reset(0)

        while True:
            if self.last_recovery_time < 0:
                raise Exception("Negative last recover time")

            # The loop below is what makes the difference for avoiding weird
            # amplification of failures when having machine failures.
            # The reason is as follows: when generateEvents is called once for
            # the whole duration of the simulation(as when there are no
            # machine failures), this loop will never be executed. But when
            # machine fail, the function is called for the time interval
            # between machine recovery and second failure. The first time
            # the disk failure event generated, it may occur after the machine
            # failure event, so it is discarded when it is called for the next
            # time interval, the new failure event might be generated, to be
            # before the current start of the current interval. It's tempting
            # to round that event to the start of the interval, but then it
            # occurs concurrently to many disks. So the critical addition is
            # this loop, which effectively forces the proper generation of the
            # event, which is consistent with the previously generated one that
            # was discarded.
            failure_time = 0
            failure_time = self.failure_generator.generateNextEvent(
                self.last_recovery_time)
            while failure_time < start_time:
                failure_time = self.failure_generator.generateNextEvent(
                    self.last_recovery_time)

            if failure_time > end_time:
                for [fail_time, recover_time, flag] in self.failure_intervals:
                    self.addCorrelatedFailures(result_events, fail_time, recover_time, flag)
                if self.latent_error_generator is None:
                    break
                self.generateLatentErrors(result_events, current_time,
                                          end_time)
                # self.generateScrub(result_events, current_time, end_time)
                break

            if failure_time < start_time or failure_time > end_time:
                raise Exception("Wrong time range.")

            recovery_time = self.generateRecoveryEvent(result_events,
                                                       failure_time, end_time)
            if recovery_time < 0:
                raise Exception("recovery time is negative")

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

            fail_event = Event(Event.EventType.Failure, failure_time, self)
            result_events.addEvent(fail_event)

            fail_event.next_recovery_time = recovery_time

            # generate latent errors from the current time to the time of the
            # generated failure.
            self.generateLatentErrors(result_events, current_time,
                                      failure_time)

            # lifetime of a latent error starts when the disk is reconstructed
            self.latent_error_generator.reset(recovery_time)

            # scrubs get generated depending on the scrub frequency, starting
            # from the previous scrub finish event.
            # self.generateScrub(result_events, current_time, failure_time)

            # scrub generator is reset on the next recovery from the disk error
            # self.scrub_generator.reset(self.last_recovery_time)

            # move the clocks, next iteration starts from the next recovery
            current_time = self.last_recovery_time
            if current_time < 0:
                raise Exception("current recovery time is negative")

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
        if isinf(start_time) or isnan(start_time):
            raise Exception("start time = Inf or NAN")
        if isinf(end_time) or isnan(end_time):
            raise Exception("end time = Inf or NaN")

        current_time = start_time
        while True:
            latent_error_time = self.latent_error_generator.generateNextEvent(
                current_time)
            if isinf(latent_error_time):
                break
            if isinf(current_time) or isnan(current_time):
                raise Exception("current time is infinitiy or -infinitiy")
            if isinf(latent_error_time) or isnan(latent_error_time):
                raise Exception("current time is infinitiy or -infinitiy")

            LSE_in_CFI = False
            for [fail_time, recover_time, _bool] in self.failure_intervals:
                if fail_time <= latent_error_time <= recover_time:
                    LSE_in_CFI = True
            current_time = latent_error_time
            if current_time > end_time or LSE_in_CFI:
                break
            e = Event(Event.EventType.LatentDefect, current_time, self)
            result_events.addEvent(e)
            latent_recovery_time = ceil(current_time/self.scan_period)*self.scan_period + self.chunk_repair_time
            e.next_recovery_time = latent_recovery_time
            if latent_recovery_time >= end_time:
                break
            recovery_e = Event(Event.EventType.LatentRecovered, latent_recovery_time, self)
            result_events.addEvent(recovery_e)

    def generateScrub(self, result_events, start_time, end_time):
        if isinf(start_time) or isnan(start_time):
            raise Exception("start time = Inf or NAN")
        if isinf(end_time) or isnan(end_time):
            raise Exception("end time = Inf or NaN")

        current_time = start_time
        while True:
            scrub_time = self.scrub_generator.generateNextEvent(current_time)
            # if scrub time is later than end time, it will be regenerated
            # next time
            if scrub_time > end_time:
                break

            # scrub time could be earlier than current time
            assert (scrub_time >= start_time)

            scrub_end = Event(Event.EventType.ScrubComplete, scrub_time, self)
            result_events.addEvent(scrub_end)

            scrub_start = Event(Event.EventType.ScrubStart, scrub_time+1e-5,
                                self)
            result_events.addEvent(scrub_start)
            current_time = scrub_time
            self.scrub_generator.reset(current_time)

