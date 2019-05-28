from collections import OrderedDict
from math import sqrt
from random import randint
from copy import deepcopy

from simulator.Event import Event
from simulator.Result import Result
from simulator.EventHandler import EventHandler
from simulator.Configuration import Configuration
from simulator.unit.Rack import Rack
from simulator.unit.Machine import Machine
from simulator.unit.Disk import Disk
from simulator.unit.DiskWithScrubbing import DiskWithScrubbing
from simulator.unit.SliceSet import SliceSet
from simulator.utils.Log import info_logger, error_logger


class Recovery(object):

    def __init__(self, start, end, data_recovered):
        self.start = start
        self.end = end
        self.data_recovered = data_recovered

    def bandwidht(self):
        # scale it to GB/day
        return ((self.data_recovered/(self.end-self.start))*24/1024)


class RandomDistributeEventHandler(EventHandler):

    def __init__(self):
        self.conf = Configuration()
        self.n = self.conf.n
        self.k = self.conf.k
        self.num_chunks_diff_racks = self.conf.num_chunks_diff_racks
        self.lost_slice = -100
        # how to express Long.MAX_VALUE in python?
        self.min_av_count = 10000000000

        self.end_time = self.conf.total_time

        # A slice is recovered when recoveryThreshold number of chunks are
        # 'lost', where 'lost' can include durability events (disk failure,
        # latent failure), as well as availability events (temporary machine
        # failure) if availabilityCountsForRecovery is set to true (see below)
        # However, slice recovery can take two forms:
        # 1. If lazyRecovery is set to false: only the chunk that is in the
        # current disk being recovered, is recovered.
        # 2. If lazyRecovery is set to true: all chunks of this slice that are
        # known to be damaged, are recovered.
        self.lazy_recovery = self.conf.lazy_recovery
        self.recovery_threshold = self.conf.recovery_threshold

        # Lazy recovery threshold can be defined in one of two ways:
        #  1. a slice is recovered when some number of *durability* events
        #     happen
        #  2. a slice is recovered when some number of durability and/or
        #     availability events happen
        # where durability events include permanent machine failures, or disk
        # failures, while availabilty events are temporary machine failures
        # This parameter -- availabilityCountsForRecovery -- determines which
        # policy is followed. If true, then definition #2 is followed, else
        # definition #1 is followed.
        self.availability_counts_for_recovery = \
            self.conf.availability_counts_for_recovery

        self.available_count = []
        self.durable_count = []
        self.latent_defect = []
        self.known_latent_defect = []

        self.unavailable_slice_count = 0
        self.undurable_slice_count = 0

        # There is an anomaly (logical bug?) that is possible in the current
        # implementation:
        # If a machine A suffers a temporary failure at time t, and between t
        # and t+failTimeout, if a recovery event happens which affects a slice
        # which is also hosted on machine A, then that recovery event may
        # rebuild chunks of the slice that were made unavailable by machine
        # A's failure. This should not happen, as technically, machine A's
        # failure should not register as a failure until t+failTimeout.
        # This count -- anomalousAvailableCount -- keeps track of how many
        # times this happens
        self.anomalous_available_count = 0

        self.current_slice_degraded = 0
        self.current_avail_slice_degraded = 0

        self.recovery_bandwidth_cap = self.conf.recovery_bandwidth_gap
        # instantaneous total recovery b/w, in MB/hr, not to exceed above cap
        self.current_recovery_bandwidth = 0
        # max instantaneous recovery b/w, in MB/hr
        self.max_recovery_bandwidth = 0

        # current counter of years to print histogram
        self.snapshot_year = 1

        self.max_bw = 0
        self.bandwidth_list = OrderedDict()

        self.total_latent_failures = 0
        self.total_scrubs = 0
        self.total_scrub_repairs = 0
        self.total_disk_failures = 0
        self.total_disk_repairs = 0
        self.total_machine_failures = 0
        self.total_machine_repairs = 0
        self.total_perm_machine_failures = 0
        self.total_short_temp_machine_failures = 0
        self.total_long_temp_machine_failures = 0
        self.total_machine_failures_due_to_rack_failures = 0
        self.total_eager_machine_repairs = 0
        self.total_eager_slice_repairs = 0
        self.total_skipped_latent = 0
        self.total_incomplete_recovery_attempts = 0

        self.slice_locations = []
        self.slices_degraded_list = []
        self.slices_degraded_avail_list = []

        # unavailable statistic dict, {slice_index:[[start, end],...]}
        self.unavailable_durations = {}
        # degraded slice statistic dict
        self.slices_degraded_durations = {}

        self.cccccccccccccc = 0

    def _my_assert(self, expression):
        if not expression:
            raise Exception("My Assertion failed!")
        return True

    def computeReconstructionBandwidth(self, num_missing_blocks):
        if self.conf.bandwidth_efficient_scheme:
            if num_missing_blocks == 1:
                return float(self.k)/2*self.conf.chunk_size
        if self.conf.is_msr:
            d = self.conf.d
            if num_missing_blocks <= (self.n - d) and num_missing_blocks == 1:
                return (float(d)/(d - self.k + 1))*self.conf.chunk_size
        return self.conf.chunk_size*(self.k+num_missing_blocks-1)

    def putBandwidthList(self, key, r):
        if key in self.bandwidth_list.keys():
            ll = self.bandwidth_list[key]
        else:
            ll = []
            self.bandwidth_list[key] = ll
        ll.append(r)

    def addBandwidthStat(self, r):
        if self.max_bw < r.bandwidht():
            self.max_bw = r.bandwidht()
            # logging.info("Max bw now is:"+ self.max_bw)

        self.putBandwidthList(r.start, r)
        self.putBandwidthList(r.end, r)

    def analyzeBandwidth(self):
        current_bandwidth = 0
        tmp_bw_list = []
        while True:
            keys = self.bandwidth_list.keys()
            keys.sort()
            key = keys[0]
            rlist = self.bandwidth_list.pop(key)
            for r in rlist:
                start = (key == r.start)
                if start:
                    current_bandwidth += r.bandwidht()
                else:
                    current_bandwidth -= r.bandwidht()
            if current_bandwidth < 0 and current_bandwidth > -1:
                current_bandwidth = 0
            if current_bandwidth < 0:
                raise Exception("Negative bandwidth count")
            tmp_bw_list.append((key, current_bandwidth))

            # if bandwidth list is empty, end while
            if self.bandwidth_list == OrderedDict():
                break

        self.printDegradedStat(tmp_bw_list, "Avg_banwidth_", "GBPerday")

    def sliceRecovered(self, slice_index):
        if self.durable_count[slice_index] - \
           (1 if self.latent_defect[slice_index] else 0) == self.n:
            self.current_slice_degraded -= 1
        self.sliceRecoveredAvailability(slice_index)

    def sliceDegraded(self, slice_index):
        if self.durable_count[slice_index] - \
           (1 if self.latent_defect[slice_index] else 0) == self.n:
            self.current_slice_degraded += 1
        self.sliceDegradedAvailability(slice_index)

    def sliceRecoveredAvailability(self, slice_index):
        if self.k == 1:
            # replication is not affected by this
            return
        undurable = self.n - self.durable_count[slice_index] + \
            (1 if self.latent_defect[slice_index] else 0)
        unavailable = self.n - self.available_count[slice_index]
        if undurable == 0 and unavailable == 0:
            self.current_avail_slice_degraded -= 1
        else:
            if unavailable == 0:
                self.total_incomplete_recovery_attempts += 1

    def sliceDegradedAvailability(self, slice_index):
        if self.k == 1:
            # replication is not affected by this
            return
        undurable = self.n - self.durable_count[slice_index] + \
            (1 if self.latent_defect[slice_index] else 0)
        unavailable = self.n - self.available_count[slice_index]
        if undurable == 0 and unavailable == 0:
            self.current_avail_slice_degraded += 1

    # a slice start Unavailable
    def startUnavailable(self, slice_index, ts):
        slice_unavail_durations = self.unavailable_durations.pop(slice_index, [])
        if slice_unavail_durations == []:
            slice_unavail_durations.append([ts, None])
        else:
            if slice_unavail_durations[-1][1] is None:
                pass
            else:
                slice_unavail_durations.append([ts, None])
        self.unavailable_durations[slice_index] = slice_unavail_durations

    # a slice end Unavailable, the slice undurable or become available
    def endUnavailable(self, slice_index, ts):
        slice_unavail_durations = self.unavailable_durations.pop(slice_index, [])
        if slice_unavail_durations == []:
            slice_unavail_durations.append([ts, self.end_time])
        else:
            if (self.durable_count[slice_index] < self.k) or \
               (self.durable_count[slice_index] == self.k and self.latent_defect[slice_index]):
                if slice_unavail_durations[-1][1] is None:
                    slice_unavail_durations[-1][1] = self.end_time
                else:
                    slice_unavail_durations.append([ts, self.end_time])
            else:
                slice_unavail_durations[-1][1] = ts
        self.unavailable_durations[slice_index] = slice_unavail_durations

    def printPerYearStart(self, per_day_start, description):
        d = 0
        year = 365
        for t in xrange(1, len(per_day_start)):
            d += per_day_start[t]
            if t % year == 0:
                d /= 365
                info_logger.info(description + " " + str(t/year) + " " +
                                 str(d))
                d = 0
        info_logger.info(description + " " + str(len(per_day_start)/year) +
                         " " + str(d/365))

    def printDegradedStat(self, degraded, description, unit):
        current_sample_average = 0
        current_time = 0

        sampling_period = 24
        # sampling per min, so 24*60 items in below list
        values_per_sample = []
        samples = int(self.conf.total_time/24)
        if self.conf.total_time % 24 != 0:
            samples += 1
        samples += 1

        day_samples = [0] * samples
        previous_window_value = 0
        avg_of_avgs = 0
        avg_count = 0
        max_v = 0

        it = iter(degraded)
        try:
            t = it.next()
        except StopIteration:
            t = None

        while t is not None:
            values_per_sample = [0]*(24*60)
            for i in xrange(sampling_period*60):
                if t is None:
                    break
                per_sample_count = 0
                while True:
                    if t[0] > current_time+i/60:
                        per_sample_count = 0
                        values_per_sample[i] = previous_window_value
                        break
                    else:
                        values_per_sample[i] = (values_per_sample[i] *
                                                per_sample_count+t[1]) /\
                                               (per_sample_count+1)
                        previous_window_value = t[1]
                        per_sample_count += 1
                        try:
                            t = it.next()
                        except StopIteration:
                            t = None
                            break

            current_sample_average = 0
            for i in xrange(sampling_period*60):
                current_sample_average += values_per_sample[i]
                if max_v < values_per_sample[i]:
                    max_v = values_per_sample[i]
            current_sample_average /= (sampling_period*60)

            if int(current_time/24) >= samples:
                break
            day_samples[int(current_time/24)] = current_sample_average
            current_time += sampling_period
            avg_of_avgs += current_sample_average
            avg_count += 1

        avg_of_avgs /= avg_count
        stdev = 0.0
        for val in day_samples:
            stdev += (val - avg_of_avgs)*(val-avg_of_avgs)

        info_logger.info("%s_per_%dh_%s %d stdev:%f max:%d" %
                         (description, sampling_period, unit, avg_of_avgs,
                          sqrt(stdev/(len(day_samples)-1)), max_v))

        self.printPerYearStart(day_samples, description)

    def end(self):
        ret = Result()
        Result.unavailable_count = self.unavailable_slice_count
        Result.undurable_count = self.undurable_slice_count

        info_logger.info(
            "anomalous available count: %d, total latent failure: %d,\
             total scrubs: %d, total scrubs repairs: %d, \
             total disk failures:%d, total disk repairs:%d, \
             total machine failures:%d, total machine repairs:%d, \
             total permanent machine failures:%d, \
             total short temperary machine failures:%d, \
             total long temperary machine failures:%d, \
             total machine failures due to rack failures:%d, \
             total eager machine repairs:%d, total eager slice repairs:%d, \
             total skipped latent:%d, total incomplete recovery:%d\n \
             max recovery bandwidth:%f\n \
             unavailable_slice_count:%d, undurable_slice_count:%d\n \
             durability:%f%%" %
            (self.anomalous_available_count, self.total_latent_failures,
             self.total_scrubs, self.total_scrub_repairs,
             self.total_disk_failures, self.total_disk_repairs,
             self.total_machine_failures, self.total_machine_repairs,
             self.total_perm_machine_failures,
             self.total_short_temp_machine_failures,
             self.total_long_temp_machine_failures,
             self.total_machine_failures_due_to_rack_failures,
             self.total_eager_machine_repairs,
             self.total_eager_slice_repairs,
             self.total_skipped_latent,
             self.total_incomplete_recovery_attempts,
             self.max_recovery_bandwidth,
             self.unavailable_slice_count,
             self.undurable_slice_count,
             (1 - float(self.undurable_slice_count) /
              len(self.available_count))*100.0))

        self.printDegradedStat(self.slices_degraded_list,
                               "Avg_durable_degraded_", "slices")
        self.printDegradedStat(self.slices_degraded_avail_list,
                               "Avg_available_degraded_", "slices")

        self.analyzeBandwidth()

        return ret

    def handleEvent(self, e, queue):
        # print "********event info********"
        # print "event ID: ", e.event_id
        # print "event type: ", e.getType()
        # print "event unit: ", e.getUnit().getFullName()
        # print "event Time: ", e.getTime()
        # print "event next reovery time: ", e.next_recovery_time
        if e.getType() == Event.EventType.Failure:
            self.handleFailure(e.getUnit(), e.getTime(), e, queue)
        elif e.getType() == Event.EventType.Recovered:
            self.handleRecovery(e.getUnit(), e.getTime(), e)
        elif e.getType() == Event.EventType.EagerRecoveryStart:
            self.handleEagerRecoveryStart(e.getUnit(), e.getTime(), e, queue)
        elif e.getType() == Event.EventType.EagerRecoveryInstallment:
            self.handleEagerRecoveryInstallment(e.getUnit(), e.getTime(), e)
        elif e.getType() == Event.EventType.LatentDefect:
            self.handleLatentDefect(e.getUnit(), e.getTime(), e)
        elif e.getType() == Event.EventType.LatentRecovered:
            self.handleLatentRecovered(e.getUnit(), e.getTime(), e)
        elif e.getType() == Event.EventType.ScrubStart:
            self.handleScrubStart(e.getUnit(), e.getTime(), e)
        elif e.getType() == Event.EventType.ScrubComplete:
            self.handleScrubComplete(e.getUnit(), e.getTime(), e)
        else:
            raise Exception("Unknown event: " + e.getType())

    def computeHistogramBool(self, data, what):
        histogram = [0, 0]
        for i in len(data):
            histogram[1 if data[i] is True else 0] += 1
        print "what: " + what
        for i, item in enumerate(histogram):
            print i + "->" + item + " "
        return histogram[1]

    def computHistogram(self, data, max_val, what):
        histogram = [0]*max_val
        for i in len(data):
            histogram[data[i]] += 1
        print "what: " + what
        less_than_max = 0
        for i, item in enumerate(histogram):
            print i + "->" + item + " "
            if i < max_val:
                less_than_max += item

        return less_than_max

    def handleFailure(self, u, time, e, queue):
        if e.ignore:
            return

        if isinstance(u, Machine):
            self.total_machine_failures += 1
            u.setLastFailureTime(e.getTime())

            if e.info == 3:
                self.total_perm_machine_failures += 1
            else:
                if e.info == 1:
                    self.total_short_temp_machine_failures += 1
                elif e.info == 2:
                    self.total_long_temp_machine_failures += 1
                else:  # machine failure due to rack failure
                    self.total_machine_failures_due_to_rack_failures += 1
                    if e.next_recovery_time - e.getTime() <= u.fail_timeout:
                        self.total_short_temp_machine_failures += 1
                    else:
                        self.total_long_temp_machine_failures += 1
                        if u.eager_recovery_enabled:
                            eager_recovery_start_time = e.getTime() + \
                                u.fail_timeout
                            eager_recovery_start_event = Event(
                                Event.EventType.EagerRecoveryStart,
                                eager_recovery_start_time, u)
                            eager_recovery_start_event.next_recovery_time = \
                                e.next_recovery_time - (1E-5)
                            queue.addEvent(eager_recovery_start_event)

                for child in u.getChildren():
                    if child.getMetadata().slice_count == 0:
                        error_logger.error("lost machine failures")
                        continue
                    for i in xrange(child.getMetadata().slice_count):
                        slice_index = child.getMetadata().slices[i]
                        if self.durable_count[slice_index] == self.lost_slice:
                            continue
                        self.sliceDegradedAvailability(slice_index)
                        self.available_count[slice_index] -= 1
                        self._my_assert(self.available_count[slice_index] >= 0)
                        if self.available_count[slice_index] < self.k:
                            self.unavailable_slice_count += 1
                            self.startUnavailable(slice_index, time)

                self.slices_degraded_avail_list.append(
                    (e.getTime(), self.current_avail_slice_degraded))

        elif isinstance(u, Disk):
            self.total_disk_failures += 1
            u.setLastFailureTime(e.getTime())
            # need to compute projected reovery b/w needed
            projected_bandwidth_need = 0.0

            slice_count = u.getMetadata().slice_count
            for i in xrange(slice_count):
                slice_index = u.getMetadata().slices[i]
                if self.durable_count[slice_index] == self.lost_slice:
                    continue
                if (u.getMetadata().nonexistent_slices is not None) and \
                   (slice_index in u.getMetadata().nonexistent_slices):
                    continue

                self.sliceDegraded(slice_index)
                self.durable_count[slice_index] -= 1
                if u.getMetadata().nonexistent_slices is None:
                    u.getMetadata().nonexistent_slices = set()
                u.getMetadata().nonexistent_slices.add(slice_index)

                self._my_assert(self.durable_count[slice_index] >= 0)

                if (u.getMetadata().defective_slices is not None) and \
                   slice_index in u.getMetadata().defective_slices:
                    self.latent_defect[slice_index] = False
                if (u.getMetadata().known_defective_slices is not None) and \
                   slice_index in u.getMetadata().known_defective_slices:
                    self.known_latent_defect[slice_index] = False

                if self.durable_count[slice_index] < self.k:
                    info_logger.info(
                        "time: " + str(time) + " slice:" + str(slice_index) +
                        " durCount:" + str(self.durable_count[slice_index]) +
                        " latDefect:" + str(self.latent_defect[slice_index]) +
                        " due to disk " + str(u.getID()))
                    self.durable_count[slice_index] = self.lost_slice
                    self.undurable_slice_count += 1
                    self.endUnavailable(slice_index, time)
                    continue

                if self.durable_count[slice_index] == self.k and \
                   self.latent_defect[slice_index]:
                    info_logger.info(
                        "time: " + str(time) + " slice:" + str(slice_index) +
                        " durCount:" + str(self.durable_count[slice_index])
                        + " latDefect:" + str(self.latent_defect[slice_index])
                        + " due to latent error and disk " + str(u.getID()))
                    self.durable_count[slice_index] = self.lost_slice
                    self.undurable_slice_count += 1
                    self.endUnavailable(slice_index, time)

                # is this slice one that needs recovering? if so, how much
                # data to recover?
                if self.durable_count[slice_index] != self.lost_slice:
                    threshold_crossed = False
                    num_undurable = self.n - self.durable_count[slice_index]
                    if self.known_latent_defect[slice_index]:
                        num_undurable += 1
                    if num_undurable >= self.n - self.recovery_threshold:
                        threshold_crossed = True

                    num_unavailable = 0
                    if self.availability_counts_for_recovery:
                        num_unavailable = self.n - \
                            self.available_count[slice_index]
                        if num_unavailable + num_undurable >= self.n - \
                           self.recovery_threshold:
                            threshold_crossed = True
                    if threshold_crossed:
                        projected_bandwidth_need += \
                            self.computeReconstructionBandwidth(
                                num_undurable + num_unavailable)

            # current recovery bandwidth goes up by projected bandwidth need
            projected_bandwidth_need /= (e.next_recovery_time -
                                         e.getTime())
            u.setLastBandwidthNeed(projected_bandwidth_need)
            self._my_assert(self.current_recovery_bandwidth >= 0)
            self.current_recovery_bandwidth += projected_bandwidth_need
            self._my_assert(self.current_recovery_bandwidth >= 0)
            if self.current_recovery_bandwidth > self.max_recovery_bandwidth:
                self.max_recovery_bandwidth = self.current_recovery_bandwidth
            self._my_assert(self.current_recovery_bandwidth >= 0)
            u.getMetadata().defective_slice = None
            u.getMetadata().known_defective_slices = None

            self.slices_degraded_list.append((e.getTime(),
                                              self.current_slice_degraded))
            self.slices_degraded_avail_list.append(
                (e.getTime(), self.current_avail_slice_degraded))

        else:
            for tmp in u.getChildren():
                self.handleFailure(tmp, time, e, queue)

    def handleRecovery(self, u, time, e):
        if e.ignore:
            return

        if isinstance(u, Machine):
            self.total_machine_repairs += 1

            # this is a recovery from a temporary machine failure
            # recovery from temporary machine failure increments slice
            # availability, whereas recovery from permanent machine failure
            # improves slice durability. the former effect is simulated here,
            # while the latter effect is simulated in disk recoveries
            # generated by this permanent machine recovery
            if e.info != 3:
                for child in u.getChildren():
                    slice_count = child.getMetadata().slice_count
                    for i in xrange(slice_count):
                        slice_index = child.getMetadata().slices[i]
                        if self.durable_count[slice_index] == self.lost_slice:
                            continue

                        # We are going to check, using the 'info' field of the
                        # event, whether this was a temporary machine failure
                        # of short duration.
                        # If so, then all of the availabilityCounts should be
                        # less than n
                        # If they are not, then anomalousAvailableCount will
                        # be incremented
                        if self.available_count[slice_index] < self.n:
                            if self.available_count[slice_index] + self.durable_count[slice_index] == self.n + self.k - 1:
                                self.endUnavailable(slice_index, time)
                            self.available_count[slice_index] += 1
                            self.sliceRecoveredAvailability(slice_index)
                        elif e.info == 1:  # temp & short failure
                            self.anomalous_available_count += 1
                            # info_logger.info(
                            #   "anomalous available count: %d Machine
                            #    recovery: %s" %
                            #   (self.anomalous_available_count, e.toString())
                            # )
                        else:
                            pass

                self.slices_degraded_avail_list.append(
                    (e.getTime(), self.current_avail_slice_degraded))

        elif isinstance(u, Disk):
            self.total_disk_repairs += 1
            # this disk finished recovering, so decrement current recov b/w
            self.current_recovery_bandwidth -= u.getLastBandwidthNeed()
            if self.current_recovery_bandwidth > -1 and \
               self.current_recovery_bandwidth < 0:
                self.current_recovery_bandwidth = 0
            self._my_assert(self.current_recovery_bandwidth >= 0)

            transfer_required = 0.0
            slice_count = u.getMetadata().slice_count
            for i in xrange(slice_count):
                slice_index = u.getMetadata().slices[i]
                if self.durable_count[slice_index] == self.lost_slice:
                    continue

                threshold_crossed = False
                actual_threshold = self.recovery_threshold
                if self.conf.lazy_only_available:
                    actual_threshold = self.n - 1
                if (self.current_slice_degraded <
                    (self.conf.max_degraded_slices *
                     Configuration.total_slices)):
                    actual_threshold = self.recovery_threshold

                num_undurable = self.n - self.durable_count[slice_index]
                if self.known_latent_defect[slice_index]:
                    num_undurable += 1
                if num_undurable >= self.n - actual_threshold:
                    threshold_crossed = True

                if self.availability_counts_for_recovery:
                    num_unavailable = self.n - \
                        self.available_count[slice_index]
                    if num_unavailable + num_undurable >= self.n - \
                       actual_threshold:
                        threshold_crossed = True

                if threshold_crossed:
                    if self.lazy_recovery:
                        # recovery all replicas of this slice.
                        chunks_recovered = self.handleSliceRecovery(
                            slice_index, e, True)
                        if chunks_recovered > 0:
                            # transfer required for 1 chunk is k,for 2 is k+1,
                            # etc...
                            transfer_required += \
                                self.computeReconstructionBandwidth(
                                    chunks_recovered)
                            if self.durable_count[slice_index] + self.available_count[slice_index] >= self.n + self.k - chunks_recovered and self.durable_count[slice_index] + self.available_count[slice_index] <= self.n + self.k - 1:
                                self.endUnavailable(slice_index, time)
                    else:
                        if (u.getMetadata().nonexistent_slices is not None) \
                           and (slice_index in
                                u.getMetadata().nonexistent_slices):
                            u.getMetadata().nonexistent_slices.remove(
                                slice_index)
                            if self.durable_count[slice_index] + self.available_count[slice_index] == self.n + self.k - 1:
                                self.endUnavailable(slice_index, time)
                            self.durable_count[slice_index] += 1
                            transfer_required += \
                                self.computeReconstructionBandwidth(1)

                    # must come after all counters are updated
                    self.sliceRecovered(slice_index)

            self.slices_degraded_list.append(
                (e.getTime(), self.current_slice_degraded))
            self.slices_degraded_avail_list.append(
                (e.getTime(), self.current_avail_slice_degraded))

            self.addBandwidthStat(Recovery(u.getLastFailureTime(),
                                           e.getTime(), transfer_required))
        else:
            for tmp in u.getChildren():
                self.handleRecovery(tmp, time, e)

    def handleEagerRecoveryStart(self, u, time, e, queue):
        self._my_assert(isinstance(u, Machine))
        self.total_eager_machine_repairs += 1
        u.setLastFailureTime(e.getTime())
        original_failure_time = e.getTime()

        # Eager recovery begins now, and ends at time e.next_recovery_time
        # (which is when the machine recovers). Recovery rate will be
        # (recoveryBandwidthCap - currentRecoveryBandwidth) MB/hr. Therefore,
        # total number of chunks that can be recovered = eager recovery
        # duration * recovery rate. This happens in installments, of
        # installmentSize number of chunks each. The last installment will
        # have (total num chunks % installmentSize) number of chunks
        self._my_assert(e.next_recovery_time - e.getTime() > 0)
        self._my_assert(self.current_recovery_bandwidth >= 0)
        recovery_rate = self.recovery_bandwidth_cap - \
            self.current_recovery_bandwidth
        if self.recovery_rate:
            return

        num_chunks_to_recover = int((recovery_rate/self.conf.chunk_size) *
                                    (e.next_recovery_time-e.getTime()))
        if num_chunks_to_recover < 1:
            return

        recovery_rate = num_chunks_to_recover*self.conf.chunk_size / \
            (e.next_recovery_time-e.getTime())
        self._my_assert(recovery_rate >= 0)
        self.current_recovery_bandwidth += recovery_rate
        self._my_assert(self.current_recovery_bandwidth >= 0)
        if self.current_recovery_bandwidth > self.max_recovery_bandwidth:
            self.max_recovery_bandwidth = self.current_recovery_bandwidth

        curr_installment_size = self.conf.installment_size
        if num_chunks_to_recover < self.conf.installment_size:
            curr_installment_size = num_chunks_to_recover

        try:
            slice_installment = \
                SliceSet.forName("simulator.unit.SliceSet").newInstance()
            slice_installment.init("SliceSet-"+u.toString(), [])
            slice_installment.setLastFailureTime(u.getLastFailureTime())
            slice_installment.setOriginalFailureTime(original_failure_time)
        except Exception, e:
            error_logger.error("Error in eager recovery: " + e)
            return

        total_num_chunks_added_for_repair = 0
        num_chunks_added_to_curr_installment = 0
        curr_time = time
        for child in u.getChildren():
            slice_count = child.getMetadata().slice_count
            for i in xrange(slice_count):
                slice_index = child.getMetadata().slices[i]
                # When this machine failed, it decremented the availability
                # count of all its slices. This eager recovery is the first
                # point in time that this machine failure has been
                # 'recognized' by the system (since this is when the timeout
                # expires). So if at this point we find any of the
                # availability counts NOT less than n, then we need to count
                # it as an anomaly
                if self.available_count[slice_index] >= self.n:
                    self.anomalous_available_count += 1
                if self.durable_count[slice_index] == self.lost_slice:
                    continue

                threshold_crossed = False
                num_undurable = self.n - self.durable_count[slice_index]
                if self.known_latent_defect[slice_index]:
                    num_undurable += 1
                actual_threshold = self.recovery_threshold
                expected_recovery_time = curr_time + curr_installment_size * \
                    self.conf.chunk_size/recovery_rate
                actual_threshold = self.conf.getAvailableLazyThreshold(
                    expected_recovery_time -
                    slice_installment.getOriginalFailureTime())

                if num_undurable >= self.n - actual_threshold:
                    threshold_crossed = True

                num_unavailable = 0
                if self.availability_counts_for_recovery:
                    num_unavailable = self.n - \
                        self.available_count[slice_index]
                    if num_undurable + num_unavailable >= self.n - \
                       actual_threshold:
                        threshold_crossed = True

                if threshold_crossed:
                    slice_installment.slices.append(slice_index)
                    self.total_num_chunks_added_for_repair += self.k + \
                        num_unavailable - 1
                    num_chunks_added_to_curr_installment += self.k + \
                        num_unavailable - 1
                    if num_chunks_added_to_curr_installment >= \
                       curr_installment_size - self.k:
                        curr_time += num_chunks_added_to_curr_installment * \
                            self.conf.chunk_size/recovery_rate
                        queue.addEvent(
                            Event(Event.EventType.EagerRecoveryInstallment,
                                  curr_time, slice_installment, False))
                        if self.total_num_chunks_added_for_repair >= \
                           num_chunks_to_recover - self.k:
                            # the last installment must update recovery
                            # bandwidth
                            slice_installment.setLastBandwidthNeed(
                                recovery_rate)
                            return
                        curr_installment_size = self.conf.installment_size
                        if num_chunks_to_recover - \
                           total_num_chunks_added_for_repair < \
                           self.conf.installment_size:
                            curr_installment_size = num_chunks_to_recover - \
                                total_num_chunks_added_for_repair
                        try:
                            slice_installment = SliceSet.forName(
                                "simulator.unit.SliceSet").newInstance()
                            slice_installment.init("SliceSet-"+u.toString(),
                                                   [])
                            slice_installment.setLastFailureTime(curr_time)
                            slice_installment.setOriginalFailureTime(
                                original_failure_time)
                            slice_installment.setLastBandwidthNeed(-1)
                        except Exception, e:
                            # logging.error("Error in eager recovery: " + e)
                            return
                        num_chunks_added_to_curr_installment = 0

        # Arriving at this point in the code means number of slices added <
        # num_chunks_to_recover
        if len(slice_installment.slices) != 0:
            curr_time += num_chunks_added_to_curr_installment * \
                self.conf.chunk_size/recovery_rate
            slice_installment.setLastBandwidthNeed(recovery_rate)
            queue.addEvent(Event(Event.EventType.EagerRecoveryInstallment,
                                 curr_time, slice_installment, False))
            return

        # No slices were found for eager recovery, undo the current bandwidth
        # need.
        self.current_recovery_bandwidth -= recovery_rate
        self._my_assert(self.current_recovery_bandwidth >= 0)

    def handleEagerRecoveryInstallment(self, u, time, e):
        self._my_assert(isinstance(SliceSet, u))
        transfer_required = 0.0
        if u.getLastBandwidthNeed() != -1:
            self.current_recovery_bandwidth -= u.getLastBandwidthNeed()
            if self.current_recovery_bandwidth < 0 and \
               self.current_recovery_bandwidth > -1:
                self.current_recovery_bandwidth = 0
                self._my_assert(self.current_recovery_bandwidth >= 0)

            for s in u.slices:
                slice_index = s.intValue()
                if self.durable_count[slice_index] == self.lost_slice:
                    continue

                threshold_crossed = False
                num_undurable = self.n - self.undurable_count[slice_index]
                if self.known_latent_defect[slice_index]:
                    num_undurable += 1
                actual_threshold = self.recovery_threshold
                # need uc = u?
                actual_threshold = self.conf.getAvailableLazyThreshold(
                    e.getTime() - u.getOriginalFailureTime())

                if num_undurable >= self.n - actual_threshold:
                    threshold_crossed = True

                if self.availability_counts_for_recovery:
                    num_unavailable = self.n - \
                        self.available_count[slice_index]
                    if num_undurable + num_unavailable >= self.n - \
                       actual_threshold:
                        threshold_crossed = True

                if threshold_crossed:
                    self.total_eager_slice_repairs += 1
                    if self.lazy_recovery:
                        chunks_recovered = self.handleSliceRecovery(
                            slice_index, e, False)
                        self._my_assert(self.available_count[slice_index] ==
                                        self.n and
                                        self.durable_count[slice_index] ==
                                        self.n)
                        if num_undurable != 0:
                            self.sliceRecovered(slice_index)
                        else:
                            self.sliceRecoveredAvailability(slice_index)
                        transfer_required += \
                            self.computeReconstructionBandwidth(
                                chunks_recovered)
                    else:
                        if self.available_count[slice_index] < self.n:
                            if self.available_count[slice_index] + self.durable_count[slice_index] == self.n + self.k - 1:
                                self.endUnavailable(slice_index, time)
                            self.available_count[slice_index] += 1
                            transfer_required += \
                                self.computeReconstructionBandwidth(1)
                            if num_undurable != 0:
                                self.sliceRecovered(slice_index)
                            else:
                                self.sliceRecoveredAvailability(slice_index)
            self.slices_degraded_list.append((e.getTime(),
                                              self.current_slice_degraded))
            self.slices_degraded_avail_list.append(
                (e.getTime(), self.current_avail_slice_degraded))
            self.addBandwidthStat(Recovery(u.getLastFailureTime(),
                                           e.getTime(), transfer_required))

            u.setLastFailureTime(e.getTime())

    def handleLatentDefect(self, u, time, e):
        if isinstance(u, Disk):
            if u.getMetadata().slice_count == 0:
                return
            self._my_assert(u.getMetadata().slice_count > 10)

            index = randint(0, self.conf.chunks_per_disk-1)
            if index >= u.getMetadata().slice_count:
                self.total_skipped_latent += 1
                return
            slice_index = u.getMetadata().slices[index]

            if self.durable_count[slice_index] == self.lost_slice:
                self.total_skipped_latent += 1
                return
            if (u.getMetadata().nonexistent_slices is not None) and \
               (slice_index in u.getMetadata().nonexistent_slices):
                self.total_skipped_latent += 1
                return

            # A latent defect cannot hit replicas of the same slice multiple
            # times.
            if self.latent_defect[slice_index]:
                self.total_skipped_latent += 1
                return

            self._my_assert(self.durable_count[slice_index] >= 0)
            self.sliceDegraded(slice_index)

            self.latent_defect[slice_index] = True
            self.total_latent_failures += 1

            # Maybe this if can be deleted.
            if u.getMetadata().defective_slices is None:
                u.getMetadata().defective_slices = set()
            u.getMetadata().defective_slices.add(slice_index)

            if self.durable_count[slice_index] == self.k and \
               self.latent_defect[slice_index]:
                info_logger.info(
                    str(time) + " slice: " + str(slice_index) +
                    " durCount: " + str(self.durable_count[slice_index]) +
                    " latDefect " + str(self.latent_defect[slice_index]) +
                    "  due to ===latent=== error " + " on disk " +
                    str(u.getID()))
                self.undurable_slice_count += 1
                self.endUnavailable(slice_index, time)
                self.durable_count[slice_index] = self.lost_slice
                u.getMetadata().defective_slices.remove(slice_index)
        else:
            raise Exception("Latent defect should only happen for disk")

        self.slices_degraded_list.append(
            (e.getTime(), self.current_slice_degraded))
        self.slices_degraded_avail_list.append(
            (e.getTime(), self.current_avail_slice_degraded))

    def handleLatentRecovered(self, u, time, e):
        transfer_required = 0.0
        if isinstance(u, Disk):
            self.total_scrubs += 1
            if u.getMetadata().defective_slices is None:
                return
            u.getMetadata().known_defective_slices = \
                deepcopy(u.getMetadata().defective_slices)
            for slice_index in u.getMetadata().known_defective_slices:
                self.known_latent_defect[slice_index] = True
            if u.getMetadata().known_defective_slices is None:
                return
            for slice_index in u.getMetadata().known_defective_slices:
                self.total_scrub_repairs += 1
                self.latent_defect[slice_index] = False
                self.known_latent_defect[slice_index] = False
                transfer_required += self.computeReconstructionBandwidth(1)
                self.sliceRecovered(slice_index)

            u.getMetadata().defective_slices = None
            u.getMetadata().known_defective_slices = None
        else:
            raise Exception("Latent Recovered should only happen for disk")
        self.slices_degraded_list.append(
            (e.getTime(), self.current_slice_degraded))
        self.slices_degraded_avail_list.append(
            (e.getTime(), self.current_avail_slice_degraded))
        self.addBandwidthStat(
            Recovery(u.getLastScrubStart(), e.getTime(), transfer_required))

    def handleScrubStart(self, u, time, e):
        if isinstance(u, Disk):
            u.setLastScrubStart(time)
            self.total_scrubs += 1
            if u.getMetadata().defective_slices is None:
                return
            u.getMetadata().known_defective_slices = \
                deepcopy(u.getMetadata().defective_slices)
            for slice_index in u.getMetadata().known_defective_slices:
                self.known_latent_defect[slice_index] = True
        else:
            raise Exception("Scrub start should only happen for disk")

    def handleScrubComplete(self, u, time, e):
        transfer_required = 0.0
        if isinstance(u, Disk):
            if u.getMetadata().known_defective_slices is None:
                return
            for slice_index in u.getMetadata().known_defective_slices:
                self.total_scrub_repairs += 1
                self.latent_defect[slice_index] = False
                self.known_latent_defect[slice_index] = False
                transfer_required += self.computeReconstructionBandwidth(1)
                self.sliceRecovered(slice_index)

            u.getMetadata().defective_slices = None
            u.getMetadata().known_defective_slices = None
        else:
            raise Exception("Scrub complete should only happen for disk")
        self.slices_degraded_list.append(
            (e.getTime(), self.current_slice_degraded))
        self.slices_degraded_avail_list.append(
            (e.getTime(), self.current_avail_slice_degraded))
        self.addBandwidthStat(
            Recovery(u.getLastScrubStart(), e.getTime(), transfer_required))

    def handleSliceRecovery(self, slice_index, e, is_durable_failure):
        if self.durable_count[slice_index] == self.lost_slice:
            return 0

        recovered = 0
        for i in xrange(self.n):
            disk = self.slice_locations[slice_index][i]
            if (disk.getMetadata().known_defective_slices is not None) and \
               (slice_index in disk.getMetadata().known_defective_slices):
                self.latent_defect[slice_index] = False
                self.known_latent_defect[slice_index] = False
                disk.getMetadata().defective_slices.remove(slice_index)
                disk.getMetadata().known_defective_slices.remove(slice_index)
                recovered += 1
            if (disk.getMetadata().nonexistent_slices is not None) and \
               (slice_index in disk.getMetadata().nonexistent_slices):
                self.durable_count[slice_index] += 1
                disk.getMetadata().nonexistent_slices.remove(slice_index)
                recovered += 1
        self._my_assert((not self.known_latent_defect[slice_index]) and
                        (self.durable_count[slice_index] == self.n))

        if self.availability_counts_for_recovery and \
           (not is_durable_failure or not self.conf.lazy_only_available):
            recovered += self.n - self.available_count[slice_index]
            self.available_count[slice_index] = self.n

        return recovered

    def start(self, root, total_slices, disk_count):
        self.distributeSlices(root, total_slices, disk_count)

    def distributeSlices(self, root, total_slices, disk_count):
        disks = []
        self.slice_locations = []
        self.available_count = [0] * total_slices
        self.durable_count = [0] * total_slices
        self.latent_defect = [None] * total_slices
        self.known_latent_defect = [None] * total_slices

        self.getAllDisks(root, disks)
        for i in xrange(total_slices):
            self.slice_locations.append([])
            tmp_racks = [item for item in disks]
            for j in xrange(self.n):
                if j < self.num_chunks_diff_racks:
                    self.distributeOneSliceToOneDisk(i, disks, tmp_racks, True)
                else:
                    self.distributeOneSliceToOneDisk(i, disks, tmp_racks,
                                                     False)

            # print "tmp_racks:%d, rack_count:%d, n:%d" % (len(tmp_racks), Configuration.rack_count, self.n)
            self._my_assert(len(tmp_racks) == (Configuration.rack_count -
                                               self.n))
            self._my_assert(len(self.slice_locations[i]) == self.n)
            self.available_count[i] = self.n
            self.durable_count[i] = self.n

        self._my_assert(len(self.slice_locations) == total_slices)

    def distributeOneSliceToOneDisk(self, slice_index, disks, available_racks,
                                    separate_racks):
        retry_count = 0
        same_rack_count = 0
        same_disk_count = 0
        full_disk_count = 0
        while True:
            retry_count += 1
            # choose disk from the right rack
            if len(available_racks) == 0:
                raise Exception("No racks left")
            prev_racks_index = randint(0, len(available_racks)-1)
            rack_disks = available_racks[prev_racks_index]

            disk_index_in_rack = randint(0, len(rack_disks)-1)
            disk = rack_disks[disk_index_in_rack]
            if disk.getMetadata().slice_count >= self.conf.chunks_per_disk:
                full_disk_count += 1
                rack_disks.remove(disk)

                if len(rack_disks) == 0:
                    error_logger.error(
                        "One rack is completely full " +
                        str(disk.getParent().getParent().getID()))
                    available_racks.remove(rack_disks)
                    disks.remove(rack_disks)
                if retry_count > 100:
                    error_logger.error(
                        "Unable to distribute slice " +
                        str(slice_index) + "; picked full disk " +
                        str(full_disk_count) + " times, same rack " +
                        str(same_rack_count) + " times, and same disk " +
                        str(same_disk_count) + " times")
                    raise Exception("Disk distribution failed")

                continue

            available_racks.remove(rack_disks)

            m = disk.getMetadata()
            if m.slices == []:
                m.slices = [-10] * self.conf.chunks_per_disk

            # LZR
            self.slice_locations[slice_index].append(disk)
            m.slices[m.slice_count] = slice_index
            m.slice_count += 1
            break

    def getAllDisks(self, u, disks):
        for tmp in u.getChildren():
            if isinstance(tmp, Rack):
                rack_disks = []
                self.getAllDisksInRack(tmp, rack_disks)
                disks.append(rack_disks)
            else:
                self.getAllDisks(tmp, disks)

    def getAllDisksInRack(self, u, disks):
        for tmp in u.getChildren():
            for m in tmp.getChildren():
                disks.append(m)


if __name__ == "__main__":
    pass
