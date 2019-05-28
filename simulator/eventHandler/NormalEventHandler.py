from collections import OrderedDict
from math import sqrt, ceil
from random import randint, choice
from copy import deepcopy

from simulator.Event import Event
from simulator.Result import Result
from simulator.Metadata import Metadata
from simulator.Log import info_logger, error_logger
from simulator.unit.Rack import Rack
from simulator.unit.Machine import Machine
from simulator.unit.Disk import Disk
from simulator.unit.DiskWithScrubbing import DiskWithScrubbing


class Recovery(object):

    def __init__(self, start, end, data_recovered):
        self.start = start
        self.end = end
        self.data_recovered = data_recovered

    def bandwidht(self):
        # scale it to GB/day
        return ((self.data_recovered/(self.end-self.start))*24/1024)


class ManualNormalEventHandler(object):
    """
    Data recovery will not be executed until new disks or nodes join the system.
    TTR: time-to-repair

    Repair Time = TTR(failed component) + data transfer time
    """

    def __init__(self, distributer):
        self.distributer = distributer
        self.conf = self.distributer.returnConf()
        self.drs_handler = self.conf.DRSHandler()
        self.n, self.k = self.distributer.returnCodingParameters()
        self.slice_locations = self.distributer.returnSliceLocations()

        self.num_chunks_diff_racks = self.conf.num_chunks_diff_racks
        self.lost_slice = -100
        # how to express Long.MAX_VALUE in python?
        self.min_av_count = 10000000000

        self.end_time = self.conf.total_time
        self.total_slices_table = self.conf.tableForTotalSlice()
        # the final total slices
        self.total_slices = self.total_slices_table[-1][2]

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

        self.available_status = [[1 for i in xrange(self.n)] for j in xrange(self.total_slices)]
        self.durable_status = [[1 for i in xrange(self.n)] for j in xrange(self.total_slices)]
        self.latent_defect = [None] * self.total_slices
        self.known_latent_defect = [None] * self.total_slices

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

        self.total_repairs = 0
        self.total_repair_transfers = 0
        self.total_optimal_repairs = 0

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

    def durableCount(self, slice_index):
        if isinstance(self.durable_status[slice_index], int):
            return self.durable_status[slice_index]
        else:
            return self.durable_status[slice_index].count(1)

    def availableCount(self, slice_index):
        if isinstance(self.available_status[slice_index], int):
            return self.available_status[slice_index]
        else:
            return self.available_status[slice_index].count(1)

    def sliceRecovered(self, slice_index):
        if self.durableCount(slice_index) == self.n:
            # - (1 if self.latent_defect[slice_index] else 0) == self.n:
            self.current_slice_degraded -= 1
        self.sliceRecoveredAvailability(slice_index)

    def sliceDegraded(self, slice_index):
        if self.durableCount(slice_index) == self.n:
            # - (1 if self.latent_defect[slice_index] else 0) == self.n:
            self.current_slice_degraded += 1
        self.sliceDegradedAvailability(slice_index)

    def sliceRecoveredAvailability(self, slice_index):
        if self.k == 1:
            # replication is not affected by this
            return
        undurable = self.n - self.durableCount(slice_index) # + (1 if self.latent_defect[slice_index] else 0)
        unavailable = self.n - self.availableCount(slice_index)
        if undurable == 0 and unavailable == 0:
            self.current_avail_slice_degraded -= 1
        else:
            if unavailable == 0:
                self.total_incomplete_recovery_attempts += 1

    def sliceDegradedAvailability(self, slice_index):
        if self.k == 1:
            # replication is not affected by this
            return
        undurable = self.n - self.durableCount(slice_index) #  + (1 if self.latent_defect[slice_index] else 0)
        unavailable = self.n - self.availableCount(slice_index)
        if undurable == 0 and unavailable == 0:
            self.current_avail_slice_degraded += 1

    def startUnavailable(self, slice_index, ts):
        slice_unavailable_durations = self.unavailable_durations.pop(slice_index, [])
        if slice_unavailable_durations == []:
            slice_unavailable_durations.append([ts, None])
        else:
            if slice_unavailable_durations[-1][1] is None:
                pass
            else:
                slice_unavailable_durations.append([ts, None])
        self.unavailable_durations[slice_index] = slice_unavailable_durations

    def endUnavailable(self, slice_index, ts):
        slice_unavailable_durations = self.unavailable_durations.pop(slice_index, [])
        if slice_unavailable_durations == []:
            slice_unavailable_durations.append([ts, self.end_time])
        else:
            if not self.drs_handler.isRepairable(self.durable_status[slice_index]):
               # (self.durableCount(slice_index) < self.k) or (self.durableCount(slice_index) == self.k and self.latent_defect[slice_index]):
                if slice_unavailable_durations[-1][1] is None:
                    slice_unavailable_durations[-1][1] = self.end_time
                else:
                    slice_unavailable_durations.append([ts, self.end_time])
            else:
                slice_unavailable_durations[-1][1] = ts
        self.unavailable_durations[slice_index] = slice_unavailable_durations

    def getCurrentState(self, slice_index):
        available_state = self.available_status[slice_index]
        durable_state = self.durable_status[slice_index]

        if isinstance(available_state, int) or isinstance(durable_state, int):
            return [0]*self.n

        state = []
        for i in xrange(self.n):
            if available_state[i] and durable_state[i]:
                state.append(1)
            else:
                state.append(0)
        return state

    def repairCost(self, slice_index, repaired_index):
        rc = self.drs_handler.stateRepairCost(self.getCurrentState(slice_index), repaired_index)
        if rc < self.drs_handler.RC:
            self.total_optimal_repairs += 1

        return rc * self.conf.chunk_size

    def parallelRepairCost(self, slice_index):
        rc = self.drs_handler.stateParallRepairCost(self.getCurrentState(slice_index))
        return rc * self.conf.chunk_size

    def isRepairable(self, slice_index):
        return self.drs_handler.isRepairable(self.getCurrentState(slice_index))

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

    # calculate the data unavailable probability
    def calUnavailProb(self):
        total_slices_durations = 0.0
        for item in self.total_slices_table:
            total_slices_durations += item[2] * (item[1] - item[0])
            if item[-1]:
                total_slices_durations += pow(item[1]-item[0], 2) * item[-1]/2

        total_unavailable_durations = 0.0
        slice_indexes = self.unavailable_durations.keys()
        for slice_index in slice_indexes:
            for duration in self.unavailable_durations[slice_index]:
                total_unavailable_durations += duration[1] - duration[0]

        return format(total_unavailable_durations/total_slices_durations, ".4e")

    # calculate current total slices to cope with system scaling.
    def calCurrentTotalSlices(self, ts):
        if len(self.total_slices_table) == 1:
            return self.total_slices

        for [s_time, end_time, count, rate] in self.total_slices_table:
            if s_time <= ts <= end_time:
                return int(ceil(count + rate*(ts - s_time)))

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

    def handleFailure(self, u, time, e, queue):
        if e.ignore:
            return

        current_total_slices = self.calCurrentTotalSlices(time)
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
                else:
                    self.total_machine_failures_due_to_rack_failures += 1
                    if e.next_recovery_time - e.getTime() <= u.fail_timeout:
                        self.total_short_temp_machine_failures += 1
                    else:
                        self.total_long_temp_machine_failures += 1

                disks = u.getChildren()
                for child in disks:
                    # if child.getMetadata().slice_count == 0:
                    #     error_logger.error("lost machine failures")
                    #     continue

                    slice_indexes = child.getChildren()
                    for slice_index in slice_indexes:
                        if slice_index >= current_total_slices:
                            continue
                        if self.durable_status[slice_index] == self.lost_slice:
                            continue
                        self.sliceDegradedAvailability(slice_index)
                        index = self.slice_locations[slice_index].index(child)
                        self.available_status[slice_index][index] = 0
                        self._my_assert(self.availableCount(slice_index) >= 0)

                        # undurable blocks also effect the repairable result.
                        if not self.isRepairable(slice_index):
                        # if not self.drs_handler.isRepairable(self.available_status[slice_index]):
                            self.unavailable_slice_count += 1
                            self.startUnavailable(slice_index, time)

                self.slices_degraded_avail_list.append((e.getTime(), self.current_avail_slice_degraded))

        elif isinstance(u, Disk):
            self.total_disk_failures += 1
            u.setLastFailureTime(e.getTime())
            # need to compute projected reovery b/w needed
            projected_bandwidth_need = 0.0

            slice_indexes = u.getChildren()
            for slice_index in slice_indexes:
                if slice_index >= current_total_slices:
                    continue
                if self.durableCount(slice_index) == self.lost_slice:
                    continue
                if (u.getMetadata().nonexistent_slices is not None) and \
                   (slice_index in u.getMetadata().nonexistent_slices):
                    continue

                self.sliceDegraded(slice_index)
                index = self.slice_locations[slice_index].index(u)
                self.durable_status[slice_index][index] = 0

                if u.getMetadata().nonexistent_slices is None:
                    u.getMetadata().nonexistent_slices = set()
                u.getMetadata().nonexistent_slices.add(slice_index)

                self._my_assert(self.durableCount(slice_index) >= 0)

                if (u.getMetadata().defective_slices is not None) and \
                   slice_index in u.getMetadata().defective_slices:
                    self.latent_defect[slice_index] = False
                if (u.getMetadata().known_defective_slices is not None) and \
                   slice_index in u.getMetadata().known_defective_slices:
                    self.known_latent_defect[slice_index] = False

                if not self.drs_handler.isRepairable(self.durable_status[slice_index]):
                    info_logger.info(
                        "time: " + str(time) + " slice:" + str(slice_index) +
                        " durCount:" + str(self.durableCount(slice_index)) +
                        " latDefect:" + str(self.latent_defect[slice_index]) +
                        " due to disk " + str(u.getID()))
                    self.durable_status[slice_index] = self.lost_slice
                    self.undurable_slice_count += 1
                    self.endUnavailable(slice_index, time)
                    continue

                if self.durableCount(slice_index) == self.k and \
                   self.latent_defect[slice_index]:
                    info_logger.info(
                        "time: " + str(time) + " slice:" + str(slice_index) +
                        " durCount:" + str(self.durableCount(slice_index))
                        + " latDefect:" + str(self.latent_defect[slice_index])
                        + " due to latent error and disk " + str(u.getID()))
                    self.durable_status[slice_index] = self.lost_slice
                    self.undurable_slice_count += 1
                    self.endUnavailable(slice_index, time)

                # is this slice one that needs recovering? if so, how much
                # data to recover?
                if self.durable_status[slice_index] != self.lost_slice:
                    threshold_crossed = False
                    num_undurable = self.n - self.durableCount(slice_index)
                    if self.known_latent_defect[slice_index]:
                        num_undurable += 1
                    if num_undurable >= self.n - self.recovery_threshold:
                        threshold_crossed = True

                    num_unavailable = 0
                    if self.availability_counts_for_recovery:
                        num_unavailable = self.n - \
                            self.availableCount(slice_index)
                        if num_unavailable + num_undurable >= self.n - \
                           self.recovery_threshold:
                            threshold_crossed = True
                    if threshold_crossed:
                        projected_bandwidth_need += \
                            self.parallelRepairCost(slice_index)

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
            for child in u.getChildren():
                self.handleFailure(child, time, e, queue)

    def handleRecovery(self, u, time, e):
        if e.ignore:
            return

        current_total_slices = self.calCurrentTotalSlices(time)
        if isinstance(u, Machine):
            self.total_machine_repairs += 1

            # The temporary machine failures is simulated here, while the
            # permanent machine failure is simulated in disk recoveries
            if e.info != 3:
                disks = u.getChildren()
                for child in disks:
                    slice_indexes = child.getChildren()
                    for slice_index in slice_indexes:
                        if slice_index >= current_total_slices:
                            continue
                        if self.durableCount(slice_index) == self.lost_slice:
                            continue

                        if self.availableCount(slice_index) < self.n:
                            former_available = self.isRepairable(slice_index)
                            index = self.slice_locations[slice_index].index(child)
                            self.available_status[slice_index][index] = 1
                            self.sliceRecoveredAvailability(slice_index)
                            latter_available = self.isRepairable(slice_index)
                            if (not former_available) and latter_available:
                                self.endUnavailable(slice_index, time)
                        elif e.info == 1:  # temp & short failure
                            self.anomalous_available_count += 1
                        else:
                            pass
                self.slices_degraded_avail_list.append((e.getTime(), self.current_avail_slice_degraded))

        elif isinstance(u, Disk):
            self.total_disk_repairs += 1
            # this disk finished recovering, so decrement current recovery b/w
            self.current_recovery_bandwidth -= u.getLastBandwidthNeed()
            if self.current_recovery_bandwidth > -1 and self.current_recovery_bandwidth < 0:
                self.current_recovery_bandwidth = 0
            self._my_assert(self.current_recovery_bandwidth >= 0)

            transfer_required = 0.0
            slice_indexes = u.getChildren()
            for slice_index in slice_indexes:
                if slice_index >= current_total_slices:
                    continue
                if self.durableCount(slice_index) == self.lost_slice:
                    continue

                threshold_crossed = False
                actual_threshold = self.recovery_threshold

                num_undurable = self.n - self.durableCount(slice_index)
                if self.latent_defect[slice_index] and (not self.known_latent_defect[slice_index]):
                    num_undurable -= 1
                if num_undurable >= self.n - actual_threshold:
                    threshold_crossed = True

                if self.availability_counts_for_recovery:
                    num_unavailable = self.n - self.availableCount(slice_index)
                    if num_unavailable >= self.n - actual_threshold:
                        threshold_crossed = True

                if threshold_crossed:
                    if (u.getMetadata().nonexistent_slices is not None) \
                       and (slice_index in u.getMetadata().nonexistent_slices):
                        u.getMetadata().nonexistent_slices.remove(slice_index)

                        former_available = self.isRepairable(slice_index)
                        index = self.slice_locations[slice_index].index(u)

                        rc = self.repairCost(slice_index, index)
                        self.total_repairs += 1
                        self.durable_status[slice_index][index] = 1
                        transfer_required += rc
                        self.total_repair_transfers += rc
                        latter_available = self.isRepairable(slice_index)
                        if (not former_available) and latter_available:
                            # I think this place have to change, the end unavailable time should add the block recovery time?
                            self.endUnavailable(slice_index, time)

                    # after disk recovery, all sector errors are disappeared
                    if (u.getMetadata().defective_slices is not None) \
                       and (slice_index in u.getMetadata().defective_slices):
                        u.getMetadata().defective_slices.remove(slice_index)

                    # must come after all counters are updated
                    self.sliceRecovered(slice_index)

            self.slices_degraded_list.append((e.getTime(), self.current_slice_degraded))
            self.slices_degraded_avail_list.append((e.getTime(), self.current_avail_slice_degraded))

            self.addBandwidthStat(Recovery(u.getLastFailureTime(), e.getTime(), transfer_required))

        else:
            for child in u.getChildren():
                self.handleRecovery(child, time, e)

    def handleLatentDefect(self, u, time, e):
        current_total_slices = self.calCurrentTotalSlices(time)

        if isinstance(u, Disk):
            slice_count = len(u.getChildren())
            if slice_count == 0:
                return
            self._my_assert(slice_count > 10)

            slice_index = choice(u.getChildren())
            if slice_index >= current_total_slices:
                return

            if self.durableCount(slice_index) == self.lost_slice:
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

            self._my_assert(self.durableCount(slice_index) >= 0)
            self.sliceDegraded(slice_index)

            durable_count_before = self.durableCount(slice_index)
            index = self.slice_locations[slice_index].index(u)
            self.durable_status[slice_index][index] = 0
            self.latent_defect[slice_index] = True
            self.total_latent_failures += 1

            # Maybe this if can be deleted.
            if u.getMetadata().defective_slices is None:
                u.getMetadata().defective_slices = set()
            u.getMetadata().defective_slices.add(slice_index)

            if durable_count_before == self.k:
                info_logger.info(
                    str(time) + " slice: " + str(slice_index) +
                    " durCount: " + str(durable_count_before) +
                    " latDefect " + str(True) +
                    "  due to ===latent=== error " + " on disk " +
                    str(u.getID()))
                self.undurable_slice_count += 1
                self.endUnavailable(slice_index, time)
                self.durable_status[slice_index] = self.lost_slice
                u.getMetadata().defective_slices.remove(slice_index)
        else:
            raise Exception("Latent defect should only happen for disk")

        self.slices_degraded_list.append(
            (e.getTime(), self.current_slice_degraded))
        self.slices_degraded_avail_list.append(
            (e.getTime(), self.current_avail_slice_degraded))

    def handleLatentRecovered(self, u, time, e):
        transfer_required = 0.0
        current_total_slices = self.calCurrentTotalSlices(time)
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
                if slice_index >= current_total_slices:
                    continue
                self.total_scrub_repairs += 1
                self.latent_defect[slice_index] = False
                index = self.slice_locations[slice_index].index(u)
                if isinstance(self.durable_status[slice_index], int):
                    continue
                else:
                    rc = self.repairCost(slice_index, index)
                    self.total_repairs += 1
                    self.durable_status[slice_index][index] = 1
                    self.known_latent_defect[slice_index] = False
                    transfer_required += rc
                    self.total_repair_transfers += rc
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

    def end(self):
        # all_disks = []
        # self.distributer.getAllDisks(self.distributer.getRoot(), all_disks)
        # for rack_disks in all_disks:
        #     for u in rack_disks:
        #         u.meta = Metadata()

        ret = Result()

        # data loss probability and data unvailable probability
        data_loss_prob = format(float(self.undurable_slice_count)/self.total_slices, ".4e")
        unavailable_prob = self.calUnavailProb()

        Result.undurable_count = self.undurable_slice_count
        Result.unavailable_durations = self.unavailable_durations
        Result.data_loss_prob = data_loss_prob
        Result.unavailable_prob = unavailable_prob
        # repair bandwidth in GBs
        Result.total_repair_transfers = format(float(self.total_repair_transfers)/1024, ".4e")

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
             undurable_slice_count:%d\n \
             total repairs:%d, total optimal repairs:%d" %
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
             self.undurable_slice_count,
             self.total_repairs, self.total_optimal_repairs))

        self.printDegradedStat(self.slices_degraded_list,
                               "Avg_durable_degraded_", "slices")
        self.printDegradedStat(self.slices_degraded_avail_list,
                               "Avg_available_degraded_", "slices")

        self.analyzeBandwidth()

        return ret


class AutoNoramlEventHandler(object):
    """
    When system detects and identifies the permanent data loss, system will execute data repair and
    put the repaired data on a new chosen disk.

    Repair Time = Identification Time + Data Transferring Time
    """
    pass


if __name__ == "__main__":
    pass
