from numpy import inf
from math import ceil
from enum  import Enum
from random import sample

from simulator.Log import info_logger, error_logger
from simulator.Event import Event
from simulator.unit.Machine import Machine
from simulator.unit.Disk import Disk
from simulator.unit.SliceSet import SliceSet

from simulator.eventHandler.EventHandler import EventHandler


class FailedSlice(object):
    intervals = []

    class RAFITransition(Enum):
          OutToOut = 0
          OutToIn = 1
          InToOut = 2
          InToIn = 3

    def __init__(self):
        self.start_time = []
        self.end_time = []

        # does slice already timeout?
        self.timeout = False

        # does slice already in RAFI event?
        self.inRAFI = False

    def getTimeout(self):
        return self.timeout

    def getRAFIflag(self):
        return self.inRAFI

    def setRAFIflag(self, flag=True):
        self.inRAFI = flag

    def failedNum(self):
        return len(self.start_time)

    def addInfo(self, start_time, end_time):
        self.start_time.append(start_time)
        self.end_time.append(end_time)

    def delete(self, ts):
        flag = True
        for item in self.end_time:
            if ts < item:
                flag = False

        return flag  # and not self.timeout

    def check(self, ts):
        recover_intervals = []
        origin_timeout = self.timeout
        for et in self.end_time:
            # failure recovery time already passed
            if ts > et:
                return
            recover_intervals.append(et - ts)

        self.timeout = True
        threshold = FailedSlice.intervals[self.failedNum()]
        for item in recover_intervals:
            if item < threshold:
                self.timeout = False

        # does check transmit the slice's timeout flag?
        if not origin_timeout and not self.timeout:
            return FailedSlice.RAFITransition.OutToOut
        elif not origin_timeout and self.timeout:
            return FailedSlice.RAFITransition.OutToIn
        elif origin_timeout and not self.timeout:
            return FailedSlice.RAFITransition.InToOut
        else:
            return FailedSlice.RAFITransition.InToIn


class UnfinishRAFIEvents(object):
    queue = None
    events = []
    event_id = 0

    def addEvent(self, slices, ts):
        s = SliceSet("SliceSet-RAFI"+str(UnfinishRAFIEvents.event_id), slices)
        UnfinishRAFIEvents.event_id += 0
        event = Event(Event.EventType.RAFIRecovered, ts, s)
        UnfinishRAFIEvents.events.append(event)
        UnfinishRAFIEvents.queue.addEvent(event)

    def updateEvent(self, slices, ts):
        slices_to_event = self.indexBySlices()
        all_slices = slices_to_event.keys()

        for s in slices:
            for item in all_slices:
                if s in item:
                    event = slices_to_event[item]
                    u = event.getUnit()
                    u.slices.remove(slice_index)

        self.addEvent(slices, ts)

    def removeEvent(self, event):
        UnfinishRAFIEvents.events.remove(event)
        # UnfinishRAFIEvents.queue.remove(event)

    def indexByTime(self):
        res = {}
        for event in events:
            ts = event.getTime()
            event_list = res.pop(ts, [])
            event_list.append(event)
            res[ts] = event_list

        return res

    def indexBySlices(self):
        res = {}
        for event in events:
            slices = event.getUnit().slices
            res[tuple(slices)] = event

        return res


class RAFIEventHandler(EventHandler):

    def __init__(self, distributer):
        super(RAFIEventHandler, self).__init__(distributer)
        self.rafi_recovery = self.conf.rafi_recovery
        if not self.rafi_recovery:
            raise Exception("RAFI recovery is not setting!")

        self.node_state_check = 0.25
        self.detect_intervals = self.conf.detect_intervals
        self._my_assert(len(self.detect_intervals) == self.n - self.k)
        FailedSlice.intervals = self.detect_intervals

        self.failed_slices = {}
        self.unfinished_rafi_events = UnfinishRAFIEvents()

    def handleFailure(self, u, time, e, queue):
        if e.ignore:
            return

        UnfinishRAFIEvents.queue = queue
        outtoin_slices = {}
        intoin_slices = {}

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
                slice_indexes = child.getChildren()
                for slice_index in slice_indexes:
                    if slice_index >= self.total_slices:
                        continue
                    if self.status[slice_index] == self.lost_slice:
                        continue

                    if e.info == 3:
                        self.sliceDegraded(slice_index)
                    else:
                        self.sliceDegradedAvailability(slice_index)

                    repairable_before = self.isRepairable(slice_index)
                    index = self.slice_locations[slice_index].index(child)
                    if self.status[slice_index][index] == -1:
                        continue
                    if e.info == 3:
                        self.status[slice_index][index] == -1
                        self._my_assert(self.durableCount(slice_index) >= 0)
                    else:
                        if self.status[slice_index][index] == 1:
                            self.status[slice_index][index] = 0
                        self._my_assert(self.availableCount(slice_index) >= 0)

                    repairable_current = self.isRepairable(slice_index)
                    if repairable_before and not repairable_current:
                        self.unavailable_slice_count += 1
                        if slice_index in self.unavailable_slice_durations.keys():
                            self.unavailable_slice_durations[slice_index].append([time])
                        else:
                            self.unavailable_slice_durations[slice_index] = [[time]]

                    # rafi start
                    unavailable = self.n - self.availableCount(slice_index)
                    fs = FailedSlice()
                    fs.addInfo(time, e.next_recovery_time)
                    self.failed_slices[slice_index] = fs

                    rafi_flag = fs.check(time)
                    # slice from not in rafi event to in a rafi event
                    if rafi_flag == FailedSlice.RAFITransition.OutToIn:
                        outtoin_slices[slice_index] = fs
                    # slice from lower risk rafi event to higher risk rafi event
                    elif rafi_flag == FailedSlice.RAFITransition.InToIn:
                        intoin_slices[slice_index] = fs
                    else:  # don't care other two situations
                        pass

                    if e.info == 3:
                        # lost stripes have been recorded in unavailable_slice_durations
                        if self.isLost(slice_index):
                            info_logger.info(
                                "time: " + str(time) + " slice:" + str(slice_index) +
                                " durCount:" + str(self.durableCount(slice_index)) +
                                " due to machine " + str(u.getID()))
                            self.status[slice_index] = self.lost_slice
                            self.undurable_slice_count += 1
                            self.undurable_slice_infos.append((slice_index, time, "machine "+ str(u.getID())))
                            continue

            outtoin_slice_indexes = outtoin_slices.keys()
            intoin_slice_indexes = intoin_slices.keys()
            new_rafi_slices = []
            upgraded_rafi_slices = []
            for slice_index in outtoin_slice_indexes:
                if self.availableCount(slice_index) <= self.recovery_threshold:
                    new_rafi_slices.append(slice_index)
            for slice_index in intoin_slice_indexes:
                if self.availableCount(slice_index) <= self.recovery_threshold:
                    upgraded_rafi_slices.append(slice_index)

            if new_rafi_slices != []:
                groups_in_new = [[] for i in xrange(self.n - self.k)]
                for slice_index in new_rafi_slices:
                    unavailable = outtoin_slices[slice_index].failedNum()
                    groups_in_new[unavailable-1].append(slice_index)
                for group in groups_in_new:
                    if group != []:
                        # timestamp of data starts to recover(ts+detect time)
                        recover_time = time + self.detect_intervals[unavailable-1]
                        self.unfinished_rafi_events.addEvent(group, recover_time)

            if upgraded_rafi_slices != []:
                groups_in_upgraded = [[] for i in xrange(self.n - self.k)]
                for slice_index in upgraded_rafi_slices:
                    unavailable = intoin_slices[slice_index].failedNum()
                    groups_in_upgraded[unavailable-1].append(slice_index)
                for group in groups_in_upgraded:
                    if group != []:
                        recover_time = time + self.detect_intervals[unavailable-1]
                        self.unfinished_rafi_events.updateEvent(group, recover_time)
        elif isinstance(u, Disk):
            self.total_disk_failures += 1
            u.setLastFailureTime(e.getTime())
            # need to compute projected reovery b/w needed
            projected_bandwidth_need = 0.0

            slice_indexes = u.getChildren()
            for slice_index in slice_indexes:
                if slice_index >= self.total_slices:
                    continue
                if self.status[slice_index] == self.lost_slice:
                    continue

                self.sliceDegraded(slice_index)
                repairable_before = self.isRepairable(slice_index)

                index = self.slice_locations[slice_index].index(u)
                if self.status[slice_index][index] == -1:
                    continue
                self.status[slice_index][index] = -1

                self._my_assert(self.durableCount(slice_index) >= 0)

                repairable_current = self.isRepairable(slice_index)
                if repairable_before and not repairable_current:
                    self.unavailable_slice_count += 1
                    if slice_index in self.unavailable_slice_durations.keys():
                        self.unavailable_slice_durations[slice_index].append([time])
                    else:
                        self.unavailable_slice_durations[slice_index] = [[time]]

                if self.isLost(slice_index):
                    info_logger.info(
                        "time: " + str(time) + " slice:" + str(slice_index) +
                        " durCount:" + str(self.durableCount(slice_index)) +
                        " due to disk " + str(u.getID()))
                    self.status[slice_index] = self.lost_slice
                    self.undurable_slice_count += 1
                    self.undurable_slice_infos.append((slice_index, time, "disk "+ str(u.getID())))
                    continue
        else:
            for child in u.getChildren():
                self.handleFailure(child, time, e, queue)

    # data recovery is no relationship with component recovery,
    # so we just update failed slices.
    def handleRecovery(self, u, time, e, queue):
        if e.ignore:
            return

        failed_slice_indexes = self.failed_slices.keys()
        if isinstance(u, Machine):
            if e.info == 3 and not self.conf.queue_disable:
                disks = u.getChildren()
                empty_flag = True
                for disk in disks:
                    if disk.getChildren() != []:
                        empty_flag = False
                        break
                if empty_flag:
                    return

                node_repair_time = self.conf.node_repair_time
                node_repair_start = time - node_repair_time
                all_racks = self.distributer.getAllRacks()

                if self.conf.data_placement == "sss":
                    queue_rack_count = self.conf.rack_count
                elif self.conf.data_placement == "pss" and not self.conf.hierarchical:
                    queue_rack_count = self.n
                elif self.conf.data_placement == "copyset" and not self.conf.hierarchical:
                    queue_rack_count = self.conf.scatter_width
                else:
                    queue_rack_count = self.conf.distinct_racks
                if self.conf.data_redundancy[0] in ["MSR", "MBR"]:
                    num = self.conf.drs_handler.d
                else:
                    num = self.conf.drs_handler.k

                chosen_racks = sample(all_racks, queue_rack_count)
                recovery_time = self.contention_model.occupy(node_repair_start, chosen_racks, num, node_repair_time)
                recovery_event = Event(Event.EventType.Recovered, recovery_time, u, 4)
                queue.addEvent(recovery_event)
            else:
                self.total_machine_repairs += 1

                disks = u.getChildren()
                for disk in disks:
                    slice_indexes = disk.getChildren()
                    for slice_index in slice_indexes:
                        if slice_index >= self.total_slices:
                            continue
                        if self.status[slice_index] == self.lost_slice:
                            if slice_index in self.unavailable_slice_durations.keys() and \
                                len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                                self.unavailable_slice_durations[slice_index][-1].append(time)
                            continue

                        delete_flag = True
                        if slice_index in failed_slice_indexes:
                            fs = self.failed_slices[slice_index]
                            delete_flag = fs.delete(time)
                            if delete_flag:
                                self.failed_slices.pop(slice_index)

                        if delete_flag:
                            if self.availableCount(slice_index) < self.n:
                                repairable_before = self.isRepairable(slice_index)

                                index = self.slice_locations[slice_index].index(disk)
                                if self.status[slice_index][index] == 0:
                                    self.status[slice_index][index] = 1
                                self.sliceRecoveredAvailability(slice_index)

                                repairable_current = self.isRepairable(slice_index)
                                if not repairable_before and repairable_current:
                                    self.unavailable_slice_durations[slice_index][-1].append(time)
                            elif e.info == 1:  # temp & short failure
                                self.anomalous_available_count += 1
                            else:
                                pass
        elif isinstance(u, Disk):
            if e.info != 4 and not self.queue_disable:
                if len(u.getChildren()) == 0:
                    return

                all_racks = self.distributer.getAllRacks()
                disk_repair_time = self.conf.disk_repair_time
                disk_repair_start = time - disk_repair_time
                if self.conf.data_placement == "sss":
                    queue_rack_count = self.conf.rack_count
                elif self.conf.data_placement == "pss" and not self.conf.hierarchical:
                    queue_rack_count = self.n
                elif self.conf.data_placement == "copyset" and not self.conf.hierarchical:
                    queue_rack_count = self.conf.scatter_width
                else:
                    queue_rack_count = self.conf.distinct_racks
                if self.conf.data_redundancy[0] in ["MSR", "MBR"]:
                    num = self.conf.drs_handler.d
                else:
                    num = self.conf.drs_handler.k

                if self.conf.data_redundancy[0] in ["MSR", "MBR"]:
                    num = self.conf.drs_handler.d
                else:
                    num = self.conf.drs_handler.k
                chosen_racks = sample(all_racks, queue_rack_count)
                recovery_time = self.contention_model.occupy(disk_repair_start, chosen_racks, num, disk_repair_time)
                recovery_event = Event(Event.EventType.Recovered, recovery_time, u, 4)
                queue.addEvent(recovery_event)
                return

            self.total_disk_repairs += 1
            transfer_required = 0.0
            slice_indexes = u.getChildren()
            for slice_index in slice_indexes:
                if slice_index >= self.total_slices:
                    continue
                if self.status[slice_index] == self.lost_slice:
                    if slice_index in self.unavailable_slice_durations.keys() and \
                        len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                        self.unavailable_slice_durations[slice_index][-1].append(time)
                    continue
                if not self.isRepairable(slice_index):
                    continue

                if slice_index in failed_slice_indexes:
                    fs = self.failed_slices[slice_index]
                    delete_flag = fs.delete(time)
                    if delete_flag:
                        self.failed_slices.pop(slice_index)
                    else:
                        continue

                threshold_crossed = False
                actual_threshold = self.recovery_threshold
                if self.conf.lazy_only_available:
                    actual_threshold = self.n - 1
                if self.current_slice_degraded < self.conf.max_degraded_slices*self.total_slices:
                    actual_threshold = self.recovery_threshold

                if self.durableCount(slice_index) <= actual_threshold:
                    threshold_crossed = True

                if self.availability_counts_for_recovery:
                    if self.availableCount(slice_index) <= actual_threshold:
                        threshold_crossed = True

                if threshold_crossed:
                    index = self.slice_locations[slice_index].index(u)
                    if self.status[slice_index][index] == -1 or self.status[slice_index][index] == -2:
                        repairable_before = self.isRepairable(slice_index)

                        # if self.lazy_recovery or self.parallel_repair:
                        rc = self.parallelRepair(slice_index, True)
                        # else:
                        #     rc = self.repair(slice_index, index)
                        if slice_index in u.getSlicesHitByLSE():
                            u.slices_hit_by_LSE.remove(slice_index)
                        self.total_repairs += 1
                        ratio = self.getRatio()
                        transfer_required += rc * ratio
                        self.total_repair_transfers += rc * ratio

                    # must come after all counters are updated
                    self.sliceRecovered(slice_index)
        else:
            for child in u.getChildren():
                self.handleRecovery(child, time, e, queue)

    # def handleEagerRecoveryStart(self, u, time, e, queue):
    #     return

    def handleRAFIRecovery(self, u, time, e, queue):
        if e.ignore:
            return

        slices = u.slices
        transfer_required = 0.0
        UnfinishRAFIEvents.queue = queue
        for slice_index in slices:
            if self.status[slice_index] == self.lost_slice:
                if slice_index in self.unavailable_slice_durations.keys() and \
                    len(self.unavailable_slice_durations[slice_index][-1]) == 1:
                    self.unavailable_slice_durations[slice_index][-1].append(time)
                continue
            if self.isLost(slice_index):
                self.status[slice_index] = self.lost_slice
                continue
            if not self.isRepairable(slice_index):
                continue

            # if self.availableCount(slice_index) <= self.recovery_threshold:
            rc = self.parallelRepair(slice_index, True)
            ratio = self.getRatio()
            transfer_required += rc * ratio
            self.total_repair_transfers += rc * ratio
            if slice_index in self.failed_slices.keys():
                self.failed_slices.pop(slice_index)

            # repairable_current = self.isRepairable(slice_index)
            # if not repairable_before and repairable_current:
            #    self.endUnavailable(slice_index, time)
            self.sliceRecovered(slice_index)
        self.unfinished_rafi_events.removeEvent(e)
