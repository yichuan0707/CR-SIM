
def splitMethod(string, split_with=','):
    s = string.strip()
    return s.split(split_with)

def splitIntMethod(string, split_with=','):
    res = []
    s = string.strip()
    tmp = s.split(split_with)
    for item in tmp:
        res.append(int(item))

    return res

def splitFloatMethod(string, split_with=','):
    res = []
    s = string.strip()
    tmp = s.split(split_with)
    for item in tmp:
        res.append(float(item))

    return res

def extractDRS(string):
    drs = splitMethod(string, '_')
    return [drs[0]] + [int(item) for item in drs[1:]]


class FIFO(object):
    """
    First-In-First-Out Rack Model for queue time.
    """
    def __init__(self, all_racks):
        self.occupation_points = {}
        for rack in all_racks:
            self.occupation_points[rack] = 0

        self.queue_records = []

    def get(self, rack):
        return self.occupation_points[rack]

    def getAll(self):
        return self.occupation_points

    def occupy(self, ts, chosen_racks, num, time_cost):
        queue_times = []
        racks = []
        for rack in chosen_racks:
            if self.occupation_points[rack] <= ts:
                q_time = 0.0
            else:
                q_time = self.occupation_points[rack] - ts

            insert_flag = False
            for i, item in enumerate(queue_times):
                if item > q_time:
                    insert_flag = True
                    queue_times.insert(i, q_time)
                    racks.insert(i, rack)
                    break
            if not insert_flag:
                queue_times.append(q_time)
                racks.append(rack)

        # real queue time for one component recovery
        real_queue_time = max(queue_times[:num])
        if real_queue_time > float(0):
            self.queue_records.append(real_queue_time)

        recovery_time = ts + time_cost + real_queue_time
        for rack in racks[:num]:
            if self.occupation_points[rack] <= ts:
                self.occupation_points[rack] = ts + time_cost
            else:
                self.occupation_points[rack] += time_cost
        return recovery_time

    def statistics(self):
        queue_times = len(self.queue_records)
        if queue_times == 0:
            return 0, 0.0
        avg_queue_time = sum(self.queue_records)/queue_times

        return queue_times, avg_queue_time


if __name__ == "__main__":
    pass
    # s = " (RS_3_1, RS_14_10) : (RS_14_10, LRC_16_10_2)"
    # r = extractHeteDRSFromDiffMedium(s)
