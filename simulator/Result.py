

class Result(object):
    undurable_count = 0
    unavailable_count = 0
    # undurable caused by LSE, disk, node, disk count cause lost, node count cause lost
    undurable_count_details = None
    unavailable_slice_durations = {}
    NOMDL = 0
    data_loss_prob = 0.0
    unavailable_prob = 0.0
    unavailable_prob1 = 0.0
    unavailable_prob2 = 0.0
    total_repair_transfers = 0.0
    queue_times = 0
    avg_queue_time = 0.0

    def toString(self):
        return "unavailable=" + str(Result.unavailable_count) + \
            "  undurable=" + str(Result.undurable_count) + \
            " unavailable_prob=" + str(Result.unavailable_prob) + \
            " unavailable_prob1=" + str(Result.unavailable_prob1) + \
            " unavailable_prob2=" + str(Result.unavailable_prob2) + \
            " data loss prob=" + str(Result.data_loss_prob) + \
            " total_repair_transfers=" + str(Result.total_repair_transfers) + "TiB" + \
            " NOMDL=" + str(Result.NOMDL) + " queue times=" + str(Result.queue_times) + \
            " average queue time=" + str(Result.avg_queue_time) + "h"
