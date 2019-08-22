

class Result(object):
    undurable_count = 0
    # undurable caused by LSE, disk, node, disk count cause lost, node count cause lost
    undurable_count_details = None
    NOMDL = 0
    unavailable_durations = {}
    data_loss_prob = 0.0
    unavailable_prob = 0.0
    total_repair_transfers = 0.0

    def toString(self):
        return "unavailable=" + str(len(Result.unavailable_durations.keys())) + \
            "  undurable=" + str(Result.undurable_count) + \
            " unavailable_prob=" + str(Result.unavailable_prob) + \
            " data loss prob=" + str(Result.data_loss_prob) + \
            " total_repair_transfers=" + str(Result.total_repair_transfers) + "TiB"
