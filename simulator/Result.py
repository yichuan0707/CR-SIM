

class Result(object):
    undurable_count = 0
    unavailable_count = 0
    # undurable caused by LSE, disk, node, disk count cause lost, node count cause lost
    undurable_count_details = None
    unavailable_slice_durations = {}
    NOMDL = 0
    PDL = 0.0
    # unavailability = MTTR/(MTTF + MTTR)
    PUA = 0.0
    # unavailability = downtime/(uptime+downtime)
    PUA1 = 0.0
    # total repair cost, in PiBs
    TRC = 0.0
    # total storage cost, in PiB*year
    TSC = 0.0
    queue_times = 0
    avg_queue_time = 0.0

    def toString(self):
        return "unavailable=" + str(Result.unavailable_count) + \
            "  undurable=" + str(Result.undurable_count) + \
            " PUA=" + str(Result.PUA) + \
            " PDL=" + str(Result.PDL) + \
            " TRC=" + str(Result.TRC) + "PiBs" + \
            " TSC=" + str(Result.TSC) + "PiB*year" + \
            " NOMDL=" + str(Result.NOMDL) + " queue times=" + str(Result.queue_times) + \
            " average queue time=" + str(Result.avg_queue_time) + "h"
