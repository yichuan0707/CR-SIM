from enum import Enum


class Event(object):
    event_id = 0

    class EventType(Enum):
        Start = 0
        Failure = 1
        Recovered = 2
        EagerRecoveryStart = 3
        EagerRecoveryInstallment = 4
        LatentDefect = 5
        LatentRecovered = 6
        RAFIRecovered = 7
        # ScrubStart = 6
        # ScrubComplete = 7
        End = 8

    def __init__(self, e_type, time, unit, info=-100, ignore=False,
                 next_recovery_time=0):
        self.type = e_type
        self.time = time
        self.unit = unit
        self.info = info
        self.ignore = ignore
        self.next_recovery_time = next_recovery_time
        self.attributes = {}
        Event.event_id += 1
        self.event_id = Event.event_id

    def getType(self):
        return self.type

    def getTime(self):
        return self.time

    def getUnit(self):
        return self.unit

    def getAttributes(self, key):
        if self.attributes == {}:
            return None
        return self.attributes[key]

    def setAttributes(self, key, value):
        self.attributes[key] = value

    # time + " " + next_recovery + " " + unit + " " + type + " " + info + " "
    # + ignore
    def toString(self):
        format_string = str(self.time) + "  " + str(self.next_recovery_time) \
            + "  " + self.unit.toString() + "  " + str(self.type) + "  " \
            + str(self.info) + "  " + str(self.ignore) + "  " \
            + str(self.event_id) + "\n"
        return format_string
