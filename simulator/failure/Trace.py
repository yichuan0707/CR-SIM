from numpy import Inf

from simulator.Configuration import Configuration
from simulator.failure.EventGenerator import EventGenerator


class TmpEvent(object):
    def __init__(self, iid, ts, start, perm):
        self.id = iid
        self.ts = ts
        self.start = start
        self.perm = perm

    def toString(self):
        return self.id + " " + self.ts + " " + self.start + " " \
               + self.perm + " "


class Trace(EventGenerator):

    def __init__(self, name, parameters):
        self.name = name
        self.filename = parameters.get("filename")
        try:
            self.initEvents(self.filename)
        except Exception, e:
            # should be replaced by error logs
            # System.err.println("Failed to read file: "+e);
            raise Exception(e)

        self.events = None
        self.current_machine = None
        self.current_event_type = False

    def parseLine(self, line):
        substrings = line.split(",")
        pair = TmpEvent[2]

        pair[0] = TmpEvent(int(substrings[2]), float(substrings[3])/3600,
                           True, substrings[6] == "permanent")
        pair[1] = TmpEvent(int(substrings[2]), float(substrings[4])/3600,
                           False, substrings[6] == "permanent")
        return pair

    def initEvents(self, filename):
        if self.events is None:
            self.events = {}
            with open(filename, 'r') as f:
                # begin = True
                first_event = Inf
                last_event = 0
                line = f.readline()
                while line is not None:
                    te = self.parseLine(line)
                    if first_event > te[0].ts:
                        first_event = te[0].ts
                    if last_event < te[1].ts:
                        last_event = te[1].ts

                    if te[0].id not in self.events.keys():
                        machine = []
                        self.events[te[0].id] = machine
                    else:
                        machine = self.events.get(te[0].id)

                    machine.append(te[0])
                    machine.append(te[1])

            v = self.events.values()
            for m in v:
                for te in m:
                    te.ts -= first_event

            if Configuration.totalTime > last_event - first_event:
                # System.err.println("WARNING: Requested simulation time is
                # LARGER than the trace time: "+ Configuration.totalTime +
                # " days > " + (lastEvent-firstEvent) +" days");
                # System.err.println("Setting simulation time to the trace
                #  time")
                Configuration.totalTime = (int)(last_event - first_event)

    def setCurrentMachine(self, m_id):
        if m_id not in self.events.keys:
            pass
            # I think this place might be error logs.
            # System.err.println("No events found for machine "+id)
        self.current_machine = self.events.get(m_id)

    def setCurrentEventType(self, start):
        self.currentEventType = start

    def eventAccepted(self):
        self.current_machine.pop()

    def generateNextEvent(self, current_time):
        if self.current_machine is None:
            # return a very big value
            return Inf

        if self.current_machine == []:
            return Inf
            # System.err.println("No more events for machine "+currentMachine);

        self.event = self.current_machine[0]
        assert (self.event.start == self.current_event_type)
        assert (current_time <= self.event.ts)
        return self.event.ts

    def reset(self, current_time):
        pass

    def getName(self):
        return self.name

    def getCurrentTime(self):
        return 0
