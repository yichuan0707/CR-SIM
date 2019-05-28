from collections import OrderedDict
from simulator.Event import Event


class EventQueue(object):
    events = OrderedDict()

    def addEvent(self, e):
        if e.getTime() in EventQueue.events.keys():
            EventQueue.events.get(e.getTime()).append(e)
        else:
            event_list = []
            event_list.append(e)
            EventQueue.events.setdefault(e.getTime(), event_list)

    def addEventQueue(self, queue):
        all_events = queue.getAllEvents()
        for e in all_events:
            self.addEvent(e)

    def removeFirst(self):
        if EventQueue.events.keys() == []:
            return None

        # pop and deal with event based on the timestamp, so we need to
        # sort at first.
        keys = EventQueue.events.keys()
        keys.sort()
        first_key = keys[0]

        first_value = EventQueue.events[first_key]
        first_event = first_value.pop(0)
        if len(first_value) == 0:
            EventQueue.events.pop(first_key)

        return first_event

    def getAllEvents(self):
        res = []
        for e in EventQueue.events.values():
            res.append(e)
        return res

    def convertToArray(self):
        event_list = []
        # check if we can operate OrderedDict like this?
        # Yes, we can. This is normal operation for Dict.
        iterator = EventQueue.events.itervalues()
        for l in iterator:
            for e in l:
                event_list.append(e)

        return event_list

    # override?
    def clone(self):
        ret = EventQueue()
        keys = EventQueue.events.keys()
        keys.sort()
        # this place will be self.events or EventQueue.events?
        for ts in keys:
            list1 = EventQueue.events.get(ts)
            list2 = []
            for e in list1:
                list2.append(e)
            ret.events.setdefault(ts, list2)

        return ret

    def size(self):
        size = 0
        for item in EventQueue.events.values():
            size += len(item)
        return size

    def printAll(self, file_name, msg):
        with open(file_name, 'w+') as out:
            out.write(msg + "\n")
            keys = EventQueue.events.keys()
            keys.sort()
            for t in keys:
                res = EventQueue.events[t]
                for e in res:
                    if e.ignore is False:
                        out.write(e.toString())

    def printEvents(self, file_name, msg, event_type=Event.EventType.Failure, sort = True):
        with open(file_name, 'w') as fp:
            fp.write(msg + "\n")
            keys = EventQueue.events.keys()
            if sort:
                keys.sort()
            for t in keys:
                res = EventQueue.events[t]
                for e in res:
                    if (e.ignore is False) and \
                            e.getType() == event_type:
                        fp.write(e.toString())

