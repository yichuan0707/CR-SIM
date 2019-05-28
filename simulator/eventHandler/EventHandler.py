from abc import ABCMeta, abstractmethod


class EventHandler:
    __metaclass__ = ABCMeta

    @abstractmethod
    def start(self, root, total_slices, disk_count):
        raise NotImplementedError

    @abstractmethod
    def handleEvent(self, event, queue):
        raise NotImplementedError

    @abstractmethod
    def end(self):
        raise NotImplementedError
