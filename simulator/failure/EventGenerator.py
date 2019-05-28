from abc import ABCMeta, abstractmethod


class EventGenerator:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, name, parameters):
        raise NotImplementedError

    @abstractmethod
    def generateNextEvent(self, current_time):
        raise NotImplementedError

    @abstractmethod
    def reset(self, current_time):
        raise NotImplementedError

    @abstractmethod
    def getName(self):
        raise NotImplementedError

    @abstractmethod
    def getCurrentTime(self):
        raise NotImplementedError
