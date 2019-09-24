from simulator.unit.Unit import Unit


class SliceSet(Unit):

    def __init__(self, name, slice_set):
        super(SliceSet, self).__init__(name, None, None)
        self.slices = slice_set
        self.original_failure_time = 0

    def getOriginalFailureTime(self):
        return self.original_failure_time

    def setOriginalFailureTime(self, original_failure_time):
        self.original_failure_time = original_failure_time

    def removeSlice(self, slice_index):
        self.slices.remove(slice_index)

    def addSlice(self, slice_index):
        self.slices.append(slice_index)
