

class Base(object):

    def __init__(self, params):
        self.n = int(params[0])
        self.k = int(params[1])
        self._check()

    def _check(self):
        if self.k < 0 or self.n < 0 or self.k >= self.n:
            raise Exception("Parameter Error!")

    @property
    def isMDS(self):
        return True

    # number of blocks on device.
    @property
    def DSC(self):
        return 1

    # total block number for one stripe.
    @property
    def SSC(self):
        return self.n * self.DSC

    # normal repair cost
    @property
    def RC(self):
        return self.k

    # optimal repair cost
    @property
    def ORC(self):
        pass

    # Check 'state' can be recovered or not. If can be recovered, return the
    # corresponding repair cost, or return False.
    # state = [1, 0, 1, 0, ..., 1], 1 for available, 0 for unavailable.
    def isRepairable(self, state):
        # state = -100 means data lost
        if isinstance(state, int):
            return False
        if len(state) != self.n:
            raise Exception("State Length Error!")

        avails = state.count(1)
        if avails >= self.k:
            return True
        return False

    # Repair failures one by one. 'index' is the block index which will be repaired.
    def stateRepairCost(self, state, index=None):
        pass

    # Repair all failures simultaneously.
    def stateParallRepairCost(self, state):
        pass


class BaseWithoutParititioning(object):
    """
    Base data redundancy scheme Class for unparititioning system, no blocks
    in it, someone calls 'online encoding'.
    """
    pass


if __name__ == "__main__":
    pass
