

class Base(object):
    """
    stripe state: [1,1,0,-1,-2,...]
        block state 1 : Normal
        block state 0 : Unavailable
        block state -1: Lost(caused by disk corruption)
        block state -2: Lost(hit by Latent Sector Error)
    """
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

    # number of blocks on each disk.
    @property
    def DSC(self):
        return 1

    # total block amount for one stripe.
    @property
    def SSC(self):
        return self.n * self.DSC

    @property
    def SO(self):
        return round(float(self.n)/self.k, 3)

    # normal repair cost
    @property
    def RC(self):
        return self.k

    # optimal repair cost
    @property
    def ORC(self):
        return self.k

    # repair traffic in number of blocks
    def repairTraffic(self, hier=False, d_racks=0):
        if not hier:
            return float(self.ORC)
        else:
            return self.ORC * (1 - (float(self.n)/d_racks-1)/self.k)

    # Check 'state' can be recovered or not. If can be recovered, return the
    # corresponding repair cost, or return False.
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
    def repair(self, state, index):
        pass

    # Repair all failures simultaneously.
    # only_lost: only repair lost blocks
    def parallRepair(self, state, only_lost=False):
        pass


if __name__ == "__main__":
    pass
