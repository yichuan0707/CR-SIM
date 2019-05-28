from simulator.drs.base import Base


class MSR(Base):

    def __init__(self, params):
        self.d = int(params[2])
        super(MSR, self).__init__(params)
        self.m = self.n - self.k

    def _check(self):
        super(MSR, self)._check()
        if self.d < self.k or self.d >= self.n:
            raise Exception("d must be in [k, n).")

    # When available blocks in state are no less than 'd', MSR can proceed
    # optimal repair.
    # Return value in blocks(Configuration.block_size).
    @property
    def ORC(self):
        return float(self.d)/float(self.d - self.k + 1)

    def stateRepairCost(self, state, index=None):
        if not self.isRepairable(state):
            return -1

        avails = state.count(1)
        if avails >= self.d:
            return self.ORC
        else:
            return self.k

    # MSR can not recover multi failures through regenerating even available
    # blocks larger than 'd'.
    def stateParallRepairCost(self, state):
        if not self.isRepairable(state):
            return -1

        avails = state.count(1)
        if avails == self.n - 1:
            return self.ORC
        else:
            return state.count(0) + self.k - 1


if __name__ == "__main__":
    msr = MSR([14, 10, 12])
    print msr.SSC
    print msr.stateRepairCost([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1])
