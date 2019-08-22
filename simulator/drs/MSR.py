from simulator.UnitState import UnitState
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

    # repair traffic in number of blocks
    def repairTraffic(self, hier=False, d_racks=0):
        if not hier:
            return float(self.ORC)
        else:
            return self.ORC * (1 - (float(self.n)/d_racks-1)/self.d)

    def repair(self, state, index):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")
        if state[index] == UnitState.Normal:
            raise Exception("index:" + str(index) + " in " +str(state) + " is normal state")

        avails = state.count(UnitState.Normal)
        state[index] = UnitState.Normal
        if avails >= self.d:
            return self.ORC
        else:
            return self.k

    # MSR can not recover multi failures through regenerating even available
    # blocks larger than 'd'.
    def parallRepair(self, state, only_lost=False):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")

        repair_amount = 0
        avails = state.count(UnitState.Normal)
        for i in xrange(self.n):
            if state[i] == UnitState.Corrupted or state[i] == UnitState.LatentError:
                state[i] = UnitState.Normal
                repair_amount += 1
            if not only_lost and state[i] == UnitState.Crashed:
                state[i] = UnitState.Normal
                repair_amount += 1

        if repair_amount == 0:
            return 0
        elif avails >= self.d and repair_amount == 1:
            return self.ORC
        else:
            return repair_amount + self.k - 1


if __name__ == "__main__":
    msr = MSR([14, 10, 12])
    state = [UnitState.Normal] * 14
    state[1] = UnitState.Crashed
    state[2] = UnitState.LatentError
    state[3] = UnitState.Corrupted
    print msr.SSC
    print msr.repair(state, 2)
    print msr.parallRepair(state, True)
