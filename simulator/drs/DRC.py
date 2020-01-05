from math import floor

from simulator.drs.base import Base


class DRC(Base):

    def __init__(self, params):
        self.r = params[2]
        super(DRC, self).__init__(params)

    def _check(self):
        super(DRC, self)._check()
        if self.r <= 0 or self.r > self.n:
            raise Exception("0< r <= n")

    @property
    def ORC(self):
        r = self.r
        k = self.k
        n = self.n
        orc = float(r - 1)/(r - floor(k*r/n))
        return round(orc, 5)

    # DRC is a hierarchical data redundanacy scheme,
    # params "hier" and "d_racks" are meaningless.
    def repairTraffic(self, hier=True, d_racks=0):
        return self.ORC

    def repair(self, state, index):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")
        if state[index] == 1:
            raise Exception("index:" + str(index) + " in " +str(state) + " is normal state")

        avails = state.count(1)
        state[index] = 1
        if avails >= self.n - 1:
            return self.ORC
        else:
            return self.k

    # MSR can not recover multi failures through regenerating even available
    # blocks larger than 'd'.
    def parallRepair(self, state, only_lost=False):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")

        repair_amount = 0
        avails = state.count(1)
        for i in xrange(self.n):
            if state[i] == -1 or state[i] == -2:
                state[i] = 1
                repair_amount += 1
            if not only_lost and state[i] == 0:
                state[i] = 1
                repair_amount += 1

        if repair_amount == 0:
            return 0
        elif avails >= self.n - 1 and repair_amount == 1:
            return self.ORC
        else:
            return repair_amount + self.k - 1


if __name__ == "__main__":
    msr = DRC([8, 6, 4])
    state = [1] * 8
    state[1] = -1
    state[2] = -2
    print msr.SSC
    print msr.repair(state, 2)
    print msr.parallRepair(state, True)

