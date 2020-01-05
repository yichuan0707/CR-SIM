from simulator.drs.base import Base


class RS(Base):

    def __init__(self, params):
        super(RS, self).__init__(params)
        self.m = self.n - self.k

    def repair(self, state, index):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")
        if state[index] == 1:
            raise Exception("index:" + str(index) + " in " +str(state) + " is normal state")
        else:
            state[index] = 1
        return self.k

    # Suppose there is a (a < m) failures in one stripe. Parallel Repair
    # starts on node hold one failure, downloads k blocks, then
    # calculates, at last uploads a-1 failures to destinations.
    def parallRepair(self, state, only_lost=False):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")

        repair_amount = 0
        for i in xrange(self.n):
            if state[i] == -1 or state[i] == -2:
                state[i] = 1
                repair_amount += 1
            if not only_lost and state[i] == 0:
                state[i] = 1
                repair_amount += 1

        if repair_amount == 0:
            return 0
        else:
            return repair_amount + self.k - 1


if __name__ == "__main__":
    rs = RS(['9','6'])
    state = [1] * 9
    state[2] = 0
    state[3] = -1
    state[4] = -2
    print rs.isMDS
    print rs.DSC
    print rs.SSC
    print rs.RC
    print rs.isRepairable(state)
    print rs.repair(state, 3)
    print rs.parallRepair(state, False)

