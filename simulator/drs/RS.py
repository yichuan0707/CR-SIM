from simulator.drs.base import Base


class RS(Base):

    def __init__(self, params):
        super(RS, self).__init__(params)
        self.m = self.n - self.k

    def stateRepairCost(self, state, index=None):
        if not self.isRepairable(state):
            return -1
        return self.k

    # Suppose there is a (a < m) failures in one stripe. Parallel Repair
    # starts on node hold one failure, downloads k blocks, then
    # calculates, at last uploads a-1 failures to destinations.
    def stateParallRepairCost(self, state):
        if not self.isRepairable(state):
            return - 1

        return state.count(0) + self.k - 1


if __name__ == "__main__":
    rs = RS(['9','6'])
    print rs.isMDS
    print rs.DSC
    print rs.SSC
    print rs.RC
    print rs.isRepairable([1, 1, 1, 0, 1, 0, 1, 1, 1])
    print rs.stateRepairCost([1, 1, 1, 1, 1, 0, 0, 1, 1])
    print rs.stateParallRepairCost([1, 0, 0, 1, 0, 0, 1, 1, 1])
