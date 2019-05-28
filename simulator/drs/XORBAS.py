from copy import deepcopy
from simulator.drs.LRC import LRC


# XORBAS can be treated as special LRC, which 'm0=1', and all parity blocks
# make up with a new group, which can repair one failure with optimal repair
# cost.
# And in XORBAS, we usually have 'l + m1 = k/l + m0'
class XORBAS(LRC):
    """
    Only works when local parity is 1 (m0 = 1).
    """

    def isRepairable(self, state):
        if isinstance(state, int):
            return False
        parity_group = state[-(self.m1 + self.ll):]
        new_state = deepcopy(state)
        if parity_group.count(0) == 1:
            parity_recovery_id = parity_group.index(0)
            new_state[self.k + parity_recovery_id] += 1
        return super(XORBAS, self).isRepairable(new_state)

    def stateRepairCost(self, state, index=None):
        if not self.isRepairable(state):
            return -1

        if state[index] != 0:
            raise Exception("Corresponding index is not lost.")

        optimal_flag = False
        b = self.k/self.ll
        index_groups = [[] for x in xrange(self.ll)]

        for i in xrange(self.k):
            g_id = i/b
            index_groups[g_id].append(i)

        for j in xrange(self.k, self.n-self.m1):
            g_id = (j-self.k)/self.m0
            index_groups[g_id].append(j)

        index_groups.append([i for i in xrange(self.k, self.n)])

        for group in index_groups:
            if index in group:
                group_state = [state[i] for i in group]
                if group_state.count(0) <= 1:
                    optimal_flag = True
        if optimal_flag:
            return self.ORC
        else:
            return self.RC

    def stateParallRepairCost(self, state):
        if not self.isRepairable(state):
            return -1

        fail = state.count(0)
        if fail == 1:
            return self.ORC
        else:
            return self.RC + fail - 1


if __name__ == "__main__":
    lrc = XORBAS([10, 6, 2])
    # print lrc.isMDS
    # print lrc.DSC
    # print lrc.SSC
    # print lrc.ORC
    # print lrc.RC

    state = [1, 1, 0, 1, 1, 1, 1, 1, 0, 1]
    print lrc.isRepairable(state)
    print "single repair cost is:", lrc.stateRepairCost(state, 8)
    print lrc.stateParallRepairCost(state)
