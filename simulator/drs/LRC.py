from simulator.drs.base import Base


class LRC(Base):
    """
    Only works when local parity is 1 (m0 = 1).
    """

    def __init__(self, params):
        self.n = int(params[0])
        self.k = int(params[1])
        self.ll = int(params[2])
        self.m0 = 1
        self.m1 = self.n - self.k - self.ll * self.m0

    def _check(self):
        if self.k < 0 or self.ll < 0 or self.m0 < 0 or self.m1 < 0:
            raise Exception("All coding parameters must be positive integer!")

    # Get local groups list.
    def _extract_local(self, state):
        b = self.k/self.ll
        local_groups = [state[x * b: (x + 1) * b] +
                        state[(self.k + x * self.m0):
                        (self.k + (x + 1) * self.m0)]
                        for x in xrange(self.ll)]
        return local_groups

    # Reverse function to _extrace_local()
    def _combine(self, local_groups):
        datas = []
        local_parities = []

        b = self.k/self.ll
        for item in local_groups:
            datas += item[:b]
            local_parities += item[b:]

        return datas + local_parities

    @property
    def isMDS(self):
        return False

    @property
    def ORC(self):
        return float(self.k)/float(self.ll)

    # state format: [data block states, local parity states in group1,
    # local parity in group 2, global parity blocks]
    def isRepairable(self, state):
        if isinstance(state, int):
            return False

        if len(state) != self.n:
            raise Exception("State Length Error!")

        avails = state.count(1)

        # all blocks is available, don't neeed repair
        if avails == self.n:
            return True

        if avails < self.k:
            return False

        local_groups = self._extract_local(state)

        # check global group after local repairs.
        global_group = []
        b = self.k/self.ll
        # avail_equ means equation number we can use for recovery.
        avail_equ = 0
        loss_amount = 0
        for group in local_groups:
            if group.count(0) <= self.m0:
                global_group += [1 for item in xrange(b)]
            else:
                avail_equ += self.m0
                loss_amount += group.count(0)
                global_group += group[:b]

        global_parity = state[-self.m1:]
        avail_equ += self.m1
        loss_amount += global_parity.count(0)
        global_group += global_parity
        # Available equations are no less than loss blocks means repairable.
        if avail_equ < loss_amount:
            return False
        else:
            return True

    # If state needs both local repair and global repair, which one first?
    # Maybe we need another function, return the state after repair?
    # Tentatively, we return a tuple (repair_cost, state_after_repair)in
    #  this function, this is different with MDS codes.
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
            index = state.index(0)
            if index < self.k + self.ll:
                return self.ORC
            else:
                return self.RC
        return self.RC + fail - 1


if __name__ == "__main__":
    lrc = LRC([10, 6, 2])
    #print lrc.isMDS
    #print lrc.DSC
    #print lrc.SSC
    #print lrc.ORC
    #print lrc.RC

    state = [1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1]
    state1 = [1, 1, 1, 1, 1, 0, 1, 0, 0, 1]
    print lrc.isRepairable(state1)
    print lrc.stateRepairCost(state1, 7)
    # print lrc.stateParallRepairCost(state1)
