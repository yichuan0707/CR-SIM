from copy import deepcopy
from itertools import combinations

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
        if len(state) != self.n:
            raise Exception("State Length Error!")

        avails = state.count(1)
        # all blocks is available, don't neeed repair
        if avails == self.n:
            return True
        if avails < self.k:
            return False

        db = self.k/self.ll
        local_indexes = [[j for j in xrange(i*db, (i+1)*db)]+
                         [a for a in xrange(self.k+i*self.m0, self.k+(i+1)*self.m0)]
                         for i in xrange(self.ll)]
        parity_index = [j for j in xrange(self.k, self.n)]
        global_index = [i for i in xrange(self.k)] + [j for j in xrange(self.n-self.m1, self.n)]

        new_state = deepcopy(state)
        tmp_state = []

        while new_state != tmp_state:
            tmp_state = deepcopy(new_state)
            # data blocks in each local group
            local_groups = [[] for i in xrange(self.ll)]
            parity_group = []
            global_group = []
            for index in xrange(self.n):
                if index < self.k:
                    local_group_id = index/db
                    local_groups[local_group_id].append(new_state[index])
                    global_group.append(new_state[index])
                elif self.k <= index < self.n - self.m1:
                    local_group_id = (index - self.k)/self.m0
                    local_groups[local_group_id].append(new_state[index])
                    parity_group.append(new_state[index])
                else:
                    parity_group.append(new_state[index])
                    global_group.append(new_state[index])

            for i, group in enumerate(local_groups):
                if group.count(1) >= len(group) - self.m0:
                    for index in local_indexes[i]:
                        new_state[index] = 1
            if parity_group.count(1) == len(parity_group) - 1:
                for index in parity_index:
                    new_state[index] = 1
            if global_group.count(1) >= len(global_group) - self.m1:
                for index in global_index:
                    new_state[index] = 1
            if new_state.count(1) == self.n:
                return True
        return False

    def repair(self, state, index):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")
        if state[index] == 1:
            raise Exception("index:" + str(index) + " in " +str(state) + " is normal state")

        optimal_flag = False
        b = self.k/self.ll
        if 0 <= index < b or self.k <= index < self.k+self.m0:
            group = state[0:b] + state[self.k:self.k+self.m0]
        elif b <= index < 2*b or self.k+self.m0 <= index < self.k+2*self.m0:
            group = state[b:2*b] + state[self.k+self.m0:self.k+2*self.m0]
        else:
            group = []

        parity_group = state[self.k:]

        if group.count(1) >= b:
            optimal_flag = True
        if index >= self.k and len(parity_group) - parity_group.count(1) == 1:
            optimal_flag = True

        state[index] = 1
        if optimal_flag:
            return self.ORC
        else:
            return self.RC

    def parallRepair(self, state, only_lost=False):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")

        repair_index = []
        for i in xrange(self.n):
            if state[i] == -1 or state[i] == -2:
                state[i] = 1
                repair_index.append(i)
            if not only_lost and state[i] == 0:
                state[i] = 1
                repair_index.append(i)

        repair_amount = len(repair_index)
        if repair_amount == 0:
            return 0
        elif repair_amount == 1:
            return self.ORC
        else:
            return self.RC + repair_amount - 1

def test(n, f):
    if n == 10:
        xor = XORBAS([10,6,2])
    elif n == 16:
        xor = XORBAS([16,10,2])
    else:
        raise ValueError, "Only support test with n=10 or n=16"

    com = combinations(xrange(n), f)
    total_count = 0
    repairable_count = 0
    for item in com:
        total_count += 1
        state = [1]*n
        for index in item:
            state[index] = -1
        if xor.isRepairable(state):
            repairable_count += 1
        print "state:"+ str(state) + "  Repairable: " + str(xor.isRepairable(state))
    print "repairable prob:", round(float(repairable_count)/total_count, 5)


if __name__ == "__main__":
    test(10, 4)
