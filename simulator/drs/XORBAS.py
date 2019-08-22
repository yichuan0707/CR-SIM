from copy import deepcopy

from simulator.UnitState import UnitState
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
        if parity_group.count(UnitState.Normal) == len(parity_group) - 1:
            for i, s in enumerate(parity_group):
                if s != UnitState.Normal:
                    new_state[self.k + i] = UnitState.Normal
                    break
        return super(XORBAS, self).isRepairable(new_state)

    def repair(self, state, index):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")
        if state[index] == UnitState.Normal:
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

        if group.count(UnitState.Normal) >= b:
            optimal_flag = True
        if index >= self.k and len(parity_group) - parity_group.count(UnitState.Normal) == 1:
            optimal_flag = True

        state[index] = UnitState.Normal
        if optimal_flag:
            return self.ORC
        else:
            return self.RC

    def parallRepair(self, state, only_lost=False):
        if not self.isRepairable(state):
            raise Exception("state can not be repaired!")

        repair_index = []
        for i in xrange(self.n):
            if state[i] == UnitState.Corrupted or state[i] == UnitState.LatentError:
                state[i] = UnitState.Normal
                repair_index.append(i)
            if not only_lost and state[i] == UnitState.Crashed:
                state[i] = UnitState.Normal
                repair_index.append(i)

        repair_amount = len(repair_index)
        if repair_amount == 0:
            return 0
        elif repair_amount == 1:
            return self.ORC
        else:
            return self.RC + repair_amount - 1


if __name__ == "__main__":
    lrc = XORBAS([10, 6, 2])
    # print lrc.isMDS
    # print lrc.DSC
    # print lrc.SSC
    # print lrc.ORC
    # print lrc.RC

    state = [UnitState.Normal]*10
    state[3] = UnitState.Crashed
    state[4] = UnitState.Corrupted
    state[5] = UnitState.LatentError
    state[7] = UnitState.Corrupted
    print lrc.isRepairable(state)
    print "single repair cost is:", lrc.repair(state, 7)
    print lrc.parallRepair(state, False)
