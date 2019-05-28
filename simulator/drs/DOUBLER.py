from simulator.drs.base import Base


class DOUBLER(Base):

    def __init__(self, params):
        self.r = params[2]
        super(DOUBLER, self).__init__(params)

    def _check(self):
        super(DOUBLER, self)._check()
        if self.r < 0:
            raise Exception("r must be positive integer!")
