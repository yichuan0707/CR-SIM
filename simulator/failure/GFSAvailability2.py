from simulator.failure.Piecewise import Piecewise


class GFSAvailability2(Piecewise):
    avail_table = [0.1, 0.16, 0.66, 1.6, 6.6, 16, 32, 48, 166]
    #                60%  80%  85%    90%   92%   99%  99.9%  1
    freq_table = [0, 0.6, 0.2, 0.05, 0.05, 0.02, 0.07, 0.001, 1]

    @classmethod
    def GFSAvailability2(cls):
        super(GFSAvailability2, cls).Piecewise(GFSAvailability2.freq_table,
                                               GFSAvailability2.avail_table)
