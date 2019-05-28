from simulator.failure.Piecewise import Piecewise


class GFSAvailability(Piecewise):
    avail_table = [0.1, 0.25, 0.5, 1, 6, 24, 144]
    freq_table = [0, 0.91, 0.083, 0.0047, 0.001, 0.00075, 0.00060, 1]

    @classmethod
    def GFSAvailability(cls):
        super(GFSAvailability, cls).Piecewise(GFSAvailability.freq_table,
                                              GFSAvailability.avail_table)
