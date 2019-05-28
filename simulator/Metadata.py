from simulator.UnitState import UnitState


class Metadata(object):

    def __init__(self, state=UnitState.Normal, last_failure_time=0.0,
                 slices=[], slice_count=0, defective_slices=None,
                 known_defective_slices=None, nonexistent_slices=None):
        self.state = state
        self.last_failure_time = last_failure_time
        self.slices = slices
        self.slice_count = slice_count
        # slices hit by latent error
        self.defective_slices = defective_slices
        # latent errors detected during scrubbing
        self.known_defective_slices = known_defective_slices
        # slices not yet recovered after disk failure
        self.nonexistent_slices = nonexistent_slices
