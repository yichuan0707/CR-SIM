
class MixTwo(object):

    def __init__(self, params1, params2):
        self.first_dr_params = params1
        self.second_dr_params = params2

    def SSS(self):
        pass

    def isRepairable(self):
        pass


class MixTwoCombine(MixTwo):
    pass


class MixTwoWithSize(MixTwo):

    def __init__(self, params1, params2, size_threshold):
        pass


class MixTwoWithHotness(MixTwo):

    def __init__(self, params1, params2, hotness_threshold):
        pass
