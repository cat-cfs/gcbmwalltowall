class Transition:

    def __init__(self, age_after=-1, regen_delay=0, classifiers=None):
        self.age_after = age_after
        self.regen_delay = regen_delay
        self.classifiers = classifiers
