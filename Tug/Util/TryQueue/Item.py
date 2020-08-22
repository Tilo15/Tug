

class Item:

    def __init__(self, value, proceeds = None):
        self.value = value
        self.proceeds: Item = proceeds
        self.preceeds: Item = None
    