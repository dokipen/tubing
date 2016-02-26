class Apparatus(object):
    """
    Apparatus represents a tubing setup, from source to sink.
    """
    def __init__(self, source):
        self.source = source
        self.tubes = []
        self.sink = None

    def tail(self):
        """
        tail returns the last apparatii on the aparatus.
        """
        if self.sink:
            return self.sink

        elif self.tubes:
            return self.tubes[-1]

        else:
            return self.source
