import struct


class Reference:
    def __init__(self, checksum, size):
        self.checksum = checksum
        self.size = size

    def serialise(self):
        return self.checksum + struct.pack("!Q", self.size)

    @staticmethod
    def deserialise(data):
        checksum = data[:32]
        size = struct.unpack("!Q", data[32:])[0]
        return Reference(checksum, size)
        