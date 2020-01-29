from Tug.Util.Sendable import Sendable

import struct

ARTEFACT_BLOB = b"B"
ARTEFACT_FILE = b"F"
ARTEFACT_MAP = b"M"
ARTEFACT_KEY = b"K"

MAGIC_NUMBER = b"TUGARTEFACT\""

class Artefact:

    def __init__(self, artefact_type, data_stream_get, size, checksum):
        self.type = artefact_type
        self.get_data_stream = data_stream_get
        self.size = size
        self.checksum = checksum

    def get_header(self):
        return MAGIC_NUMBER + self.type + self.checksum + struct.pack("!Q", self.size)

    def as_sendable(self):
        return Sendable(self.get_header(), self.get_data_stream(), self.size)
