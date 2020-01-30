import Tug.Artefacts as Artefacts
from Tug.Artefacts.Reference import Reference
import struct

class Destination:

    VALID_DESTINATIONS = [Artefacts.ARTEFACT_KEY, Artefacts.ARTEFACT_MAP, Artefacts.ARTEFACT_FILE]

    def __init__(self, name, reference_type, reference):
        if(reference_type not in Destination.VALID_DESTINATIONS):
            raise ValueError("Reference type not a valid type for a destination")
        
        self.name = name
        self.reference_type = reference_type
        self.reference = reference

    def serialise(self):
        name = self.name.encode("UTF-8")
        return struct.pack("!B", len(name)) + name + self.reference_type + self.reference.serialise()

    def get_size(self):
        return len(self.name.encode("UTF-8")) + 42

    @staticmethod
    def deserialise(stream):
        size = struct.unpack("!B", stream.read(1))[0]
        name = stream.read(size).decode("UTF-8")
        reference_type = stream.read(1)
        reference = Reference.deserialise(stream.read(40))

        return (42 + size, Destination(name, reference_type, reference))