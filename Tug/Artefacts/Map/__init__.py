import Tug.Artefacts as Artefacts
from Tug.Util.Sendable import Sendable
from Tug.Artefacts.Reference import Reference
from Tug.Artefacts.Map.Destination import Destination

import hashlib
import io
import struct

class Map(Artefacts.Artefact):
    
    def __init__(self, destinations):
        self.destinations = destinations

        # Get a checksum of the map
        hasher = hashlib.sha256()
        hasher.update(self._get_send_stream().read())

        super().__init__(Artefacts.ARTEFACT_MAP, self._get_send_stream, sum(x.get_size() for x in self.destinations), hasher.digest())


    def _get_send_stream(self):
        return io.BytesIO(b"".join(x.serialise for x in self.destinations))

    @staticmethod
    def build(checksum, size, stream):
        # Prepare an array for destinations
        destinations = []

        data_read = 0
        while(data_read < size):
            # Read the destination, and get the data read and object back
            size, destination = Destination.deserialise(stream)

            # Add to data read
            data_read += size

            # Add to destinations
            destinations.append(destination)

        # Create the object
        artefact = Map(destinations)

            
