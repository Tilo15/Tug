import Tug.Artefacts as Artefacts
from Tug.Artefacts.Key import Key
from Tug.Artefacts.Map import Map
from Tug.Artefacts.File import File
from Tug.Artefacts.Blob import Blob

import struct

class ArtefactFactory:

    TYPE_MAP = {
        Artefacts.ARTEFACT_KEY: Key,
        Artefacts.ARTEFACT_MAP: Map,
        Artefacts.ARTEFACT_FILE: File,
        Artefacts.ARTEFACT_BLOB: Blob
    }

    @staticmethod
    def deserialise(stream, expected_size = 0):
        # Make sure we have a valid artefact
        identifier = stream.read(len(Artefacts.MAGIC_NUMBER))

        if(identifier != Artefacts.MAGIC_NUMBER):
            raise IOError("Invalid magic number for artefact")

        # Read the header
        header = stream.read(41)

        artefact_type = header[0:1]
        checksum = header[1:33]
        size = struct.unpack("!Q", header[33:])[0]

        # Do we have an expected size?
        if(expected_size != 0 and expected_size != size):
            raise Exception("Artefact size is {} but deserialise was called with an expected size of {}".format(size, expected_size))

        # Is the type valid?
        if(artefact_type not in ArtefactFactory.TYPE_MAP):
            raise IOError("Invalid artefact type")

        # Build the artefact
        artefact = ArtefactFactory.TYPE_MAP[artefact_type].build(checksum, size, stream)

        # Verify checksum
        if(artefact.checksum != checksum):
            raise Exception("Artefact has an invalid checksum")

        return artefact

        
