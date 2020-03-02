import Tug.Artefacts as Artefacts
from Tug.Util.Sendable import Sendable
from Tug.Artefacts.Reference import Reference

import io
import struct
import hashlib

class File(Artefacts.Artefact):

    def __init__(self, blobs, file_checksum):
        self.blob_refs = blobs
        self.file_checksum = file_checksum

        # Get a checksum of the file metadata
        hasher = hashlib.sha256()
        hasher.update(self._get_send_stream().read())

        # Each reference serialises to 40 bytes
        super().__init__(Artefacts.ARTEFACT_FILE, self._get_send_stream, 32 + len(self.blob_refs) * 40, hasher.digest())

    def _get_send_stream(self):
        return io.BytesIO(self.file_checksum + b"".join((x.serialise() for x in self.blob_refs)))

    @staticmethod
    def build(checksum, size, stream):
        # Get number of blob refs
        ref_size = size - 32
        ref_count = int(ref_size / 40)

        if(ref_size % 40 != 0):
           raise IOError("File artefact invalid size")

        # Get file checksum
        file_checksum = stream.read(32)

        refs = []
        for i in range(ref_count):
            refs.append(Reference.deserialise(stream.read(40)))

        return File(refs, file_checksum)

    
    def get_related(self):
        return list(self.blob_refs)
        