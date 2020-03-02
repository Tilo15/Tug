import Tug.Artefacts as Artefacts

import io
import hashlib

class Blob(Artefacts.Artefact):

    def __init__(self, data_stream_get, size, checksum):
        self.blob_data_stream = data_stream_get
        super().__init__(Artefacts.ARTEFACT_BLOB, data_stream_get, size, checksum)

    @staticmethod
    def build(checksum, size, stream):
        # Read the blob
        data = stream.read(size)

        # Build the checksum
        hasher = hashlib.sha256()
        hasher.update(data)

        return Blob(lambda: io.BytesIO(data), size, checksum)

    
    def get_related(self):
        # This is a blob and does not have any related artefacts
        return []
        