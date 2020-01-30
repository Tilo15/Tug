from Tug.Artefacts.File import File
from Tug.Artefacts.Reference import Reference
from Tug.Artefacts import BLOB_SIZE
from Tug.Util.Publish import Blob

import hashlib

def Create(stream):
    # Save blob references
    blob_refs = []

    # For building the file checksum
    hasher = hashlib.sha256()

    # Create blobs
    while True:
        # Read up to blob size data
        blob_data = stream.read(BLOB_SIZE)

        # Do we have data?
        if(not blob_data):
            break

        # Hash data
        hasher.update(blob_data)

        # Create the blob
        blob = Blob.Create(blob_data)

        # Create reference to the blob
        blob_refs.append(Reference(blob.checksum, blob.size))

        # Yield blob to caller
        yield blob

    # Finally, yield the file
    yield File(blob_refs, hasher.digest())


