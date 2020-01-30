from Tug.Artefacts.Blob import Blob

from io import BytesIO

import hashlib

def Create(data):
    # Build the checksum
    hasher = hashlib.sha256()
    hasher.update(data)

    # Return the blob
    return Blob(lambda: BytesIO(data), len(data), hasher.digest())