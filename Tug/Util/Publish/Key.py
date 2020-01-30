from Tug.Artefacts.Key import Key
from Tug.Artefacts.Reference import Reference
from Tug.Util.Publish import Map
from Tug.Util.Publish import File

import time

def Create(key, name, obj):
    # Get ready for a reference
    reference = None

    # What type of object are we dealing with?
    if(isinstance(obj, dict)):
        # Create a map
        final = None
        for artefact in Map.Create(obj):
            yield artefact
            final = artefact

        reference = Reference(final.checksum, final.size)

    elif(isinstance(obj, Reference)):
        # It's already a reference
        reference = obj

    else:
        # Assume it's a file
        final = None
        for artefact in File.Create(obj):
            yield artefact
            final = artefact

        reference = Reference(final.checksum, final.size)

    # Return the key object
    yield Key.author(reference, name, key)