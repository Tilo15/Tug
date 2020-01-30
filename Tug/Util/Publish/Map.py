from Tug.Artefacts.Map import Map
from Tug.Artefacts.Map.Destination import Destination
from Tug.Artefacts.Reference import Reference
from Tug import Artefacts
from Tug.Util.Publish import File

def Create(structure):
    # Somewhere to hold the destinations
    destinations = []

    # Loop over all the items
    for key, value in structure.items():
        # What type of object do we have?
        if(isinstance(value, Destination)):
            # Object is already a destination
            destinations.append(value)

        elif(isinstance(value, dict)):
            # Object is a dictionary, create another map
            final = None
            for artefact in Create(value):
                yield artefact
                final = artefact

            # Create destination for map
            destinations.append(Destination(key, Artefacts.ARTEFACT_MAP, Reference(final.checksum, final.size)))

        else:
            # Object must be a file (probably)
            final = None
            for artefact in File.Create(value):
                yield artefact
                final = artefact
            
            # Create destination for file
            destinations.append(Destination(key, Artefacts.ARTEFACT_FILE, Reference(final.checksum, final.size)))

    # Create the actual map
    yield Map(destinations)

            

