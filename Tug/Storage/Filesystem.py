import Tug.Storage as Storage
from Tug.Artefacts.Factory import ArtefactFactory
from Tug.Util import Checksum

import os

class Filesystem(Storage.Storage):

    def __init__(self, directory, assistive_expiary = 864000):
        self.temporary_path = os.path.join(directory, "temporary")
        self.participatory_path = os.path.join(directory, "participatory")
        self.assistive_path = os.path.join(directory, "assistive")
        self.mirror_path = os.path.join(directory, "mirror")

        self.path_types = {
            Storage.STORAGE_TEMPORARY: self.temporary_path,
            Storage.STORAGE_PARTICIPATORY: self.participatory_path,
            Storage.STORAGE_ASSISTIVE: self.assistive_path,
            Storage.STORAGE_MIRROR: self.mirror_path
        }

        self.assistive_expiary = assistive_expiary

        self._create_path(self.temporary_path, True)
        self._create_path(self.participatory_path, True)
        self._create_path(self.assistive_path, False)
        self._create_path(self.mirror_path, False)


    def _create_path(self, path, delete):
        # Does the path already exist?
        exists = os.path.exists(path)

        # Path exists, and we should delete contents
        if(exists and delete):
            os.rmdir(path)
            os.mkdir(path)

        elif(not exists):
            os.mkdir(path)


    def has_artefact(self, checksum):
        # Check if we have an artefact
        return any(os.path.exists(os.path.join(x, Checksum.stringify(checksum))) for x in self.path_types.values())

    def get_storage_type(self, checksum):
        # Get the storage type
        return next(key for key, value in self.path_types.items() if os.path.exists(os.path.join(value, Checksum.stringify(checksum))))

    def get_artefact(self, checksum, expected_size = 0):
        # Get the artefact path
        path = next(path for path in (os.path.join(x, Checksum.stringify(checksum)) for x in self.path_types.values()) if os.path.exists(path))

        # Get atrefact stream
        stream = open(path, "rb")

        # Return artefact
        ArtefactFactory.deserialise(stream, expected_size)

    def save_artefact(self, storage_type, artefact):
        # Get path for artefact
        path = os.path.join(self.path_types[storage_type], Checksum.stringify(artefact.checksum))

        # Get stream to save to
        stream = open(path, "wb")

        # Write path artefact to stream
        for data in artefact.as_sendable():
            stream.write(data)

        stream.close()

    def remove_artefact(self, checksum):
        # Get the artefact storage type
        storage_type = self.get_storage_type(checksum)

        # Remove from disk
        os.remove(os.path.join(self.path_types[storage_type], Checksum.stringify(checksum)))