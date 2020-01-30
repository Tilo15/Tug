
STORAGE_TEMPORARY = 0
STORAGE_PARTICIPATORY = 1
STORAGE_ASSISTIVE = 2
STORAGE_MIRROR = 3

class Storage:

    def has_artefact(self, checksum):
        raise NotImplementedError

    def get_storage_type(self, checksum):
        raise NotImplementedError

    def get_artefact(self, checksum, expected_size = 0):
        raise NotImplementedError

    def save_artefact(self, storage_type, artefact):
        raise NotImplementedError

    def remove_artefact(self, checksum):
        raise NotImplementedError

    def get_artefact_checksums(self):
        raise NotImplementedError