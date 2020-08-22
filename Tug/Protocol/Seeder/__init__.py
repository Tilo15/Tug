from Tug import Storage
from Tug.Protocol import Control
from Tug.Artefacts.Factory import ArtefactFactory

from LibPeer2.Protocols.STP.Stream.IngressStream import IngressStream
from LibPeer2.Protocols.STP.Stream.EgressStream import EgressStream

from rx.subject import Subject

import struct

class Seeder:


    def __init__(self, storage: Storage.Storage, control_stream: EgressStream, data_stream: IngressStream):
        self.__storage = storage
        self.__control_stream = control_stream
        self.__data_stream = data_stream
        self.alive = True
        self.available = set()

    
    def query_related(self, checksum):
        # Ask the seeder what artefacts it has that are related to this one
        self.__control_stream.write(Control.CONTROL_REQUEST_RELATED_AVAILABLE + checksum)

        # Wait for the response
        self.__check_response_ok()

        # Get count of available artefacts
        count = struct.unpack("!H", self.__data_stream.read(2))[0]

        # Create a set to return
        checksums = set()

        # Read the available artefact checksums
        for i in range(count):
            # Read checksum
            artefact_checksum = self.__data_stream.read(32)

            # Add to checksums set and available set
            checksums.add(artefact_checksum)
            self.available.add(artefact_checksum)

        # Return result of query
        return checksums


    def get_artefact(self, checksum, storage_policy = Storage.STORAGE_PARTICIPATORY):
        # Ask the seeder for the artefact
        self.__control_stream.write(Control.CONTROL_REQUEST_ARTEFACT + checksum)

        # Wait for the response
        self.__check_response_ok()

        # Read the artefact
        artefact = ArtefactFactory.deserialise(self.__data_stream)

        # Save the artefact
        self.__storage.save_artefact(storage_policy, artefact)

        # Return the artefact
        return artefact


    def __check_response_ok(self):
        # Read the first byte of the response
        status = self.__data_stream.read(1)

        # Is it ok?
        if(status == Control.DATA_OK):
            # Yes
            return

        elif(status == Control.DATA_UNAVAILABLE):
            raise Exception("The seeder was unable to fufil the request")

        else:
            raise Exception("The seeder returned an unexpected response")


    def disconnect(self):
        self.__control_stream.close()
        self.__data_stream.close()