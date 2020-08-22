from Tug.Storage import Storage
from Tug.Protocol import Control

from LibPeer2.Protocols.STP.Stream.IngressStream import IngressStream
from LibPeer2.Protocols.STP.Stream.EgressStream import EgressStream

import struct

class Leacher:

    def __init__(self, storage: Storage, control_stream: IngressStream, data_stream: EgressStream):
        self.__storage = storage
        self.__control_stream = control_stream
        self.__data_stream = data_stream
        self.__alive = True

    
    def serve(self):
        # While the connection with the leacher is still open
        while self.__alive:
            # Read a command
            command = self.__control_stream.read(1)

            # What are we doing?
            if(command == Control.CONTROL_REQUEST_RELATED_AVAILABLE):
                self.__handle_related_available_request()

            elif(command == Control.CONTROL_REQUEST_ARTEFACT):
                self.__handle_artefact_request()


    def __handle_related_available_request(self):
        print("RELATED AVAILABLE REQUEST")
        # Read the checksum
        checksum = self.__control_stream.read(32)
        
        # Do we have the checksum?
        if(not self.__storage.has_artefact(checksum)):
            # No, send failure
            self.__fail_request()

        # Prepare list of references
        references = self.__storage.get_artefact(checksum).get_related()

        # Get list of checksums we have
        checksums = set(self.__storage.get_artefact_checksums())

        # Get a list of available references
        available_references = [x.checksum for x in references if x.checksum in checksums]

        print("SENDING")

        # Send the list
        self.__data_stream.write(Control.DATA_OK + struct.pack("!H", len(available_references)) + b"".join(available_references))
        print(len(available_references), len(b"".join(available_references)))


    def __handle_artefact_request(self):
        print("ARTEFACT REQUEST")
        # Read the checksum
        checksum = self.__control_stream.read(32)
        
        # Do we have the checksum?
        if(not self.__storage.has_artefact(checksum)):
            # No, send failure
            self.__fail_request()

        # Get the object
        artefact = self.__storage.get_artefact(checksum)

        # Get a sendable
        sendable = artefact.as_sendable()

        # Respond to the request
        self.__data_stream.write(Control.DATA_OK)

        # Send the artefact
        for chunk in sendable:
            if(len(chunk) > 0):
                self.__data_stream.write(chunk)


    def __fail_request(self):
        # Send failure
        self.__data_stream.write(Control.DATA_UNAVAILABLE);
