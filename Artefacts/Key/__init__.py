import Tug.Artefacts as Artefacts
from Tug.Util.Sendable import Sendable
from Tug.Artefacts.Reference import Reference

import io
import struct
import time
import nacl.signing

class Key(Artefacts.Artefact):
    
    def __init__(self, reference, serial_number, name, message, public_key):
        self.serial_number = serial_number
        self.reference = reference
        self.name = name
        self.message = message

        super().__init__(Artefacts.ARTEFACT_KEY, self._get_send_stream, len(self.message), public_key)


    def _get_send_stream(self):
        return io.BytesIO(self.message)

    @staticmethod
    def author(reference, name, signing_key = None, serial_number = None):
        if(serial_number == None):
            serial_number = int(time.time())

        if(signing_key == None):
            signing_key = nacl.signing.SigningKey()

        # Get public key
        public_key = signing_key.verify_key.encode()

        # Encode name
        bin_name = name.encode("UTF-8")

        # Build message to sign
        message = struct.pack("!QB", serial_number, len(bin_name)) + bin_name + self.reference.serialise() 

        # Sign message
        message = signing_key.sign(message)

        # Return the object
        return Key(reference, serial_number, name, message, public_key)

    @staticmethod
    def build(public_key, size, stream):
        # Read entire artefact into memory
        message = stream.read(size)

        # Get public key object
        verify_key = nacl.signing.VerifyKey(public_key)

        # Validate message
        data = verify_key.verify(message)

        # Unpack data
        serial_number, name_size = struct.unpack("!QB", data[:9])

        # Get name
        name = data[9:name_size + 9].decode("UTF-8")

        # Get reference
        reference = Reference.deserialise(data[name_size + 9:])

        # Construct the object
        return Key(reference, serial_number, name, message)






    