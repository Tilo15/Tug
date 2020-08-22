from Tug import Storage
from Tug.Protocol.Leacher import Leacher
from Tug.Protocol.Seeder import Seeder

from rx.subject import Subject
from typing import Dict
from threading import Thread

import LibPeer2

class Protocol:
    
    def __init__(self, storage: Storage.Storage):
        # Keep reference to storage object
        self.storage = storage

        # Create a LibPeer instance manager
        self.__instance_manager = LibPeer2.InstanceManager("net.unitatem.tug")

        # Subscribe to new stream hook
        self.__instance_manager.new_stream.subscribe(self.__handle_new_stream)

        # Subjects for new seeders and leachers
        self.new_seeder = Subject()
        self.new_leacher = Subject()

        # Keep a dictionary of seeders and leachers by peer
        self.seeders: Dict[object, Seeder] = {}
        self.leachers: Dict[object, Leacher] = {}

    
    def __handle_new_stream(self, control_stream: LibPeer2.IngressStream):
        # Function to handle data stream establishment
        def data_stream_established(data_stream: LibPeer2.EgressStream):
            # Create leacher object
            leacher = Leacher(self.storage, control_stream, data_stream)

            # Add to dictionary
            self.leachers[control_stream.origin] = leacher

            # Notify the subject
            self.new_leacher.on_next(leacher)

            # Serve the leacher
            Thread(target=leacher.serve, name="Seeding to leacher").start()

        # A leacher is connected, establish a data stream
        self.__instance_manager.establish_stream(control_stream.origin, in_reply_to=control_stream.id).subscribe(data_stream_established)

    
    def discover_seeders_with(self, checksum):
        # Look for seeders that advertise the specified checksum
        self.__instance_manager.find_resource_peers(checksum).subscribe(lambda x: self.__handle_seeder(x, checksum))


    def __handle_seeder(self, peer, checksum):
        # Function to handle establishment of the control stream
        def control_stream_established(control_stream: LibPeer2.EgressStream):
            # Function to handle establishment of the data stream
            def data_stream_established(data_stream: LibPeer2.IngressStream):
                # Create the seeder
                seeder = Seeder(self.storage, control_stream, data_stream)

                # Add checksum to available set
                seeder.available.add(checksum)

                # Add to dictionary
                self.seeders[control_stream.target] = seeder

                # Notify the subject
                self.new_seeder.on_next(seeder)

            # Remote peer will establish data stream
            control_stream.reply.subscribe(data_stream_established)

        # Do we already have this peer?
        if(peer in self.seeders):
            # Yes, add this checksum to the seeders available set
            self.seeders[peer].available.add(checksum)

            # Don't establish a new connection
            return

        # Establish connection with the peer
        self.__instance_manager.establish_stream(peer).subscribe(control_stream_established)


    def advertise_checksums(self, checksums):
        # Advertise as a resource peer using the checksum as the resource identifier
        self.__instance_manager.resources.update(checksums)