from Tug import Storage
from Tug.Protocol.Channel import Channel
from Tug.Artefacts.Factory import ArtefactFactory
from Tug.Util import Checksum

from LibPeer2 import InstanceManager

import uuid
import random
import rx
import base64
import threading
import struct

REQUEST_ARTEFACT = b"\x01"
REQUEST_LISTING = b"\x02"


class Protocol:
    
    def __init__(self, store: Storage.Storage):
        # Keep reference to storage object
        self.store = store

        # Keep an artefact location cache
        self.artefact_locations = {}

        # Keep track of what peers we hit the most
        self.peer_hits = {}

        # Initialise the LibPeer application
        self.manager = InstanceManager("tug")

        # Listen for streams
        self.manager.new_stream.subscribe(self.handle_stream)

        # Advertise initial artefact checksums we offer
        for checksum in self.store.get_artefact_checksums():
            # Advertise with label
            self.manager.resources.add(checksum)
        
        print("Added checksums to resources")
        

    def handle_stream(self, stream):
        # Read the request type
        request = stream.read(1)

        if(request == REQUEST_ARTEFACT):
            # Read the checksum from the stream
            checksum = stream.read(32)

            print("Received request for artefact {}".format(Checksum.stringify(checksum)))

            # Check if we have the artefact
            has_artefact = self.store.has_artefact(checksum)

            # Do we have the artefact?
            if(has_artefact):
                # Yes, request a stream
                self.manager.establish_stream(stream.origin, in_reply_to=stream.id).subscribe(lambda x: self.handle_reply_stream(x, checksum))

        elif(request == REQUEST_LISTING):
            print("Received request for artefact listing")
            # Open a reply
            self.manager.establish_stream(stream.origin, in_reply_to=stream.id).subscribe(self.handle_listing_stream)


    def handle_reply_stream(self, stream, checksum):
        print("Established reply stream for artefact {}".format(Checksum.stringify(checksum)))

        # Get the object
        artefact = self.store.get_artefact(checksum)

        # Get a sendable
        sendable = artefact.as_sendable()

        # Send the artefact
        for chunk in sendable:
            if(len(chunk) > 0):
                stream.write(chunk)

        stream.close()


    def handle_listing_stream(self, stream):
        print("Established reply stream for artefact listing")

        # Get list of checksums
        checksums = self.store.get_artefact_checksums()

        # Send checksum count
        stream.write(struct.pack("!H", len(checksums)))

        # Send checksums
        stream.write(b"".join(checksums))
        stream.close()

    
    def retrieve_artefact(self, checksum, storage_policy = Storage.STORAGE_PARTICIPATORY, peers_to_try = 10):
        # TODO Remove
        print("Retreive {}".format(Checksum.stringify(checksum)))

        # Create the subject to give to the caller
        subject = rx.subjects.ReplaySubject()

        # Do we already have the artefact in storage?
        if(self.store.has_artefact(checksum)):
            # Return that instead
            try:
                subject.on_next(self.store.get_artefact(checksum))
            except Exception as e:
                subject.on_error(e)
                
            return subject

        # Callback for when a peer replies to the request
        def on_peer_reply(stream):
            nonlocal subject
            nonlocal storage_policy

            print("Reading data from resource peer")
            try:
                # Create an artefact
                artefact = ArtefactFactory.deserialise(stream)

                # Save the artefact
                self.store.save_artefact(storage_policy, artefact)

                # Return the artefact to the subject
                subject.on_next(artefact)
                
            except Exception as e:
                subject.on_error(e)

        # Callback for when a stream has been established with a resource peer
        def on_stream_established(stream):
            print("Established connection with resource peer")
            # Subscribe to the stream reply
            stream.reply.subscribe(on_peer_reply)

            # Ask the peer for the artefact
            stream.write(REQUEST_ARTEFACT + checksum)
            stream.close()

        # Callback for when resource peers have been contacted
        def on_resource_peer(peer, from_cache = False):
            print("Discovered resource peer")
            # Establish a stream with the peer
            self.manager.establish_stream(peer).subscribe(on_stream_established)

            # Have we hit this peer 4 times already?
            if(self.hit_peer(peer) == 4):
                self.request_peer_artefact_listing(peer)

        # Do we already know peers with this artefact?
        if(checksum in self.artefact_locations):
            # Try directly
            for peer in self.artefact_locations[checksum]:
                on_resource_peer(peer)

        else:
            # Find peers claiming to have the artefact
            self.manager.find_resource_peers(checksum).subscribe(on_resource_peer)

        return subject


    def add_artefact_peer(self, checksum, peer):
        if(checksum not in self.artefact_locations):
            self.artefact_locations[checksum] = set()

        self.artefact_locations[checksum].add(peer)


    def request_peer_artefact_listing(self, peer):

        def on_reply(stream):
            # Read number of items
            items = struct.unpack("!H", stream.read(2))[0]

            # Add artefact peer
            for i in range(items):
                self.add_artefact_peer(stream.read(32))

        def on_stream(stream):
            stream.reply.subscribe(on_reply)
            stream.write(REQUEST_LISTING)
            stream.close()

        self.manager.establish_stream(peer).subscribe(on_stream)


    def hit_peer(self, peer):
        if(peer not in self.peer_hits):
            self.peer_hits[peer] = 0

        self.peer_hits[peer] += 1
        return self.peer_hits
        
