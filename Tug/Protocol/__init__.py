from Tug import Storage
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
REQUEST_RELATED_AVAILABLE = b"\x02"
RESPONSE_ARTEFACT = b"\x01"
RESPONSE_RELATED_AVAILABLE = b"\x02"


def in_thread(func):
    def threaded(*args):
        threading.Thread(target=func, args=args).start()

    return threaded


class Protocol:
    
    def __init__(self, store: Storage.Storage):
        # Keep reference to storage object
        self.store = store

        # Keep an artefact location cache
        self.artefact_locations = {}

        # Keep collection of active control streams
        self.control_streams = {}

        # Keep a collection of subjects related to artefacts
        self.artefact_subjects = {}

        # Keep a map of storage policies for artefacts
        self.artefact_policy = {}

        # Initialise the LibPeer application
        self.manager = InstanceManager("tug")

        # Listen for streams
        self.manager.new_stream.subscribe(self.handle_stream)

        # Advertise initial artefact checksums we offer
        for checksum in self.store.get_artefact_checksums():
            # Advertise with label
            self.manager.resources.add(checksum)
        
        print("Added checksums to resources")
    
    @in_thread
    def handle_stream(self, stream):
        # TODO cleanup
        while True:
            # Read the request type
            request = stream.read(1)

            if(request == REQUEST_ARTEFACT):
                # Read the checksum from the stream
                checksum = stream.read(32)

                def respond():
                    print("Received request for artefact {}".format(Checksum.stringify(checksum)))

                    # Check if we have the artefact
                    has_artefact = self.store.has_artefact(checksum)

                    # Do we have the artefact?
                    if(has_artefact):
                        # Yes, request a stream
                        self.manager.establish_stream(stream.origin, in_reply_to=stream.id).subscribe(lambda x: self.handle_reply_stream(x, checksum))

                threading.Thread(target=respond, name="Tug Response Thread").start()

            elif(request == REQUEST_RELATED_AVAILABLE):
                # Read the checksum from the stream
                checksum = stream.read(32)

                def respond():
                    print("Received request for listing of available related artefacts")

                    # Check if we have the artefact
                    has_artefact = self.store.has_artefact(checksum)

                    if(has_artefact and len(self.store.get_artefact(checksum).get_related()) > 0):
                        # Open a reply
                        self.manager.establish_stream(stream.origin, in_reply_to=stream.id).subscribe(lambda x: self.handle_listing_stream(x, checksum))

                threading.Thread(target=respond, name="Tug Response Thread").start()


    @in_thread
    def handle_reply_stream(self, stream, checksum):
        print("Established reply stream for artefact {}".format(Checksum.stringify(checksum)))

        # Get the object
        artefact = self.store.get_artefact(checksum)

        # Get a sendable
        sendable = artefact.as_sendable()

        # Write the response type anc checksum
        stream.write(RESPONSE_ARTEFACT + checksum)

        # Send the artefact
        for chunk in sendable:
            if(len(chunk) > 0):
                stream.write(chunk)

        stream.close()

    @in_thread
    def handle_listing_stream(self, stream, checksum):
        print("Established reply stream for artefact listing")

        # Prepare list of references
        references = self.store.get_artefact(checksum).get_related()

        # Get list of checksums we have
        checksums = set(self.store.get_artefact_checksums())

        # Get a list of available references
        available_references = [x.checksum for x in references if x.checksum in checksums]

        # Send checksums
        stream.write(RESPONSE_RELATED_AVAILABLE + struct.pack("!H", len(available_references)) + b"".join(available_references))
        stream.close()

    
    def retrieve_artefact(self, checksum, storage_policy = Storage.STORAGE_PARTICIPATORY, peers_to_try = 10):
        # TODO Remove
        print("Retreive {}".format(Checksum.stringify(checksum)))

        # Save storage policy
        self.artefact_policy[checksum] = storage_policy

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

        # Do we already have a subject for this artefact?
        if(checksum in self.artefact_subjects):
            # Return that
            return self.artefact_subjects[checksum]

        # Save the subject
        self.artefact_subjects[checksum] = subject

        # Callback for when a stream has been established with a resource peer
        def on_stream_established(stream):
            print("Established connection with resource peer")
            # Subscribe to the stream reply
            stream.reply.subscribe(self.on_peer_response)

            # Store reference to control stream
            self.control_streams[stream.target] = stream

            # Request the artefact
            self.request_artefact(checksum, stream)

        # Callback for when resource peers have been contacted
        def on_resource_peer(peer, from_cache = False):
            # Do we already have a connection open with this peer?
            if(peer in self.control_streams):
                # Request the artefact
                print("Already connected to a peer with this resource")
                self.request_artefact(checksum, self.control_streams[peer])
            
            else:    
                # Establish a stream with the peer
                print("Establishing connection to resource peer")
                self.manager.establish_stream(peer).subscribe(on_stream_established)

        # Do we already know peers with this artefact?
        if(checksum in self.artefact_locations):
            print("Found peer with {} from cache".format(Checksum.stringify(checksum)))
            # Try directly
            for peer in self.artefact_locations[checksum]:
                on_resource_peer(peer)

        else:
            print("Searching for peer with {}".format(Checksum.stringify(checksum)))
            # Find peers claiming to have the artefact
            self.manager.find_resource_peers(checksum).subscribe(on_resource_peer)

        return subject


    def request_artefact(self, checksum, stream):
        # Ask the peer for the artefact
        stream.write(REQUEST_ARTEFACT + checksum)

        # Also ask peer for a listing of related artefacts
        stream.write(REQUEST_RELATED_AVAILABLE + checksum)

    @in_thread
    def on_peer_response(self, stream):
        # Read the type of reply
        response_type = stream.read(1)

        # Are we receiving an artefact?
        if(response_type == RESPONSE_ARTEFACT):
            # Read the checksum of the incoming artefact
            checksum = stream.read(32)

            # Do we have a subject for this artefact?
            if(checksum not in self.artefact_subjects):
                stream.close()
                return

            # Get the subject
            subject = self.artefact_subjects[checksum]

            print("Reading data from resource peer")
            try:
                # Create an artefact
                artefact = ArtefactFactory.deserialise(stream)

                # Save the artefact
                self.store.save_artefact(self.artefact_policy[checksum], artefact)

                # Return the artefact to the subject
                subject.on_next(artefact)
                
            except Exception as e:
                subject.on_error(e)

        elif(response_type == RESPONSE_RELATED_AVAILABLE):
            # We are receiving related available artefacts from this peer
            print("Reading artefact listing")
            # Read number of items
            items = struct.unpack("!H", stream.read(2))[0]

            # Add artefact peer
            for i in range(items):
                checksum = stream.read(32)
                self.add_artefact_peer(checksum, stream.origin)


    def add_artefact_peer(self, checksum, peer):
        if(checksum not in self.artefact_locations):
            self.artefact_locations[checksum] = set()

        self.artefact_locations[checksum].add(peer)
        
