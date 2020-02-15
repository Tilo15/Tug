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


class Protocol:
    
    def __init__(self, store: Storage.Storage):
        # Keep reference to storage object
        self.store = store

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
        # Read the checksum from the stream
        checksum = stream.read(32)

        print("Received request for artefact {}".format(Checksum.stringify(checksum)))

        # Check if we have the artefact
        has_artefact = self.store.has_artefact(checksum)

        # Do we have the artefact?
        if(has_artefact):
            # Yes, request a stream
            self.manager.establish_stream(stream.origin, in_reply_to=stream.id)


    def handle_reply_stream(self, stream, checksum):
        print("Established reply stream for artefact {}".format(Checksum.stringify(checksum)))

        # Get the object
        artefact = self.store.get_artefact(checksum)

        # Get a sendable
        sendable = artefact.as_sendable()

        # Send the artefact
        for chunk in sendable:
            stream.write(chunk)

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
            stream.write(checksum)
            stream.close()

        # Callback for when resource peers have been contacted
        def on_resource_peer(peer):
            print("Discovered resource peer")
            # Establish a stream with the peer
            self.manager.establish_stream(peer).subscribe(on_stream_established)

        # Find peers claiming to have the artefact
        self.manager.find_resource_peers(checksum).subscribe(on_resource_peer)

        return subject



        
        
