from Tug import Storage
from Tug.Protocol.Channel import Channel
from Tug.Artefacts.Factory import ArtefactFactory
from Tug.Util import Checksum

from LibPeer.Application import Application
from LibPeer.Interfaces.DSI import DSI
from LibPeer.Interfaces.SODI import SODI

import uuid
import random
import rx
import base64
import threading


class Protocol:
    
    def __init__(self, store: Storage.Storage):
        # Keep reference to storage object
        self.store = store

        # Initialise the LibPeer application
        self.application = Application("tug")

        # SODI instance on default channel
        self.sodi = SODI(self.application)

        # Listen for solicitations
        self.sodi.solicited.subscribe(self.handle_solicitation)

        # Holds open channels
        self.channels = {}

        # Download channels
        self.dsis = {}

        # Advertise initial artefact checksums we offer
        for checksum in self.store.get_artefact_checksums():
            print("Added label for {}".format(Checksum.stringify(checksum)))
            # Advertise with label
            #self.application.add_label(checksum)

        # Make this instance discoverable
        self.application.set_discoverable(True)
        

    def handle_solicitation(self, solicitation):
        # Get checksum from solicitation
        checksum = Checksum.parse(solicitation.query)

        # Solicitation string contains hash of artefact
        has_artefact = self.store.has_artefact(checksum)

        # What channel at this peer to connect to in order to receive the artefact
        find_at_channel = None

        # Do we have the artefact?
        if(has_artefact):
            # Get the channel identifier
            find_at_channel = self.get_channel(checksum).uuid

        # Reply to the solicitation
        solicitation.reply({
            "has_artefact": has_artefact,
            "find_at_channel": base64.b64encode(find_at_channel)
        })


    def get_channel(self, checksum):
        # Do we already have a channel open for this artefact?
        if(checksum in self.channels):
            return self.channels[checksum]

        # Create a channel id
        channel_id = uuid.uuid4().bytes

        # Create the channel
        channel = Channel(channel_id, self.store.get_artefact(checksum), self.application)

        # Save the channel
        self.channels[checksum] = channel

        # Return the channel
        return channel


    def get_dsi(self, uuid):
        # Do we already have a download channel for this artefact?
        if(uuid in self.dsis):
            return self.dsis[uuid]

        # Create the data stream interface
        dsi = DSI(self.application, uuid)

        # Save DSI
        self.dsis[uuid] = dsi

        # Return DSI
        return dsi

    
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

        # Find peers claiming to have the artefact
        #discoveries = self.application.find_peers_with_label(checksum)
        discoveries = self.application.find_peers()

        if(len(discoveries) == 0):
            # If there were no discoveries, return the subject with an error
            subject.on_error(Exception("No peers found advertising themselves as having the requested artefact"))
            return subject

        # Pick random peers to solicit
        random.shuffle(discoveries)
        discoveries = discoveries[:peers_to_try]

        got_response = False
        responses = 0

        def handle_reply(reply):
            nonlocal responses
            nonlocal got_response
            nonlocal subject
            # Increment responses
            responses += 1

            try:
                # Get object from reply
                obj = reply.get_object()

                # Peer replied after another peer or doesn't have the object
                if(got_response or not obj["has_artefact"]):
                    # Have we had the number of responses we sent?
                    if(responses == peers_to_try):
                        subject.on_error(Exception("All peers tried could not deliver artefact"))
                    
                    # Stop execution
                    return

                # We have our response
                got_response = True

                def get_it():
                    nonlocal subject

                    # Get a DSI to get the artefact from
                    dsi = self.get_dsi(base64.b64decode(obj["find_at_channel"]))

                    # Connect to the peer at this channel
                    connection = dsi.connect(reply.peer)

                    # Create an artefact
                    artefact = ArtefactFactory.deserialise(connection)

                    # Save the artefact
                    self.store.save_artefact(storage_policy, artefact)

                    # Return the artefact to the subject
                    subject.on_next(artefact)

                threading.Thread(target=get_it).start()
                

            except Exception as e:
                subject.on_error(e)


        def handle_error(exception):
            nonlocal responses
            nonlocal got_response
            nonlocal subject
            # Increment responses
            responses += 1

            # Have we had the number of responses we sent?
            if(responses == peers_to_try):
                    subject.on_error(Exception("All peers tried could not deliver artefact"))

        # Loop over each peer
        for discovery in discoveries:
            # Solicit the peer
            self.sodi.solicit(discovery.peer, Checksum.stringify(checksum)).subscribe(handle_reply, handle_error)

        # Return the subject
        return subject


        



        
        
