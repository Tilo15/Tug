from Tug.Storage import Storage
from Tug.Protocol.Channel import Channel
from Tug.Artefacts.Factory import ArtefactFactory

from LibPeer.Application import Application
from LibPeer.Interfaces.DSI import DSI
from LibPeer.Interfaces.SODI import SODI

import base64
import uuid
import random
import rx


class Protocol:
    
    def __init__(self, store: Storage):
        # Keep reference to storage object
        self.store = store

        # Initialise the LibPeer application
        self.application = Application("tug")

        # Make this instance discoverable
        self.application.set_discoverable(True)

        # SODI instance on default channel
        self.sodi = SODI(self.application)

        # Listen for solicitations
        self.sodi.solicited.subscribe(self.handle_solicitation)

        # Holds open channels
        self.channels = {}

        # Download channels
        self.dsis = {}
        

    def handle_solicitation(self, solicitation):
        # Get checksum from solicitation
        checksum = base64.urlsafe_b64decode(solicitation + "=")

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
            "find_at_channel": find_at_channel
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

    
    def retrieve_artefact(self, checksum, peers_to_try = 10):
        # Find peers claiming to have the artefact
        discoveries = self.application.find_peers_with_label(checksum)

        # Pick random peers to solicit
        random.shuffle(discoveries)
        discoveries = discoveries[:peers_to_try]

        subject = rx.subjects.ReplaySubject()
        got_response = False
        responses = 0

        def handle_reply(reply):
            # Increment responses
            responses += 1

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

            # Get a DSI to get the artefact from
            dsi = self.get_dsi(obj["find_at_channel"])

            # Connect to the peer at this channel
            connection = dsi.connect(reply.peer)

            # Create an artefact
            artefact = ArtefactFactory.deserialise(connection)

        def handle_error(exception):
            # Increment responses
            responses += 1

            # Have we had the number of responses we sent?
            if(responses == peers_to_try):
                    subject.on_error(Exception("All peers tried could not deliver artefact"))

        # Loop over each peer
        for discovery in discoveries:
            # Solicit the peer
            self.sodi.solicit(discovery.peer, base64.urlsafe_b64encode(checksum)[:-1]).subscribe(handle_reply, handle_error)

        # If there were no discoveries, start with an error
        subject.on_error(Exception("No peers found advertising themselves as having the requested artefact"))

        # Return the subject
        return subject


        



        
        
