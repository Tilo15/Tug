from Tug.Artefacts import Artefact

from LibPeer.Interfaces.DSI import DSI

import time
import threading

CHANNEL_EXPIARY = 600

class Channel:
    
    def __init__(self, uuid, artefact: Artefact, application):
        # Save the artefact with the channel
        self.artefact = artefact

        # Also keep the UUID
        self.uuid = uuid

        # Instansiate the DSI object
        self.dsi = DSI(application, uuid)

        # Subscribe to new connections
        self.dsi.new_connection.subscribe(self.handle_connection)

        # Save a timestamp of when this channel was created
        self.timestamp = time.time()


    def handle_connection(self, connection):
        # Send all the data of the artefact
        for chunk in self.artefact.as_sendable():
            connection.send(chunk)

        # Close the connection
        connection.close()
        
