from Tug import Storage
from Tug.Protocol import Protocol
from Tug.Protocol.Seeder import Seeder
from Tug.Protocol.Leacher import Leacher
from Tug.Artefacts import Artefact
from Tug.Artefacts.Blob import Blob
from Tug.Util.TryQueue import TryQueue

from rx.subject import ReplaySubject
from typing import Set
from queue import Queue
from threading import Thread


class Tug:

    def __init__(self, storage: Storage.Storage):
        self.storage = storage
        self.__protocol = Protocol(storage)
        self.__queue = TryQueue()
        self.__seeders: Set[Seeder] = set()
        self.__artefact_subjects = {}
        self.__alive = True
        self.__seeder_query_queues = {}
        self.__session_interests = set()

        # Attach hook for new seeders
        self.__protocol.new_seeder.subscribe(self.__handle_seeder)

        # Advertise what we have in storage
        self.__protocol.advertise_checksums(self.storage.get_artefact_checksums())


    def publish_artefact(self, artefact: Artefact):
        # Save to store using mirror policy
        self.storage.save_artefact(Storage.STORAGE_MIRROR, artefact)

        # Allow peers to discover that we have this resource
        self.__protocol.advertise_checksums([artefact.checksum])

    
    def retrieve_artefact(self, checksum):
        print("RA")
        # Do we have the artefact in storage?
        if(self.storage.has_artefact(checksum)):
            print("I already have that")
            # Yes, return that
            subject = ReplaySubject()
            subject.on_next(self.storage.get_artefact(checksum))
            subject.on_completed()
            return subject

        # Do we have a subject for this checksum?
        if(checksum not in self.__artefact_subjects):
            # No, create it
            self.__artefact_subjects[checksum] = ReplaySubject()

            # Do we have a peer that has this artefact?
            if(not self.__has_seeder_with_artefact(checksum)):
                # No, Find peers that have this artefact
                self.__protocol.discover_seeders_with(checksum)

            # Add to the queue
            self.__queue.add(checksum)

        # Return the subject
        return self.__artefact_subjects[checksum]


    def __handle_seeder(self, seeder: Seeder):
        # Create a queue for this seeder
        queue = Queue()
        self.__seeder_query_queues[seeder] = queue

        # If this seeder has any of our session interests, queue the queries for them
        for checksum in seeder.available:
            # Is this one of our session interests?
            if(checksum in self.__session_interests):
                # Yes, add it to the queue
                queue.put(checksum)

        # Add to seeders
        self.__seeders.add(seeder)

        # Run thread
        Thread(target=self.__seeder_thread, args=(seeder,), name="Leaching from Seeder").start()


    def __seeder_thread(self, seeder: Seeder):
        # Loop
        while seeder.alive:
            # Do we have an artefact to get?
            checksum = self.__queue.get(lambda x: x in seeder.available, False);
            if(checksum != None):
                print("Got checksum from TryQueue")
                # Yes, get it
                artefact = seeder.get_artefact(checksum)

                # Is this a blob?
                if(not isinstance(artefact, Blob)):
                    # No, see if this peer has related artefacts
                    seeder.query_related(checksum)

                # Handle it
                self.__handle_artefact(artefact)

            try:
                # Get the next artefact to query related artefacts for
                checksum = self.__seeder_query_queues[seeder].get(False)
                print("Got checksum from Query Queue")

                # Run the query
                checksums = seeder.query_related(checksum)

                # Are any of these a session interest now?
                for checksum in checksums:
                    if(checksum in self.__session_interests):
                        # Yes, add to the queue
                        self.__seeder_query_queues[seeder].put(checksum)

            except:
                pass

        # Clean up
        del self.__seeder_query_queues[seeder]


    def __handle_artefact(self, artefact: Artefact):
            # Is this a blob?
            if(not isinstance(artefact, Blob)):
                print("Adding to interests")
                # No, add to session interests
                self.__session_interests.add(artefact.checksum)

                # Add to the queue of related artefact queries for any seeder
                # that has this checksum
                for seeder in self.__seeders:
                    if(artefact.checksum in seeder.available):
                        self.__seeder_query_queues[seeder].put(artefact.checksum)

                # Find other peers that have this artefact so we can query them for related artefacts
                self.__protocol.discover_seeders_with(artefact.checksum)

            # Notify the subject
            subject = self.__artefact_subjects[artefact.checksum]
            subject.on_next(artefact)
            subject.on_completed()

    def __has_seeder_with_artefact(self, checksum):
        for seeder in self.__seeders:
            if(checksum in seeder.available):
                return True
        
        return False
