from Tug.Application import Tug
from Tug.Storage.Filesystem import Filesystem
from Tug.Util import Checksum
from Tug.Artefacts.Blob import Blob
from Tug.Artefacts.File import File
from Tug.Artefacts.Map import Map
from Tug.Artefacts.Key import Key

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler

import queue
import sys

store = Filesystem(sys.argv[1])
tug = Tug(store)

class BasicHttpServer(BaseHTTPRequestHandler):

    def do_GET(self):
        self.done = False

        if(self.path == "/favicon.ico"):
            return

        # Attempt to get the artefact
        self.path_parts = self.path.split("/")[1:]
        self.path_parts.reverse()
        part = self.path_parts.pop()
        subject = tug.retrieve_artefact(Checksum.parse(part))
        subject.subscribe(self.got_artefact, self.error)

        while not self.done:
            pass

    def got_artefact(self, artefact):
        if(isinstance(artefact, Blob)):
            self.handle_blob(artefact)

        if(isinstance(artefact, File)):
            self.handle_file(artefact)

        if(isinstance(artefact, Map)):
            self.handle_map(artefact)

        if(isinstance(artefact, Key)):
            self.handle_key(artefact)

    def handle_key(self, artefact: Key):
        self.done = True
        pass

    def handle_map(self, artefact: Map):
        if(len(self.path_parts) > 0):
            part = self.path_parts.pop()
            for entry in artefact.destinations:
                if(entry.name == part):
                    subject = tug.retrieve_artefact(entry.reference.checksum)
                    subject.subscribe(self.got_artefact, self.error)
                    return
            
            self.error(Exception("A map was retrieved however the subpath '{}' could not be found".format(part)))
            return

        self.send_response(200)
        self.end_headers()
        html = """
        <h1>Map Listing for {}</h1>
        <hr/>
        <ul>
        """.format(Checksum.stringify(artefact.checksum))

        for entry in artefact.destinations:
            html += """
            <li><a href="/{}">{}</a></li>
            """.format(Checksum.stringify(entry.reference.checksum), entry.name)
        
        html += """</ul>"""
        self.wfile.write(html.encode("utf-8"))
        self.wfile.flush()
        self.done = True

    def handle_file(self, artefact: File):
        received_blobs = {}
        next_blob_to_transmit = 0
        blobs_to_receive = len(artefact.blob_refs)
        blob_order = {artefact.blob_refs[i].checksum: i for i in range(len(artefact.blob_refs))}

        def handle_blob(blob: Blob):
            nonlocal blobs_to_receive
            nonlocal next_blob_to_transmit
            
            print(blob_order[blob.checksum], Checksum.stringify(blob.checksum))
            received_blobs[blob_order[blob.checksum]] = blob
            blobs_to_receive -= 1

            while next_blob_to_transmit in received_blobs:
                print("NBTT")
                self.wfile.write(received_blobs[next_blob_to_transmit].blob_data_stream().read())
                self.wfile.flush()
                next_blob_to_transmit += 1

            print("DONE")

            if(blobs_to_receive == 0):
                self.done = True

        for reference in artefact.blob_refs:
            tug.retrieve_artefact(reference.checksum).subscribe(handle_blob)


    def handle_blob(self, artefact: Blob):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(artefact.blob_data_stream().read())
        self.wfile.flush()
        self.done = True

    def error(self, exception):
        self.send_error(404, "Could not retreive artefact '{}'".format(self.path[1:]), str(exception))
        self.done = True

print("About to do it?")
httpd = HTTPServer(('localhost', int(sys.argv[2])), BasicHttpServer)
print("Serving at http://localhost:{}/".format(int(sys.argv[2])))
httpd.serve_forever()