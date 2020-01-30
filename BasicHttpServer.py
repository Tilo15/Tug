from Tug.Protocol import Protocol
from Tug.Storage.Filesystem import Filesystem
from Tug.Util import Checksum
from Tug.Artefacts.Blob import Blob
from Tug.Artefacts.File import File
from Tug.Artefacts.Map import Map
from Tug.Artefacts.Key import Key

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler

import queue

store = Filesystem("store")
protocol = Protocol(store)

class BasicHttpServer(BaseHTTPRequestHandler):

    def do_GET(self):
        # Attempt to get the artefact
        subject = protocol.retrieve_artefact(Checksum.parse(self.path[1:]))
        subject.subscribe(self.got_artefact, self.error)

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
        pass

    def handle_map(self, artefact: Map):
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

    def handle_file(self, artefact: File):

        blob_queue = queue.Queue()
        for reference in artefact.blob_refs:
            blob_queue.put(reference)

        def write_blob(blob: Blob):
            self.wfile.write(blob.blob_data_stream().read())
            self.wfile.flush()

            if(blob_queue.qsize() > 0):
                ref = blob_queue.get()
                protocol.retrieve_artefact(ref.checksum, ref.size).subscribe(write_blob, self.error)
        
        self.send_response(200)
        self.end_headers()

        ref = blob_queue.get()
        protocol.retrieve_artefact(ref.checksum, ref.size).subscribe(write_blob, self.error)


    def handle_blob(self, artefact: Blob):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(artefact.blob_data_stream().read())
        self.wfile.flush()

    def error(self, exception):
        self.send_error(404, "Could not retreive artefact '{}'".format(self.path[1:]), str(exception))


httpd = HTTPServer(('localhost', 8080), BasicHttpServer)
httpd.serve_forever()