from Tug.Protocol import Protocol
from Tug.Storage.Filesystem import Filesystem
from Tug.Util import Checksum

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler

store = Filesystem("store")
protocol = Protocol(store)

class BasicHttpServer(BaseHTTPRequestHandler):

    def do_GET(self):
        # Attempt to get the artefact
        subject = protocol.retrieve_artefact(Checksum.parse(self.path[1:]))
        subject.subscribe(self.got_artefact, self.error)

    def got_artefact(self, artefact):
        self.send_response(200)
        self.end_headers()
        self.wfile.write("<h1>Got artefact {}</h1><span>Size: {}</span>".format(Checksum.stringify(artefact.checksum), artefact.size))
        self.wfile.flush()

    def error(self, exception):
        self.send_error(404, "Could not retreive artefact '{}'".format(self.path[1:]), str(exception))


httpd = HTTPServer(('localhost', 8080), BasicHttpServer)
httpd.serve_forever()