
BODY_CHUNK_SIZE = 16384

class Sendable:

    def __init__(self, head, body, body_size, tail = b""):
        self.head = head
        self.body = body
        self.tail = tail
        self.read = 0
        self.body_size = body_size
        self.body_read = 0

    @property
    def fraction(self):
        size = len(self.head) + len(self.tail) + self.body_size
        return (self.read / float(size))

    def __iter__(self):
        yield self.head
        self.read = len(self.head)

        if(self.body is Sendable):
            for(chunk in self.body):
                yield chunk
                self.body_read += len(chunk)
                self.read += len(chunk)

        else:
            while(self.body_read < self.read):
                chunk = self.body.read(BODY_CHUNK_SIZE)
                yield chunk
                self.body_read += len(chunk)
                self.read += len(chunk)

        yield self.tail
        self.read += len(self.tail)

