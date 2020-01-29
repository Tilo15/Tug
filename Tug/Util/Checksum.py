import base64

def stringify(checksum):
    return base64.urlsafe_b64encode(checksum)[:-1].decode("utf-8")

def parse(address):
    return base64.urlsafe_b64decode(address + "=")