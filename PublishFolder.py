from Tug.Storage.Filesystem import Filesystem
from Tug import Storage
from Tug.Util import Publish
from Tug.Util import Checksum

import os
import sys

file_system = Filesystem("store")

def get_structure(path):
    structure = {}
    files = os.listdir(path)
    for file in files:
        file_path = os.path.join(path, file)
        if(os.path.isdir(file_path)):
            structure[file] = get_structure(file_path)

        else:
            structure[file] = open(file_path, 'rb')
    
    return structure

def publish_folder(directiory):
    structure = get_structure(directiory)

    final = None
    for artefact in Publish.Map.Create(structure):
        file_system.save_artefact(Storage.STORAGE_MIRROR, artefact)
        final = artefact

    print("tug://{}".format(Checksum.stringify(final.checksum)))

if __name__ == "__main__":
    publish_folder(sys.argv[1])


    