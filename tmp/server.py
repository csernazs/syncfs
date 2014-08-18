
import syncfs
import os

class ServerAPI(object):
    def __init__(self, storage):
        self.storage = storage
        self.root = syncfs.Directory()

    def create_file(self, path, bitmap):
        # path = "/foo/bar.txt"
        
        dir_parts = os.path.dirname(path).split("/")[1:]
        

        curr_dir = self.root        

        if dir_parts != [""]:
            for part in dir_parts:
                next_dir = durr_dir.entries[part]
                if not isinstance(next_dir, syncfs.Directory):
                    raise ValueError("directory does not exist: %s" % os.path.dirname(path))
                
                curr_dir = next_dir
        
        new_file = syncfs.File(os.path.basename(path))
        curr_dir.create(new_file)
        