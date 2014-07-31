"""

So, here's the idea.

We have a repository of a filesystem stored somewhere.
The repository contains the following:
- the files with their checksum (SHA1, for example).
  Files are split into chunks and each chunk's checksum is computed. So if
  there are common parts of the files, they will have the same checksum so
  the content needs to be stored only once.

- the directory tree, where file objects are pointing to the files.



The client may requests one of the following:
- directory metadata. In this case it will receive some part of the
  directory tree. It will contain the POSIX attributes, the filenames, etc.

- bitmap of a specified file. The bitmap is a series of checksums for the
  chunks in the file.
  
- the chunk itself. The chunk is identified by its checksum.


So the client can re-construct both the directory structure and the files.

Also, and here comes the main point, the client may store both the metadata
and the bitmap (and the chunks) on the disk, locally.

So when a 3rd party makes some change on the filesystem, the server notifies
all the clients which are interested in this change (eg which cached those
data locally), so the clients can invalidate these objects.

The client may decide to keep the file content - this may be useful in the
future if the file content is reverted - or to remove the content as well.
But the main point is that if the user needs the invalidated file again, it
downloads it again.


If the change is initiated from the client, it notifies the server about the
invalidation, pretty like the same was as the server does for the client.

"""


import pdb

import os
import hashlib
from keyword import iskeyword
import binascii
import math
import collections

pjoin = os.path.join

def read_buffer(file, maxsize):
    """
    Reads maxsize sized buffers from the file.
    Yields the buffers.
    
    """
    buff = None
    while True:
        buff = file.read(maxsize)
        if buff == "":
            break
        else:
            yield buff

class Bitmap(list):
    """
    Represents one bitmap for a given file.
    
    Abstract class, needs to be inherited.
    
    Represents checksum in its binary form (not hex), inherits from list.
    
    """
    def __init__(self):
        super(Bitmap, self).__init__()
        self.chunk_size = None

    
    def calculate_digest(self, chunk):
        digest = self.alg(chunk).digest()
        return digest
    
    def add_chunk(self, chunk):
        digest = self.calculate_digest(chunk)
        self.append(digest)
        return digest


class SHA1Bitmap(Bitmap):
    """
    Specifies sha1 as an algorithm for the bitmap.
    
    """
    alg = hashlib.sha1
        

class Store(object):
    """
    
    Implements bitmap + content store.
    Stores bitmaps and chunks.
    
    """
    def __init__(self, output_dir, bitmap_class=SHA1Bitmap):
        self.output_dir = output_dir
        self.bitmap_class = bitmap_class
        
    def store_file(self, path, chunk_size=None):
        bitmap = self.bitmap_class()
        
        if chunk_size is None:
            chunk_size = self.calculate_chunk_size(os.path.getsize(path))

        file = open(path, "rb")
        
        for buff in read_buffer(file, chunk_size):
            digest = bitmap.add_chunk(buff)
            self.store_chunk(buff, digest)
            
        file.close()
        bitmap.chunk_size = chunk_size
        
        return bitmap

    def store_chunk(self, chunk, digest):
        if not os.path.isdir(self.output_dir):
            raise ValueError("No such directory: %r" % self.output_dir)

        hash_file = self.get_chunk_path(digest, True)
        
        if not os.path.isfile(hash_file):
            open(hash_file, "w").write(chunk)

    def get_chunk_path(self, digest, create_dir=True):
        hex_digest = binascii.b2a_hex(digest)
        hash_dir = pjoin(self.output_dir, hex_digest[:2], hex_digest[2:4])
        if create_dir:
            try:
                os.makedirs(hash_dir)
            except OSError, err:
                if err.errno != 17:
                    raise
                
        hash_file = pjoin(hash_dir, hex_digest)
        return hash_file
        
    
    def calculate_chunk_size(self, size):
        if size == 0:
            return 0
            
        exp = int(math.log(size, 2))
        ret_exp = int(exp*0.8)
        return min(max(2**int(exp*0.8), 4096), 131072)


    def iter_bitmap(self, bitmap):
        for digest in bitmap:
            yield self.read_chunk(digest)    
            
    def read_chunk(self, digest):
        path = self.get_chunk_path(digest, False)
        # FIXME - the whole chunk?
        return open(path, "rb").read()

        
class Struct(object):
    """
    Implements a general struct object.
    
    Very similar to named tuples, but it's more flexible.
    
    """

    params = ()
    def __init__(self, *args, **kwargs):
        for idx, kv in enumerate(self.params):
            if isinstance(kv, tuple) and len(kv) == 2:
                key = kv[0]
                value = kv[1]
            elif isinstance(kv, basestring) and not iskeyword(kv):
                key = kv
                value = None
            else:
                raise ValueError("invalid param: %s" % repr(kv))

            if hasattr(self, key):
                raise ValueError("Invalid param, attribute already exists: %r" % key)
            
            if idx<len(args):
                value = args[idx]    
            else:
                value = kwargs.get(key, value)

#            print "%s -> %s" % (key, value)
            setattr(self, key, value)
    
    def __getitem__(self, name):
        if name in self.params:
            return getattr(self, name)
        else:
            raise KeyError("No such key: %r" % name)

    def __setitem__(self, name, value):
        if name not in self.params:
            raise KeyError("Invalid key: %r" % name)
        setattr(self, name, value)

    def iteritems(self):
        for key in self.params:
            yield (key, getattr(self, key))

    def keys(self):
        return self.params
        
    def as_dict(self):
        return {k:getattr(self, k) for k in self.params}

    def __cmp__(self, other):
        if other is self:
            return 0 # same
        else:
            if isinstance(other, dict):
                return cmp(self.as_dict(), other)
            elif isinstance(other, Struct):
                return cmp(self.as_dict(), other.as_dict())
            else:
                return NotImplemented            





class FileMeta(Struct):
    """
    Stores meta information about a file.
    Note the "chunk_size" which contains the size of the chunk in bytes.
    
    """
    params = ("name",
              "mode",
              "owner",
              "group",
              "mtime",
              "ctime",
              "atime",
              "size",
              "chunk_size")
    

class File(object):
    """
    Represents a single file in the filesystem.
    
    """
    def __init__(self, meta=None, bitmap=None):
        if meta is None:
            self.meta = FileMeta()
        elif not isinstance(meta, FileMeta):
            raise TypeError("meta argument must be an instance of FileMeta")
        else:
            self.meta = meta
            
        if bitmap:
            self.bitmap = bitmap
        else:
            # FIXME - use bitmap class instead?
            self.bitmap = []
    
        
    @classmethod
    def from_dict(cls, kwargs):
        return cls(**kwargs)
        
    def update_from_path(self, path, store):
        name = os.path.basename(path)
        stat = os.stat(path)
        meta = FileMeta(name, stat.st_mode, stat.st_uid, stat.st_gid, stat.st_mtime, stat.st_ctime, stat.st_atime, stat.st_size)

        bitmap = store.store_file(path)
        
        # FIXME
        meta.chunk_size = bitmap.chunk_size
        
        self.meta = meta
        self.bitmap = bitmap
                
    def __eq__(self, other):
        if id(self) == id(other):
            return True
            
        if isinstance(other, File):
            return self.meta == other.meta and self.bitmap == other.bitmap
        else:
            return NotImplemented            

    def __ne__(self, other):
        if id(self) == id(other):
            return False

        if isinstance(other, File):
            return self.meta != other.meta or self.bitmap != other.bitmap
        else:
            return NotImplemented            
        
    def get_name(self):
        return self.meta.name

    def set_name(self, value):
        self.meta.name=value
    
    name = property(get_name, set_name)
                
        

class Directory(object):
    """
    Represents a directory in the filesystem.
    
    Contains the entries (File or Directory objects), which can be queried.
    Entries are stored in a dict, hashed by their key to make faster lookups.
    
    """
    def __init__(self, name=None, entries=None, parent=None):
        self.name = name
        self.parent = parent # this one is currently unused

        if entries is None:
            self.entries = {}
        else:
            self.entries = entries
        
        
    def create(self, file):
        if not isinstance(file, (File, Directory)):
            raise TypeError("file parameter must be File or Directory instance")
            
        if file.name in self.entries:
            raise KeyError("File already exists: %r" % file.name)
        
        self.entries[file.name] = file
        
    def remove(self, file):
        if isinstance(file, (File, Directory)):
            filename = file.name
        elif isinstance(file, basestring):
            filename = file
        else:
            raise TypeError("file parameter must be File, Directory or string instance")
            
        if filename not in self.entries:
            raise KeyError("File does not exist: %r" % filename)
        
        file = self.entries[filename]
        
        if isinstance(file, Directory) and len(file) > 0:
            raise KeyError("Directory is not empty: %r" % file.name)
        
            
        del self.entries[file.name]
        
    def __len__(self):
        return len(self.entries)
        
    def get_dirs_files(self):
        """
        Returns the directories and files in a 2-element tuple.
        First element contains the directories as a list, the other contains the files as list.
        
        Note: objects are references to the directory structure, be careful when changing their name.
        """
        dirs = []
        files = []
        for entry in self.entries.values():
            if isinstance(entry, File):
                files.append(entry)
            elif isinstance(entry, Directory):
                dirs.append(entry)
            else:
                raise TypeError("Unkown object??? %s" % repr(entry))
            
        return (dirs, files)

    def get_dir(self, name):
        return self.get_entry(name, Directory)
    
    def get_file(self, name):
        return self.get_entry(name, File)
    
    def get_entry(self, name, klass):
        retval = self.entries.get(name, None)
        if isintance(retval, klass):
            return retval
        else:
            raise KeyError("No such %s: %s" % (klass.__name__, name))
    
        
    def walk(self):
        parent = ""
        queue = collections.deque([self])
        
        while len(queue)>0:
            curr_dir = queue.popleft()
            if type(curr_dir) == str:
                parent = curr_dir
                curr_dir = queue.popleft()

            root = pjoin(parent, curr_dir.name)
            dirs, files = curr_dir.get_dirs_files()
            yield (root, dirs, files)
            if len(dirs) > 0:
                queue.append(root)
                queue.extend(dirs)
            

def scan(dir, store, ignore=None):
    """
    Adds a directory structure from the filesystem to the store, which
    must be created separately.
    
    Returns the root directory object.
    """
    if not os.path.isabs(dir) or not os.path.isdir(dir):
        raise ValueError("dir must be absolute path and directory")

    if ignore is None:
        ignore = set()
            
    root_dir = Directory(os.path.basename(dir))
        
    for name in os.listdir(dir):
        if name in ignore:
            continue
            
        full_path = pjoin(dir, name)
        if os.path.isfile(full_path):
            new_file = File()
            new_file.update_from_path(full_path, store)
            root_dir.create(new_file)
        elif os.path.isdir(full_path):
            new_dir = scan(full_path, store, ignore)
            root_dir.create(new_dir)
        else:
            raise ValueError("Unsupported file: %s" % full_path)
    
    return root_dir

