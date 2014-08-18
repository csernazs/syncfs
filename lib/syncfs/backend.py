
import hashlib
import binascii
import math

__all__ = ["Bitmap", "SHA1Bitmap", "Store", "add_bitmap", "get_bitmap"]

import os

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
    def __init__(self, init_list=None):
        if init_list is None:
            init_list = []
            
        super(Bitmap, self).__init__(init_list)
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
    type = "sha1"

bitmaps = {}

def add_bitmap(bitmap_class):
    bitmaps[bitmap_class.type] = bitmap_class

def get_bitmap(type):
    return bitmaps[type]
        
for tmp in (SHA1Bitmap,):
    add_bitmap(tmp)

del tmp
  
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


class MetaStore(object):
    def __init__(self, store_dir):
      self.store_dir = store_dir
      
