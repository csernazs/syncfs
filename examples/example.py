#!/usr/bin/env python

import os
import sys

script_path = os.path.realpath(sys.argv[0])
script_dir = os.path.dirname(script_path)
lib_dir = os.path.join(os.path.dirname(script_dir), "lib")

sys.path.append(lib_dir)

import syncfs
import os
pjoin=os.path.join

def main():
    store = syncfs.Store("/tmp/content")
    
    base_dir = sys.argv[1]
    dir = syncfs.scan(base_dir, store, ignore=set([".git"]))
    print dir

    status = True    
    changed = []
    for root, dirs, files in dir.walk():
        for file in files:
            full_path = pjoin(root, file.name)
            print full_path, file.meta.size, file.meta.chunk_size
            orig_path = pjoin(os.path.dirname(base_dir), full_path)
            
            if os.path.getsize(orig_path) != file.meta.size:
                status = False
            else:
                orig = open(orig_path, "rb")
                for chunk in store.iter_bitmap(file.bitmap):
                    orig_data = orig.read(len(chunk))
                    if orig_data != chunk:
                        status = False
                    
                orig.close()
    

    print "Consistency is %s" % "OK" if status else "CORRUPT"

if __name__ == "__main__":
    main()