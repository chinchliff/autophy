#!/usr/bin/env python

import autophy, sys

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "usage import_continuous.py database source_directory"
        sys.exit(1)

    database = sys.argv[1]
    source_dir =  sys.argv[2]

    db = autophy.Database(database)

    while True:
        db.import_phlawdruns_from_dir(source_dir)
