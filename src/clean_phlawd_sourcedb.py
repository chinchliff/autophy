#!/usr/bin/python

import autophy
import sys

if len(sys.argv) < 2:
    print "usage: clean_phlawd_sourcedb.py <sourcedb_filename>"
    sys.exit(0);

db = autophy.Database(sys.argv[1])

response = raw_input("Are you sure you want to wipe the database? All tables except taxonomy will be erased?\n" \
                     "enter yes or no: ")

while response != "yes" and response != "q" and response != "quit" and response != "no":
    response = raw_input("type 'yes' to erase, or 'q', 'quit', or 'no' to quit: ")

if response == "yes":
    db.wipe()
