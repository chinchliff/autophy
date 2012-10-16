#!/usr/bin/env python

import sys,sqlite3
from sqlite3_extensions import safecursor

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print ("usage: add_tables.py [database]")
		sys.exit(0)

	database = sys.argv[1]
	
	con = sqlite3.connect(database)
	curup = con.cursor(safecursor)

	cmds = list()
	curup.pexecute("create table sequences (" \
	      "id INTEGER PRIMARY KEY, " \
	      "ncbi_id INTEGER, " \
	      "phlawdrun_id INTEGER, " \
	      "accession_id VARCHAR(128), " \
	      "gene, " \
	      "seq LONGTEXT, " \
	      "seq_aligned LONGTEXT);")

	curup.pexecute("CREATE INDEX sequences_ncbi_id on sequences(ncbi_id);")
	curup.pexecute("CREATE INDEX sequences_accession_id on sequences(accession_id);")
	curup.pexecute("CREATE INDEX sequences_phlawdrun_id on sequences(phlawdrun_id);")

	# most field names match phlawd parameters (e.g. clade, search, db, coverage, identity, etc.)
	curup.pexecute("create table phlawdrun (" \
		"id INTEGER PRIMARY KEY, " \
		"db_checksum INT, " \
		"fullpath TEXT, " \
		"filename VARCHAR(256), "\
		"clade VARCHAR(256), " \
		"clade_id INT, " \
		"search TEXT, "
		"gene VARCHAR(256), " \
		"mad REAL, " \
		"coverage REAL, " \
		"identity REAL, " \
		"db TEXT, " \
		"configtext TEXT, " \
		"keeptext LONGTEXT, " \
		"excludetext TEXT);")

	curup.pexecute("CREATE INDEX phlawdrun_clade_id on phlawdrun(clade);")
	curup.pexecute("CREATE INDEX phlawnrun_gene on phlawdrun(gene);")
	con.commit()

	curup.pexecute("CREATE TABLE matrix (" \
		"id INTEGER PRIMARY KEY, " \
		"name TEXT UNIQUE, " \
		"description LONGTEXT, " \
		"decisiveness REAL);")
	con.commit()

	curup.pexecute("CREATE TABLE sequence_matrix_map(" \
		"id INTEGER PRIMARY KEY, " \
		"matrix_id INTEGER, " \
		"sequence_id INTEGER);")
	con.commit()

	curup.pexecute("CREATE TABLE flag (" \
		"id INTEGER PRIMARY KEY, " \
		"sequence_matrix_map_id, " \
		"flagger_id, " \
		"reason_id, " \
		"timestamp);")
	con.commit()

	curup.pexecute("CREATE TABLE flag_reason (" \
		"id INTEGER PRIMARY KEY, " \
		"name, " \
		"description);")

	con.commit()
	con.close()

	exit("Tables added.")
