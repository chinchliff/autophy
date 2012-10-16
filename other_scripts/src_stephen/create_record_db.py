#!/usr/bin/env python

import sys,sqlite3
from subprocess import call

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print ("usage: load_database database_name division download")
		print ("\t\tdatabase_name = any database name that will be referred to later")
		print ("\t\tdivision = the division as recognized by NCBI (used for downloading)")
		print ("\t\t\texample: use pln for the plant division")
		print ("\t\tdownload = say T or F for whether to download the files")
		sys.exit(0)
	database = sys.argv[1]
	download = sys.argv[2]

#	div = sys.argv[2]
	
	if os.path.exists(database):
		print "database file exists -- please delete before running this"
		sys.exit(0)
	
	con = sqlite3.connect(database)
	curup = con.cursor()
	curup.execute("create table taxonomy (id INTEGER PRIMARY KEY,ncbi_id INTEGER,name VARCHAR(255)," \
				  "name_class VARCHAR(32), node_rank VARCHAR(32),parent_ncbi_id INTEGER,edited_name " \
				  "VARCHAR(255),left_value INTEGER,right_value INTEGER);")
	curup.execute("CREATE INDEX taxonomy_left_values on taxonomy(left_value);")
	curup.execute("CREATE INDEX taxonomy_name on taxonomy(name);")
	curup.execute("CREATE INDEX taxonomy_edited_name on taxonomy(edited_name);")
	curup.execute("CREATE INDEX taxonomy_ncbi_id_index on taxonomy(ncbi_id);")
	curup.execute("CREATE INDEX taxonomy_parent_ncbi_id_index on taxonomy(parent_ncbi_id);")
	curup.execute("CREATE INDEX taxonomy_right_values on taxonomy(right_value);")
	curup.execute("create table sequence (id INTEGER PRIMARY KEY,ncbi_id INTEGER,accession_id " \
				  "VARCHAR(128),identifier VARCHAR(40),description TEXT,seq LONGTEXT);")
	curup.execute("CREATE INDEX sequence_ncbi_id on sequence(ncbi_id);")
	curup.execute("CREATE INDEX sequence_accession_id on sequence(accession_id);")
	curup.execute("CREATE INDEX sequence_identifier on sequence(identifier);")
	con.commit()
	con.close()
	
	if download.upper()[0] == "T":
		print("downloading taxonomy")
		call(["wget", "ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz"])
		call(["tar", "-xzvf", "taxdump.tar.gz"])
	call(["python", "enter_names_dmp_pysqlite.py", database, "names.dmp", "nodes.dmp"])
	call(["python", "rebuild_tree_pysqlite.py", database])
	
	print("Taxonomy installed. Attempting to install phlawd data storage tables.")

	call(["python", "add_tables.py"
	
	#---------------------------------
	
	if download.upper()[0] == "T":
		print("downloading sequences")
		os.system("wget ftp://ftp.ncbi.nih.gov/genbank/gb"+div+"*.seq.gz")
		os.system("gunzip -d gb"+div+"*.seq.gz")
	print("loading sequences")
	os.system("./ungz_send_to_load_all_gb_files "+database+" . "+div)

	#merge old ids with new ids in sequences
	print("merging old ids with new ids")
	os.system("./merge_old_names_in_sequences "+database+" merged.dmp")

	print("done creating "+database)
