#!/usr/bin/env python

import sys,sqlite3,subprocess

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print ("usage: clean_db.py [database]")
		sys.exit(0)
	
	database = sys.argv[1]

	response = " "
	while response[0] != 'y' and response[0] != 'n':
		response = raw_input("Are you sure you want to remove all non-taxonomy tables? Enter y or n: ")

	if response[0] != 'y':
		exit("No changes made.")
	else:
		
		con = sqlite3.connect(database)
		curup = con.cursor()

		curup.execute("DROP TABLE sequences;")
		curup.execute("DROP TABLE phlawdrun;")
		con.commit()
		con.close()

		response = " "
		while response[0] != 'y' and response[0] != 'n':
			response = raw_input("Non-taxonomy tables have been removed. Do you want to install empty ones? Enter y or n: ")

		if response[0] != 'y':
			exit("Only taxonomy remains.")
		else:
			subprocess.call(["python", "add_tables.py", sys.argv[1]])
			exit("success.")
