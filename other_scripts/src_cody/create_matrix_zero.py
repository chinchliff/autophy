#!/usr/bin/env python

import sqlite3, sys, os
from sqlite3_extensions import safecursor
from export_matrix import export_matrix

def build_matrix_zero(dbname):

    global name_m0
    global descr_m0

    con = sqlite3.connect(dbname)
    cur = con.cursor(safecursor)
    
    cur.execute("INSERT INTO matrix(name, description) VALUES (?,?);",(name_m0, descr_m0))
    con.commit()

    cur.execute("SELECT last_insert_rowid();")
    m0_id = cur.fetchall()[0][0]

    cur.execute("SELECT id FROM sequences;")
    all_sequence_ids = [item[0] for item in cur.fetchall()]

    for seq_id in all_sequence_ids:
        cur.pexecute("INSERT INTO sequence_matrix_map (sequence_id, matrix_id) VALUES (?,?);",(seq_id, m0_id))
    
    con.commit()

    return m0_id

if __name__ == "__main__":
    while len(sys.argv) < 2:
        print "usage export_sampling_matrix.py database"
        sys.exit(0)

    name_m0 = "m0_all_sequences"
    descr_m0 = "the most inclusive sampling matrix, containing all sequences from all phlawd runs in this db."
    install_new = "n"
    export = "y"

    dbname = sys.argv[1]
    con = sqlite3.connect(dbname)
    cur = con.cursor(safecursor)

    print "Connected to %s " % os.path.realpath(sys.argv[1])

    # check if there is already an all-inclusive matrix
    cur.pexecute('SELECT id from matrix where name = ?;',(name_m0,))
    results = cur.fetchall()

    # if we find one, check if it is complete, offer to export/replace it
    if len(results) > 0:
        m0_id = results[0][0]
        replace = None
        
        # check for completeness is quick and dirty; count seqs in db and seqs in matrix, should be same
        cur.pexecute("SELECT COUNT(*) FROM (SELECT DISTINCT sequence_id FROM sequence_matrix_map WHERE matrix_id = ?);",(m0_id,))
        nrecs = cur.fetchall()[0][0]
        cur.pexecute("SELECT COUNT(id) FROM sequences;")
        nseqs = cur.fetchall()[0][0]
        if nrecs == nseqs:
            while replace != "n" and replace != "y":
                replace = raw_input("There is already a matrix containing all sequences. Would you like to REPLACE it? y/n: ")
            if replace == "n":
                export = None
                while export != "n" and export != "y":
                    export = raw_input("Would you like to EXPORT it? y/n: ")

        # if count(seqs in matrix) != count(seqs in db), replace the matrix
        else:
            print "An incomplete matrix exists. It will be replaced."
            replace = "y"

        if replace == "y":
            cur.execute("DELETE FROM matrix WHERE id = ?;",(m0_id,))
            install_new = "y"
    
    con.commit()
    con.close()

    if install_new == "y":
        # we found no all-inclusive matrix, or we just deleted it, so make a new one
        m0_id = build_matrix_zero(dbname)

    if export == "y":
        m0_fname = export_matrix(dbname, m0_id)
        print "The matrix has been exported to the file " + m0_fname
