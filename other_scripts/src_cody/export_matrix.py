#!/usr/bin/env python

import sys, recorddb

if __name__ == "__main__":
    help_message = "usage:\n" \
        "  export_matrix.py database name=matrix_name\n" \
        "  export_matrix.py database id=matrix_id"

    if len(sys.argv) < 3:
        print help_message
        sys.exit(0)

    dbname = sys.argv[1]
    id_type = sys.argv[2].split("=")[0]

    if id_type == "name":
        m_name = sys.argv[2].split("=")[1]
        print "attempting to read matrix '" + m_name + "' from " + dbname
        m = recorddb.matrix(dbname, matrix_name = m_name)

    elif id_type == "id":
        m_id = sys.argv[2].split("=")[1]
        print "attempting to read matrix with id=" + m_id + " from " + dbname
        m = recorddb.matrix(dbname, matrix_id = m_id)

    else:
        print help_message
        sys.exit(0)

    m.export_sampling_matrix()
