#!/usr/bin/env python
import autophy, sys, os, subprocess, time, random

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "usage: extract_alignment_for_clade.py <db> <searchterms>"
        sys.exit(0)

    # opening a file to nowhere for unwanted output
    oblivion = open("/dev/null", "wb")

    # process command line args
    dbname = sys.argv[1]
    searchterms = sys.argv[2].split(",")

    # get info for subtree of interest; setting root node manually
#    t = autophy.Taxonomy(dbname)
#    taxon = t.get_taxon_by_name(cladename)

    db = autophy.Database(dbname)

    phlawdrunids = list()
    for searchterm in searchterms:
        phlawdrunids += db.find_matrices_by_gene_name_search(searchterm)

#    print ids
#    exit()
#    for phlawdrun_id in ids:
#        # get all child species for current taxon
#        child_species = taxon.get_depth_n_children_by_rank("species")
#        try:
#            child_species_ids = zip(*child_species)[1]
#        except IndexError:
#            print "found no child species of clade '" + cladename + "'. quitting";
#            exit

    # define temp matrix name and description
    temp_matrix_name = "TEMP_" + "_".join(searchterms)
    temp_matrix_description = "temporary matrix; should have been removed. should be removed now"

    # if a matrix with this name already exists, delete it
    db.update_matrix_list()
    try:
        if temp_matrix_name in zip(*db.matrices)[0]:
            db.remove_matrix_by_name(temp_matrix_name)
    except IndexError:
        pass

    # create new matrix containing all seqs for species gathered above
    newmatrix = db.create_matrix(temp_matrix_name, temp_matrix_description, matrix_type="temporary",included_phlawdruns = phlawdrunids, overwrite = True)

    # in the case that we find no sequences for this taxon, move on
    if newmatrix == None:
        print "found no sequences for any of the terms " + ", ".join(searchterms) + ". this is weird..."
        exit()

    newmatrix.export_alignment(".")

    db.remove_matrix_by_name(temp_matrix_name)
