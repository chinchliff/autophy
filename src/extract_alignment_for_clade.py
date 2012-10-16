#!/usr/bin/env python
import autophy, sys, os, subprocess, time, random

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "usage: extract_alignment_for_clade.py <db> <clade>"
        sys.exit(0)

    # opening a file to nowhere for unwanted output
    oblivion = open("/dev/null", "wb")

    # setting general parameters
    overwrite = False
    n_threads = "16"

    # process command line args
    dbname = sys.argv[1]
    cladename = sys.argv[2]

    # get info for subtree of interest; setting root node manually
    t = autophy.Taxonomy(dbname)
    taxon = t.get_taxon_by_name(cladename)

    db = autophy.Database(dbname)

    # get all child species for current taxon
    child_species = taxon.get_depth_n_children_by_rank("species")
    try:
        child_species_ids = zip(*child_species)[1]
    except IndexError:
        print "found no child species of clade '" + cladename + "'. quitting";
        exit

    # define temp matrix name and description
    temp_matrix_name = taxon.scientific_name + "_TEMP_all_seqs"
    temp_matrix_description = "temporary matrix; should have been removed. should be removed now"

    # if a matrix with this name already exists, delete it
    db.update_matrix_list()
    try:
        if temp_matrix_name in zip(*db.matrices)[0]:
            db.remove_matrix_by_name(temp_matrix_name)
    except IndexError:
        pass

    # create new matrix containing all seqs for species gathered above
    matrix = db.create_matrix(temp_matrix_name, temp_matrix_description, matrix_type="temporary",  \
                                  included_taxa = child_species_ids, overwrite = overwrite)

    # in the case that we find no sequences for this taxon, move on
    if matrix == None:
        print "found no sequences found for " + taxon_name + ". this is weird..."
        exit

    matrix.export_alignment(".")

    db.remove_matrix_by_name(temp_matrix_name)
