#!/usr/bin/env python
import autophy, sys, re, os, subprocess
from copy import deepcopy as copy

if __name__ == "__main__":

    if len(sys.argv) < 4:
        print "usage: get_descendant_taxon_list.py db=<path_to_autophy_db> include=<taxon1>[,<taxon2>,etc.] rank=<target_rank> [exclude=<taxon1>,etc.]"
        sys.exit(0)

    dbname = None
    target_rank = None
    include_names = None
    exclude_names = []

    for arg in sys.argv[1:]:
        if len(arg.strip()) < 1:
            continue
            
        argname, argval = arg.split("=")

        if argname == "db":
            dbname = argval.strip()

        elif argname == "rank":
            target_rank = argval

        elif argname == "include":
            include_names = [n.strip() for n in argval.split(",")]

        elif argname == "exclude":
            exclude_names = [n.strip() for n in argval.split(",")]

    assert(dbname != None)
    assert(include_names != None)
    assert(target_rank != None)

    if len(include_names) < 1:
        print "exiting search, no names specified for included taxa"
        exit(0)

    db = autophy.Database(dbname)
    taxonomy = autophy.Taxonomy(dbname)

    # get taxa to exclude
    excluded_taxa = {}
#    print "excluding:"
    for name in exclude_names:
        
#        print "    " + name
        taxon = taxonomy.get_taxon_by_name(name)
        excluded_taxa[taxon.name] = taxon # use a dict for fast lookups on keys

    # get all included taxa of specified rank
#    print "including:"
    for name in include_names:

            if name == "":
                continue

            try:
                taxon = taxonomy.get_taxon_by_name(name)
            except NameError:
                continue

#            print "    " + target_rank + " in " + name

            if taxon.node_rank != target_rank:
                # get children, add them to list
                target_children = taxon.get_depth_n_children_by_rank(target_rank,excluded_taxa=excluded_taxa)
                if len(target_children) > 0:
                    for nid in zip(*target_children)[1]:
                        print taxonomy.get_taxon_by_ncbi_id(nid).scientific_name
