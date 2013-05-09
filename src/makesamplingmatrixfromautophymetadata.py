#!/usr/bin/env python

import os
import sys
from copy import deepcopy

"""Make a binary sampling matrix--locus by taxon--out of an autophy extract_by_rank metadata file"""

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "usage: makesamplingmatrixfromautophymetadata <autophy_metadata_file> <outfile>"
        sys.exit(0)

    infile = open(sys.argv[1],"r")
    outfile = open(sys.argv[2],"w")

    print "scanning for sampled loci"
    loci = list()
    isfirstline = True
    for line in infile:
        if isfirstline:
            isfirstline = False
            continue

        locus = line.split(",")[1]
        loci.append(locus)

    # uniquify locus list through conversion to set
    loci = list(set(loci))
    loci.sort()

    # create empty row to use when populating matrix
    empty_row = dict(zip(loci, [False,] * len(loci)))

    # reopen infile for final processing
    infile = open(sys.argv[1],"r")

    print "assessing taxon by locus sampling"
    matrix  = dict()
    isfirstline = True
    for line in infile:
        if isfirstline:
            isfirstline = False
            continue

        name, locus = line.split(",")[0:2]

        if name not in matrix.keys():
            matrix[name] = deepcopy(empty_row)
        
        matrix[name][locus] = True

    print "writing sampling matrix to outfile"
    outfile.write("taxon," + ",".join(loci) + "\n")
    for name, samples in matrix.iteritems():
        outfile.write(name)

        for locus in loci:

            outfile.write(",")

            if matrix[name][locus] == True:
                outfile.write("1")
            else:
                outfile.write("0")

        outfile.write("\n")

    print "done"
