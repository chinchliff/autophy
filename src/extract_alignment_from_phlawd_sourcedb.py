#!/usr/bin/env python
import autophy, sys, re, os, subprocess
from copy import deepcopy as copy

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print "usage: extract_alignment_from_phlawddb.py <configfile>"

#<db> rank=<rank> [include=\"<tx1>,<tx2>,...\"] [includefile=<file>] [exclude=\"<tx3>,<tx4>,...\"] [genes=\"<gene1>,<gene2>,...\"] [genesfile=<file>] [consense=<Y>]"
        sys.exit(0)

    # process command line args
#    dbname = sys.argv[1]

    configfile = open(sys.argv[1])

    # initializing parameters
    consense = False
#    target_rank = ""
    includes = {}
    exclude_names = []
    gene_names = {}

    # set optional parameters
    for line in configfile:
        if len(line.strip()) < 1:
            continue

        argname, argval = line.split("=")

        if argname == "db":
            dbname = argval.strip()

        if argname == "include":
            rank, cladestr = [n.strip() for n in argval.split(":")]
            cladenames = [n.strip() for n in cladestr.split(",")]
            if includes.has_key(rank):
                includes[rank] += cladenames
            else:
                includes[rank] = cladenames

#        elif argname == "includefile":
#            includenamesfile = open(argval,"r")
#            cladenames = [n.strip() for n in includenamesfile.readlines()]
#            includenamesfile.close()

        elif argname == "exclude":
#            excludestr [n.strip() for n in argval.split(":")]
            exclude_names = [n.strip() for n in argval.split(",")]
#            if excludes.haskey(rank):
#                excludes[rank] += cladenames
#            else:
#                excludes[rank] = cladenames

        elif argname == "genes":
            gene_names_raw = [n.strip() for n in argval.split(",")]
            gene_names = dict(zip(gene_names_raw,["",]*len(gene_names_raw)))

#        elif argname == "genesfile":
#            genenamesfile = open(argval,"r")
#            gene_names_raw = [n.strip() for n in genenamesfile.readlines()]
#            genenamesfile.close()
#            gene_names = dict(zip(gene_names_raw,["",]*len(gene_names_raw)))

        elif argname == "consense":
            if argval == "Y":
                consense = True

    assert(len(includes) > 0)
#    assert(target_rank != "")

    db = autophy.Database(dbname)
    taxonomy = autophy.Taxonomy(dbname)

    # get taxa to exclude
    excluded_taxa = []
    print "excluding:"
    for name in exclude_names:
        
        print "    " + name
        taxon = taxonomy.get_taxon_by_name(name)
        excluded_taxa.append(taxon)

    # get all included taxa of specified rank
    target_children_ncbi_ids = list()
    print "including:"
    for target_rank, cladenames in includes.iteritems():
        for name in cladenames:

            if name == "":
                continue

            try:
                taxon = taxonomy.get_taxon_by_name(name)
            except NameError:
                continue

            print "    " + target_rank + " in " + name

            if taxon.node_rank != target_rank:
                # get children, add them to list
                target_children = taxon.get_depth_n_children_by_rank(target_rank,excluded_taxa=excluded_taxa)
                if len(target_children) > 0:
                    target_children_ncbi_ids += list(zip(*target_children)[1])
            else:
                target_children_ncbi_ids += [taxon.ncbi_id]            

    # find the identified phlawdruns (if any)
    target_phlawdrun_ids = list()
    if len(gene_names) > 0:
        print "only including loci:"
        for p_name, p_id in db.phlawdruns:
            p_name = p_name.rsplit(".phlawd",1)[0]
            if p_name in gene_names:
                print "    " + p_name
                target_phlawdrun_ids.append(p_id)

        # uniquify the list of ids and store it in a dict for fast access
        target_phlawdrun_ids = set(target_phlawdrun_ids)
        target_phlawdrun_ids = dict(zip(target_phlawdrun_ids,["",]*len(target_phlawdrun_ids)))

    if consense:
        # will hold all seqs for each taxon/gene, which will be used to build a consensus used as an exemplar
        allseqs_allgenes_alltaxa = dict()

    # will hold the final seq for each taxon/gene. will either be the longest or the consensus of all
    exemplars = dict()

    i = 1
    ntax_total = len(target_children_ncbi_ids)
    for target_ncbi_id in target_children_ncbi_ids:

        target = taxonomy.get_taxon_by_ncbi_id(target_ncbi_id)
        seq_ids = target.get_all_child_sequence_ids()
        print "Taxon " + str(i) + "/" + str(ntax_total) + " " + target.scientific_name + " (" + str(len(seq_ids)) + " seqs total)"

        # will hold just seqs for this target
        target_seqs = dict()

        # process all available seqs for this target
        # iterate over all child seqs simultaneously; faster than doing phlawdruns individually
        for sid in seq_ids:
            candidate = db.get_sequence_by_id(sid)

            if len(target_phlawdrun_ids) > 0:
                if candidate.phlawdrun_id not in target_phlawdrun_ids:
                    continue

            if consense:
                
                # add an entry for this phlawdrun if it hasn't been seen yet
                if candidate.phlawdrun_id not in target_seqs.keys():
                    target_seqs[candidate.phlawdrun_id] = []
 
                # add this sequence to the target seqs
                target_seqs[candidate.phlawdrun_id].append(candidate)

            else:

                # if this is the first seq from this phlawdrun, record it
                if candidate.phlawdrun_id not in target_seqs.keys():
                    target_seqs[candidate.phlawdrun_id] = candidate

                else:
                    # record the seq if it is better than the prev recorded seq
#                    if len(candidate.seq_aligned) > len(target_exemplar_seqs[cur_seq.phlawdrun_id]):
                    if len(candidate.seq_aligned) > len(target_seqs[candidate.phlawdrun_id].seq_aligned):
                        target_seqs[candidate.phlawdrun_id] = candidate

        if consense:

            # make consensus seqs for all seqs from each gene and add them to the dict 
            target_consensus_seqs = dict()

            for phlawdrun_id, phlawdrun_seqs in target_seqs.iteritems():

                if len(phlawdrun_seqs) == 1:
                    target_consensus_seqs[phlawdrun_id] = phlawdrun_seqs[0]
                    continue

                temp_path = "extract_seqs_TEMP"
                try:
                    os.mkdir(temp_path)
                except OSError:
                    pass

                print "consensing " + str(len(phlawdrun_seqs)) + " seq from phlawdrun " + str(phlawdrun_id)

                # write all seqs for this taxon/phlawdrun to a file 
                temp_file_path = temp_path + "/" + str(phlawdrun_id) + ".allseqs.fasta"
                temp_file = open(temp_file_path,"w")
                for index, seq_obj in enumerate(phlawdrun_seqs):
                    temp_file.write(">" + str(index) + "\n")
                    temp_file.write(seq_obj.seq_aligned + "\n")
                temp_file.close()

                # use pxconseq to make consensus
                pxconseq_args = ["pxconseq", "-s", temp_file.name]
                p1 = subprocess.Popen(pxconseq_args, stdout=subprocess.PIPE)
                pxconseq_out = p1.communicate()[0]
                consensus = pxconseq_out.split(">consensus\n")[1].strip()

                # hack a preexisting Sequence object to avoid having to create a new one in the db
                conseq = copy(phlawdrun_seqs[0])
                conseq.dbid = "(consensus)"
                conseq.gi = "(consensus)"
                conseq.seq = ""
                conseq.ncbi_tax_id = target_ncbi_id
                conseq.taxon_name = target.scientific_name

                # store the consensus sequence in the hacked Sequence obj
                conseq.seq_aligned = consensus

                # store the hacked seq obj in the consensus dict
                target_consensus_seqs[phlawdrun_id] = conseq

            # replace the full dict of all seqs with just the consensus seqs
            target_seqs = target_consensus_seqs
        
        # add this target's seqs to the master dict
        exemplars[target.scientific_name] = target_seqs
        i += 1

    if len(cladenames) > 1:
        fname_base = cladenames[0] + "_etc"
    else:
        fname_base = cladenames[0]
    fname_base += "_by_" + target_rank

    # prepare to write sequence metadata and tally counts of seqs/taxa
    recordfile_name = fname_base + "_metadata.csv"
    print "writing metadata to " + recordfile_name
    recordfile = open(recordfile_name, "w")
    recordfile.write("exemplified_taxon, gene, exemplar_taxname, exemplar_ncbi_tax_id, exemplar_ncbi_gi\n")
    ntax = 0
    phlawdrun_lengths = dict()

    # for each taxon, get all its sequences
    for exemplified_taxon, curtax_seqs in exemplars.iteritems():

        if len(curtax_seqs) < 1:
            continue

        for pid, seq in curtax_seqs.iteritems():
                
            # record each sequence's metadata
            phlawdrun = db.get_phlawdrun_by_id(pid)
            recordfile.write(exemplified_taxon.replace(" ","_") + "," + phlawdrun.gene_name  + "," + seq.taxon_name + "," + \
                                 str(seq.ncbi_tax_id) + "," + str(seq.gi) + "\n")

            # remember all unique phlawdruns across all seqs
            if phlawdrun.gene_name not in phlawdrun_lengths.keys():
                phlawdrun_lengths[phlawdrun.database_id] = len(seq.seq_aligned)

        ntax += 1
    recordfile.close()

    # prepare to write alignment
    alignfile_name = fname_base + "_alignment.phy"
    print "writing alignment to " + alignfile_name
    alignment = open(alignfile_name, "w")
    alignment.write(str(ntax) + " " + str(sum(phlawdrun_lengths.values()))+ "\n")

    # for each taxon, get all its sequences
    for exemplified_taxon, curtax_seqs in exemplars.iteritems():

        # only record taxa for which we've found seqs
        if len(curtax_seqs) < 1:
            continue
        else:
            alignment.write(re.sub(r"[ ,./;'\[\]<>?|:\"\\{}!@#$%^&*()-+=~`]+", "_", exemplified_taxon) + " ")

        # for every phlawdrun, write a seq if we have one, or missing data if not
        for pid in phlawdrun_lengths.iterkeys():
            if pid in curtax_seqs.keys():
                alignment.write(curtax_seqs[pid].seq_aligned)
            else:
                alignment.write("-"*phlawdrun_lengths[pid])
        alignment.write("\n")

    # write partitions file
    partfile_name = fname_base + "_partitions.part"
    print "writing raxml partitions file to " + partfile_name
    partfile = open(partfile_name, "w")
    i = 0
    for pid in phlawdrun_lengths.iterkeys():
        phlawdrun = db.get_phlawdrun_by_id(pid)
        partfile.write("DNA, " + phlawdrun.gene_name + " = " + str(i+1) + "-" + str(i+phlawdrun_lengths[pid]) + "\n")
        i += phlawdrun_lengths[pid]

    print "done"
