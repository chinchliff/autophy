#!/usr/bin/env python
import autophy, sys, re

if __name__ == "__main__":

    if len(sys.argv) < 4:
        print "usage: extract_alignment_for_clade.py <db> \"<taxname1>,[<taxname2>],...\" <rank>"
        sys.exit(0)

    # process command line args
    dbname = sys.argv[1]
    cladenames = [n.strip() for n in sys.argv[2].split(",")]
    target_rank = sys.argv[3]
    if len(sys.argv) > 4:
        outgroupname = sys.argv[4]
    else:
        outgroupname = None

    db = autophy.Database(dbname)
    taxonomy = autophy.Taxonomy(dbname)

    # get all children of specified rank for all named taxa 
    target_children_ncbi_ids = list()
    for name in cladenames:

        print name
        taxon = taxonomy.get_taxon_by_name(name)
        if taxon.node_rank != target_rank:
            # get children, add them to list
            target_children = taxon.get_depth_n_children_by_rank(target_rank)
            target_children_ncbi_ids += list(zip(*target_children)[1])
        else:
            target_children_ncbi_ids += [taxon.ncbi_id]            

    # will hold all exemplar sequences, indexed by target taxon and phlawdrun
    exemplars = dict()

    i = 1
    ntax_total = len(target_children_ncbi_ids)
    for target_ncbi_id in target_children_ncbi_ids:

        target = taxonomy.get_taxon_by_ncbi_id(target_ncbi_id)
        seq_ids = target.get_all_child_sequence_ids()
        print "Taxon " + str(i) + "/" + str(ntax_total) + " " + target.scientific_name + " (" + str(len(seq_ids)) + " seqs total)"

        # will hold just exemplar seqs for this target
        target_exemplar_seqs = dict()

        # iterate over all child seqs simultaneously; faster than doing phlawdruns individually
        for sid in seq_ids:
            Seq = db.get_sequence_by_id(sid)
            slen = len(Seq.seq_aligned)

            # record the seq if it is better than the prev recorded seq
            if Seq.phlawdrun_id in target_exemplar_seqs.keys():
                prev_best_seq_len = len(target_exemplar_seqs[Seq.phlawdrun_id].seq_aligned)
                if slen > prev_best_seq_len:
                    target_exemplar_seqs[Seq.phlawdrun_id] = Seq

            # if this is the first seq from this phlawdrun, record it
            else:
                target_exemplar_seqs[Seq.phlawdrun_id] = Seq

        # add this target's exemplars to the master dict
        exemplars[target.scientific_name] = target_exemplar_seqs
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
