#!/usr/bin/env python
import autophy, sys, os, subprocess, time

def decisivate(starting_matrix, guidetree_path, configfile_path, wd, dbname):

    #############################################################################################
    #
    #   currently decisivator isn't ready for the kind of action we want.
    #   when/if it is, we want to:
    #
    #   1. call decisivator to create optimized matrix
    #      - somehow avoid excluding sequences tagged as 'important' such as rbcl, atpb, matk
    #
    #   2. import the decisivator output matrix as a new intermediate matrix
    #      - record decisivator parameters into the newly saved matrix
    #      - record excluded seqs in the matrix_seq_excluded_map (reason = 'indecisive')
    #
    ############################################################################################

    # export matrix
    starting_matrix.export_to_csv(path_prefix = wd)

    print "decisivating (not)"

    # for now we are just re-exporting the original matrix as a placeholder for the one
    # that decisivator will hopefully be generating in the future.
    path_to_decisive_csv = starting_matrix.matrix_file_path

    # set import parameters
    name = os.path.basename(starting_matrix.matrix_file_path).split("_sampling_matrix.csv")[0] + "_decisivated"
    description = "step 1 optimization: this matrix is the result of decisiveness optimization on its " \
        "parent; some sequences may be excluded to improve decisiveness. parameters used were:\n" \
        "[no parameters to record; decisivator not yet integrated]"
    matrix_type = "intermediate"

    # placeholders
    indecisive_sequence_ids = list()

    # save the matrix and return the new matrix object 
    db = autophy.Database(dbname)
    decisive_matrix = db.import_matrix_from_csv(path_to_decisive_csv, name, description, matrix_type, \
                                                    excluded_sequences = indecisive_sequence_ids, \
                                                    exclude_criterion = "indecisive", overwrite = True, \
                                                    parent_id = starting_matrix.matrix_id, date = time.time())
    return decisive_matrix

def derogueify(starting_matrix, wd):

    print "starting rogue search..."
    starting_matrix.export_alignment(path_prefix = wd)
    alignment_path = starting_matrix.alignment_file_path

    # record where we are so we can get back here later
    start_dir = os.getcwd()

    # we have to change the cwd for raxml to put its output in the right place
    os.chdir(wd)

    # open an file to catch unwanted screen output
    oblivion = open("/dev/null", "wb")

    # RAxML general parameters
    model = "GTRCAT"
    n_threads = "8"
    rseed = "12345"

    # raxml input validation parameters
    output_path = os.path.basename(alignment_path).split("_alignment")[0]
    partition_path = alignment_path.split("alignment")[0] + "partitions.part"
    validation_output_path = output_path + "_validation"

    # build RAxML parameters for validating the input matrix
    args_raxml_validation = ["raxmlHPC-PTHREADS-SSE3", \
                               "-f", "c", \
                               "-s", alignment_path, \
                               "-q", partition_path, \
                               "-m", model, \
                               "-n", validation_output_path, \
                               "-T", n_threads]
    
    print "validating input alignment" 
    subprocess.call(args_raxml_validation, stdout = oblivion)

    # update alignments to use validated alignment if it exists
    alignment_path_reduced = alignment_path + ".reduced"
    if os.path.exists(alignment_path_reduced):
        alignment_path = alignment_path_reduced
        partition_path = partition_path + ".reduced"

    # set other raxml bootstrap search parameters
    n_reps = "100"
    bootstrap_output_name = output_path + "_bootstraps"

    # build RAxML parameters for bootstrap trees using fast bootstrapping
    args_raxml_bootstrap = ["raxmlHPC-PTHREADS-SSE3", \
                                "-T", n_threads, \
                                "-x", rseed, \
                                "-p", rseed, \
                                "-#", n_reps, \
                                "-m", model, \
                                "-s", alignment_path, \
                                "-q", partition_path, \
                                "-n", bootstrap_output_name]

    print "performing bootstrap search"
    subprocess.call(args_raxml_bootstrap, stdout = oblivion)

    # change the cwd back to where we started
    os.chdir(start_dir)

    # RogueNaRok parameters
    bootstrap_file = wd + "RAxML_bootstrap." + bootstrap_output_name
    roguenarok_output_name = output_path
    threshold = "50"
    dropset_size = "2"

    args_roguenarok = ["roguenarok", \
                           "-i", bootstrap_file, \
                           "-n", roguenarok_output_name, \
                           "-c", threshold, \
                           "-s", dropset_size, \
                           "-w", wd, \
                           "-T", n_threads]

    # call roguenarok
    print "derogueifying"
    subprocess.call(args_roguenarok) #, stdout = "/dev/null")

    # extract rogue taxon info from RogueNaRok output
    roguefile = open(wd + "RogueNaRok_droppedRogues." + roguenarok_output_name, "rb")
    rogue_ncbi_ids = list()
    i = 0
    for line in roguefile:
        if i < 2:
            i += 1
        else:
            try:
                taxon_list = line.split()[2].split(",")
                for taxon in taxon_list:
                    ncbi_id = taxon.split("_")[0]
                    rogue_ncbi_ids.append(ncbi_id)
            except IndexError:
                pass

    print "excluding rogue ncbi ids: " + ", ".join(rogue_ncbi_ids)

    # set import parameters
    name = os.path.basename(alignment_path).split("_alignment")[0] + "_derogued"
    description = "step 2 optimization: this matrix is the result of rogue identification on its " \
        "parent; it has been processed by RogueNaRok and some taxa may have been excluded to improve " \
        "RBIC. the following external calls were used to generate trees and identify rogues:\n" + \
        " ".join(args_raxml_validation) + "\n" + \
        " ".join(args_raxml_bootstrap) + "\n" + \
        " ".join(args_roguenarok)
    matrix_type = "optimized"

    # import the derogued matrix
    db = autophy.Database(dbname)

    # get a list of sequences from the original (decisivated) matrix
    seqs = starting_matrix.get_included_sequence_ids()

    derogued_matrix = db.create_matrix(name, description, matrix_type, included_sequences = seqs, \
                                           excluded_taxa = rogue_ncbi_ids, exclude_criterion = "rogue", \
                                           overwrite = True, parent_id = starting_matrix.matrix_id, \
                                           date = time.time())
    return derogued_matrix

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "usage: optimize_by_rank.py database_filename rank [working directory]"
        sys.exit(0)
    
    dbname = sys.argv[1]
    rank_to_optimize = sys.argv[2]
    try:
        wd = sys.argv[3].rstrip("/") + "/"
    except IndexError:
        wd = ""

    t = autophy.Taxonomy(dbname)
    root = t.get_taxon_by_name("Streptophytina")
    taxa_to_optimize = root.get_depth_n_children_by_rank(rank_to_optimize)

    db = autophy.Database(dbname)

    ###########################################################################################
    #
    #   we are taking a 3-step approach to dealing with matrix subsampling. 
    #
    #   currently, the first step in this approach is not happening; it needs to be built.
    #
    #   step 1: identify loci with high information content at deep nodes, using something
    #   like informativeness/decisiveness. record these, so that we can exclude them from
    #   subsampling by downstream methods (we want to keep them in the tree to inform the
    #   deep nodes even if they somewhat reduce the decisiveness of shallower clades...
    #   in fact as long as the taxa sampled for these deeply-informative nodes are also
    #   sampled for common, fast-evolving genes, then including them should not be
    #   expected to have much of an effect at shallow nodes.
    #
    #   step 1 contd: feed the identified deeply informative loci to decisivator in one large
    #   sampling matrix. although we cannot exclude deep nodes from the matrices to improve
    #   decisiveness as we can with species, we can:
    #      - flag nodes for which few decisive data are available in deeply-informative loci
    #      - determine and record which loci are most deeply-informative
    #
    #   step 2: feed subtrees extracted from the ncbi taxonomy and the corresponding
    #   sampling matrices to the subsampling optimization routines. so far we
    #   are thinking of using decisivator and roguenarok. curently the expectation is to
    #   split out subtrees at the family level to feed to the optimizers. after each 
    #   step in the optimization flow (e.g. decisivator), we re-import the subsampled
    #   matrix, recording the parameters used to subsample it, and then export it to
    #   send to the next optimization routine.
    #
    #   step 3: after all the subtrees have been subjected
    #   to all optimization procedures, we will gather the resulting optimized matrices
    #   and generate an aggregate matrix consisting of only those sequences retained by
    #   the optimizers. this will be exported and fed to raxml for final tree searching.
    #
    ###########################################################################################

    for ncbi_id in zip(*taxa_to_optimize)[1]:

        print "\n"
        taxon = t.get_taxon_by_ncbi_id(ncbi_id)

        # get a taxonomy tree for this taxon, export it
        guidetree = taxon.get_newick_subtree()
        guidetree_path = wd + taxon.scientific_name + "_taxonomy_guide.tre"
        guidetree_file = open(guidetree_path, "wb")
        guidetree_file.write(guidetree)

        # define matrix name and description
        temp_matrix_name = taxon.scientific_name + "_all_seqs"
        temp_matrix_description = "step 0 optimization: this matrix contains all seqs for " \
            "this taxon. it will be passed to subsampling methods for optimization."

        # get all child species for current taxon
        child_species = taxon.get_depth_n_children_by_rank("species")
        try:
            child_species_ids = zip(*child_species)[1]
        except IndexError:
            # in the case that this taxon has no child species, move to the next.
            # this actually does happen in the case of families that have been
            # synonymized (e.g. Nyssaceae -> Nyssoideae; original family is empty)
            continue

        # if a matrix with this name already exists, delete it
        db.update_matrix_list()
        if temp_matrix_name in zip(*db.matrices)[0]:
            db.delete_matrix_by_name(temp_matrix_name)

        # create new matrix containing all seqs for species gathered above
        matrix = db.create_matrix(temp_matrix_name, temp_matrix_description, \
                     matrix_type="intermediate", included_taxa = child_species_ids, overwrite = True)

        # in the case that we find no sequences for this taxon, move on
        if matrix == None:
            print "No sequences found for " + taxon_name
            continue

        decisive_matrix = decisivate(matrix, guidetree_path, configfile_path = "", \
                                         wd = wd, dbname = dbname)

        # export decisivated alignment and run roguenarok on it
        derogued_matrix = derogueify(decisive_matrix, wd)

        ### collect all optimized matrices
        ### combine them into a single matrix



        ### (probably for a different file)
        ### export the matrix
        ### run it through raxml
