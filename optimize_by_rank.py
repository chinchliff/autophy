#!/usr/bin/env python
import autophy, sys, os

def decisivate(matrix_path, guidetree_path, configfile_path, output_prefix, dbname):

    #############################################################################################
    #
    #   currently decisivator isn't ready for the kind of action we want.
    #   when/if it is, we want to:
    #
    #   1. call decisivator to create optimized matrix
    #      - somehow avoid excluding sequences tagged as 'important' such as rbcl, atpb, matk
    #   2. import the decisivator output matrix as a new intermediate matrix
    #      - record decisivator parameters into the newly saved matrix
    #      - record excluded seqs in the matrix_seq_excluded_map (reason = 'indecisive')
    #
    ############################################################################################

    # for now we are just re-exporting the original matrix as a placeholder for the one
    # that decisivator will hopefully be generating in the future.
    path_to_decisive_csv = matrix_path

    # set import parameters
    name = os.path.basename(matrix_path).split("_sampling_matrix.csv")[0] + "_decisivated"
    description = "step 1 optimization: this matrix has been processed by decisivator; some sequences " \
        "may be excluded to improve decisiveness. parameters used were:\n[no parameters to record]"
    matrix_type = "intermediate"

    # save the matrix and return the new matrix object 
    db = autophy.Database(dbname)
    decisive_matrix = db.import_matrix_from_csv(path_to_decisive_csv, name, description, matrix_type)
    return decisive_matrix

def derogueivate(alignment, output_prefix):

    print "derogueivating"

    ### de-rogue-ivate
    # call RAxML to get bootstrap trees
    # pass bootstraps to RogueNaRok
    # create a new sampling matrix excluding RogueNaRok-selected taxa, record RogeNaRok parameters
    # return new matrix object to main function

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
    #   we are taking a 2-step approach to dealing with matrix subsampling. 
    #
    #   currently, the first step in this approach is not happening; it needs to be built.
    #
    #   first step: identify loci with high information content at deep nodes, using something
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
    #   second step: feed subtrees extracted from the ncbi taxonomy and the corresponding
    #   sampling matrices to the subsampling optimization routines. so far we
    #   are thinking of using decisivator and roguenarok. curently the expectation is to
    #   split out subtrees at the family level to feed to the optimizers. after each 
    #   step in the optimization flow (e.g. decisivator), we re-import the subsampled
    #   matrix, recording the parameters used to subsample it, and then export it to
    #   send to the next optimization routine. after all the subtrees have been subjected
    #   to all optimization procedures, we will gather the resulting optimized matrices
    #   and generate an aggregate matrix consisting of only those sequences retained by
    #   the optimizers. this will be exported and fed to raxml for final tree searching.
    #
    ###########################################################################################

    for ncbi_id in zip(*taxa_to_optimize)[1]:

        taxon = t.get_taxon_by_ncbi_id(ncbi_id)

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
                     matrix_type="intermediate", included_taxa = child_species_ids)

        # in the case that we find no sequences for this family, move on
        if matrix == None:
            print "No sequences found for " + taxon_name
            continue

        # get a taxonomy tree for this family, export it
        guidetree = taxon.get_newick_subtree()
        guidetree_path = wd + taxon.scientific_name + "_guide.tre"
        guidetree_file = open(guidetree_path, "wb")
        guidetree_file.write(guidetree)

        # export matrix and run decisivator on it
        matrix.export_to_csv(path_prefix = wd)
        decisive_matrix = decisivate(matrix.matrix_file_path, guidetree_path, configfile_path = "", \
                                         output_prefix = wd, dbname = dbname)

        # export decisivated alignment and run roguenarok on it
        decisive_matrix.export_alignment(path_prefix = wd)
        derogued_matrix = derogueivate(decisive_matrix.alignment_file_path, output_prefix = wd)

#        db.
