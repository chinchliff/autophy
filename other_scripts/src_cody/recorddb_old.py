import sys, sqlite3, string, re, csv, os
from copy import deepcopy



class matrix():
    def __init__(self, dbname, matrix_id=None, matrix_name=None):
        self.dbname = dbname
        self.matrix_id = matrix_id
        self.matrix_name = matrix_name

        # default values for matrix presence/absence and text manip
        self.absent_code = "0"
        self.present_code = "1"
        self.r_char = "_"
        self.empty_char = "-"
        self.alignment_suffix = "_alignment"
        self.sampling_matrix_suffix = "_sampling_matrix"
        self.partition_suffix = "_partitions"

        con = sqlite3.connect(dbname)
        cur = con.cursor()
        
        # check if matrix exists, get matrix name or id
        if self.matrix_id is not None:
            cur.execute("SELECT name FROM matrix where id = ?;",(self.matrix_id,))
            r = cur.fetchone()
            if r == None:
                print "Could not find the matrix with database id = " + self.matrix_id
                raise IOError
            else:
                self.matrix_id = r[0]

        elif self.matrix_name is not None:
            cur.execute("SELECT id FROM matrix where name = ?;",(self.matrix_name,))
            r = cur.fetchone()
            if r == None:
                print "Could not find the matrix named '" + self.matrix_name + "'"
                raise IOError
            else:
                self.matrix_id = r[0]
                
        else:
            print "Class matrix() requires the name or database id of an existing matrix to initialize"
            raise NameError

        con.close()

        # get info on taxa phlawdruns referenced by this matrix
        self.update_taxa()
        self.n_taxa = len(self.taxa)
        self.update_phlawdruns()
        self.n_phlawdruns = len(self.phlawdruns)

        # extract information about partitions in the alignment defined by this matrix
        self.update_partition_info()

        # set default values for resource-intensive properties that aren't created automatically
        self.matrix_file_path = None
        self.sampling_matrix = None

    def update_taxa(self):
        # generates a list of tuples containing information about all taxa referenced by this matrix.

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()

        # get info for all taxa represented by sequences present in this matrix
        cur.execute("SELECT ncbi_id, name FROM taxonomy WHERE " \
            "name_class = 'scientific name' AND taxonomy.ncbi_id IN (" \
            "SELECT DISTINCT ncbi_id FROM sequences WHERE sequences.id IN (" \
            "SELECT sequence_id FROM sequence_matrix_map WHERE matrix_id = ?));", (self.matrix_id,))

        self.taxa = list()
        for t in cur.fetchall():
            t_ncbi_id = t[0]
            # replace non-alphanumeric strings with underscores
            t_name = self.r_char.join(re.sub(r"[" + string.punctuation + "\s]+"," ",t[1]).split())
            t_unique_id = str(t_ncbi_id) + self.r_char + t_name
            self.taxa.append((t_unique_id, t_ncbi_id, t_name))

        con.close()
        

    def update_phlawdruns(self):
        # generates a list of tuples containing information about the phlawdruns referenced by this matrix.

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()

        # get info for all phlawdruns represented by seqs present in this matrix
        cur.execute("SELECT id, filename FROM phlawdrun WHERE phlawdrun.id IN (" \
            "SELECT DISTINCT phlawdrun_id FROM sequences WHERE sequences.id IN (" \
            "SELECT sequence_id FROM sequence_matrix_map WHERE matrix_id = ?));", (self.matrix_id,))

        self.phlawdruns = [(p_filename + self.r_char + str(p_db_id), p_db_id, p_filename) for p_db_id, p_filename in cur.fetchall()]

        con.close()
        

    def update_sampling_matrix(self):
        # creates a large 2-d dict containing sampling information for this matrix. each "row" in
        # the dict corresponds to a taxon, and contains a dict with "columns" that contain
        # presence/absence codings for each phlawdrun. the keys of the dict are unique identifiers
        # consisting of taxa/phlawdrun names concatenated with their ncbi_id or dbid, respectively

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()

        # extract unique ids stored in the first position in each tuple in the taxa and phlawdruns lists
        taxon_uids = zip(*self.taxa)[0]
        phlawdrun_uids = zip(*self.phlawdruns)[0]

        # create empty matrix: taxon by phlawdrun
        empty_row = dict(zip(phlawdrun_uids, [self.absent_code] * self.n_phlawdruns))
        # we have to deepcopy the rows individually to keep them from referencing the same object
        all_rows = map(deepcopy, [empty_row] * self.n_taxa)
        self.sampling_matrix = dict(zip(taxon_uids, all_rows))

        i = 0
        report_interval = 1000
        for taxon, ncbi_id in [(uid, nid) for uid, nid, name in self.taxa]:

            # gather sampling information from the db 
            cur.execute("SELECT phlawdrun.filename, phlawdrun.id FROM phlawdrun, sequences WHERE " \
                "phlawdrun.id = sequences.phlawdrun_id AND sequences.ncbi_id = ?;", (ncbi_id,))
            runs_present = [p[0] + self.r_char + str(p[1]) for p in cur.fetchall()]

            # fill "cells" in matrix dict according to sequences present
            for phlawdrun in runs_present:
                self.sampling_matrix[taxon][phlawdrun] = self.present_code

            i += 1
            if i % report_interval == 0:
                print "Processed %s tip taxa. %s to go." % (i, self.n_taxa - i)

        con.close()

    def export_sampling_matrix(self, path_prefix=""):
        # writes the sampling matrix calculated by the update_sampling_matrix method to a file

        if self.sampling_matrix is None:
            self.update_sampling_matrix()

        # open a file to write
        temp_path = self.matrix_name + self.sampling_matrix_suffix + ".csv"
        if os.path.exists(path_prefix):
            temp_path = path_prefix.strip("/") + "/" + temp_path
        else:
            print "The path %s does not exist" % path_prefix
            raise IOError

        self.matrix_file_path = os.path.realpath(temp_path)
        outfile = open(self.matrix_file_path, mode="wb")

        # write info from sampling matrix
        print "Writing matrix to : " + outfile.name
        column_names = ["taxon"] + sorted(zip(*self.phlawdruns)[0])
        csvwriter = csv.DictWriter(outfile,column_names)
        csvwriter.writeheader()
        i = 0
        for taxon, values in self.sampling_matrix.iteritems():
            rowdata = dict(values)
            rowdata["taxon"] = taxon
            csvwriter.writerow(rowdata)

        outfile.close()

    def update_partition_info(self):
        con = sqlite3.connect(self.dbname)
        cur = con.cursor()
        self.partitions = list()
#        self.n_sites = 0
        for phlawdrun_uid, phlawdrun_id in [(p_uid, p_dbid) for p_uid, p_dbid, p_name in self.phlawdruns]:
            cur.execute("SELECT seq_aligned FROM sequences WHERE sequences.id IN (" \
                "SELECT MIN(id) FROM sequences WHERE sequences.phlawdrun_id = ?);", (phlawdrun_id,))
            part_length = len(cur.fetchone()[0])
            
            self.partitions.append((phlawdrun_uid, part_length))
#            self.n_sites += part_length

        con.close()
        
        self.n_sites = sum(zip(*self.partitions)[1])
        self.partitions.sort()

    def export_alignment(self, path_prefix=""):
        # writes an alignment containing all sequences referenced by this matrix to a file.
        # format is "relaxed phylip" (i.e. RAxML), also writes a RAxML partition file.
        # does not yet contain support for excluding flagged sequences

        # open a file to write alignment
        temp_path = self.matrix_name + self.alignment_suffix + ".phy"
        #### note, this will overwrite any file, and defaults to
        #### the current directory, as do other matrix output methods. should fix this 
        if os.path.exists(path_prefix):
            temp_path = path_prefix.strip("/") + "/" + temp_path

        print temp_path

        self.alignment_file_path = os.path.realpath(temp_path)
        outfile = open(self.alignment_file_path, mode="wb")

        outfile.write("%s %s\n" % (self.n_taxa, self.n_sites))

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()

        # concatenate aligned sequences and write them to the alignment file
        i = 0
        report_interval = 1000
        for taxon, ncbi_id in [(uid, nid) for uid, nid, name in self.taxa]:
            line = taxon + " "

            # gather all sequences for this taxon
            cur.execute("SELECT phlawdrun.filename, phlawdrun.id, sequences.seq_aligned FROM " \
                            "sequences, phlawdrun WHERE sequences.ncbi_id = ? " \
                            "AND sequences.phlawdrun_id = phlawdrun.id;", (ncbi_id,))
            sequences = dict([(pfname + "_" + str(pid), seq) for pfname, pid, seq in cur.fetchall()])

            # for each partition, see if we have a matching sequence, if not then make an empty one
            for part_name, part_length in self.partitions:
                 if part_name in sequences.keys():
                     s = sequences[part_name]
                 else:
                     s = self.empty_char * part_length
                 line += s

            outfile.write(line+"\n")

            i += 1
            if i % report_interval == 0:
                print "Processed %s taxa, %s more to go." % (i, self.n_taxa - i)

        outfile.close()

        # open a file to write partitions
        temp_path = self.matrix_name + self.partition_suffix + ".part"
        if os.path.exists(path_prefix):
            temp_path = path_prefix.strip("/") + "/" + temp_path

        self.partition_file_path = os.path.realpath(temp_path)
        partfile = open(self.partition_file_path, mode="wb")

        last_position = 0
        for part_name, part_length in self.partitions:
            start = last_position + 1
            end = last_position + part_length
            line = "DNA, %s = %s-%s\n" % (part_name, start, end)
            partfile.write(line)
            last_position = end

        partfile.close()
        con.close()
