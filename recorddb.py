import sys, sqlite3, string, re, csv, os, time
from copy import deepcopy
import sqlite3_extensions

class Database():
    def __init__(self,dbname):
        self.dbname = dbname
        self.update_table_list()
        self.update_matrix_list()
        self.update_phlawdrun_list()

    def connection(self):
        con = sqlite3.connect(self.dbname)
        return con

    def create_sampling_matrix(self, name, description, excluded_taxa=None, excluded_phlawdruns=None, \
                                   excluded_sequences=None, included_taxa = None, included_phlawdruns = None, \
                                   included_sequences = None):
        # exclusion is evaluated after inclusion, with the consequence that any sequence in any excluded set
        # will not be saved in the matrix regardless of any included sets it may be in.

        con = sqlite3.connect(self.dbname)
        cur = con.cursor(sqlite3_extensions.safecursor)

        seqs_to_include = []
        if hasattr(included_sequences, "__iter__"):
            seqs_to_include = included_sequences
        
        if hasattr(included_phlawdruns, "__iter__"):            
            for phlawdrun_id in included_phlawdruns:
                cur.execute("SELECT id FROM sequences WHERE phlawdrun_id = ?;",(phlawdrun_id,))
                seqs_to_include += [r[0] for r in cur.fetchall()]
        
        if hasattr(included_taxa,"__iter__"):
            for taxon_id in included_taxa:
                cur.execute("SELECT id FROM sequences WHERE taxon_id = ?;",(taxon_id,))
                seqs_to_include += [r[0] for r in cur.fetchall()]
        
        if not hasattr(included_sequences,"__iter__") and not hasattr(included_phlawdruns,"__iter__") and not \
                hasattr(included_taxa, "__iter__"):
            # no inclusion sets provided; add all sequences
            cur.execute("SELECT id FROM sequences;")
            seqs_to_include = [item[0] for item in cur.fetchall()]        

        seqs_to_exclude = []
        if hasattr(excluded_sequences, "__iter__"):
            seqs_to_exclude = excluded_sequences
        
        if hasattr(excluded_phlawdruns, "__iter__"):
            for phlawdru_id in excluded_phlawdruns:
                cur.execute("SELECT id FROM sequences WhERE phlawdrun_id = ?;",(phlawdrun_id,))
                seqs_to_exclude += [r[0] for r in cur.fetchall()]

        if hasattr(excluded_taxa,"__iter__"):
            for taxon_id in excluded_taxa:
                cur.execute("SELECT id FROM sequences WHERE taxon_id = ?;",(taxon_id,))
                seqs_to_exclude += [r[0] for r in cur.fetchall()]

        seqs_to_include = list(set(seqs_to_include) - set(seqs_to_exclude))

        if len(seqs_to_include) > 0:
            # create a new matrix record, and recover its id
            cur.execute("INSERT INTO matrix(name, description) VALUES (?,?);",(name, description))
            cur.execute("SELECT last_insert_rowid();")
            matrix_id = cur.fetchone()[0]

            for seq_id in seqs_to_include:
                cur.pexecute("INSERT INTO sequence_matrix_map (sequence_id, matrix_id) VALUES (?,?);",(seq_id, matrix_id))

        con.commit()

        return matrix_id

##############################################################
#
#   question: will we need methods to delete things from the
#   recorddb or will we just rebuild it each time to update
#   it? depending on how we implement flags, deleting things
#   could get tricky, though rebuilding does not seem less so
#
##############################################################

    def install_taxonomy(self):
        print "Not implemented due to speed. Use the phlawd executable to build a sequence database " \
            "and then use recorddb.database.wipe() to clear it of all non-taxonomic data."

    def import_phlawdruns_from_dir(self, dir_to_import, config_file_suffix = ".phlawd"):
        # point this to a directory containing successfully assembled phlawd config files 
	filelist = os.listdir(dir_to_import)

	for fname in filelist:
            matchstr = r"^.+" + config_file_suffix + r"$"
            if re.match(matchstr, fname):
		path_to_config_file = dir_to_import.strip("/") + "/" + fname
		print "\ncurrently attempting to import " + path_to_config_file
                source = PhlawdRun_Source(path_to_config_file)
                self.import_phlawdrun_from_source_object(source)

        self.update_phlawdruns_list()

    def import_phlawdrun_from_source_object(self, phlawdrun_source_to_import):
        
        runsource = phlawdrun_source_to_import

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()

        # extract field names from phlawdrun table
        field_info = cur.execute("pragma table_info(phlawdrun)")
        fields = [re.split(r"\,\s+u\'",str(f))[1].strip("'") for f in field_info]

        # store config file params that match db field names
        params_valid = dict()
        matched = list()
        for name, value in runsource.parameters.iteritems():
            if name in fields:
                if name not in matched:
                    if name == "clade":
                        # search clades are provided in phlawd config files as names, but in the recorddb
                        # we store references to the corresponding records in the ncbi taxononomy table.
                        # here we look up the taxon id for whatever clade is named in the config file
                        cur.execute("SELECT taxonomy.ncbi_id from taxonomy where taxonomy.name = ?;",(value,)) 
                        params_valid["clade_id"] = cur.fetchone()[0]
                    params_valid[name] = value
                    matched.append(name)
                else:
                    print("Found two declarations for parameter: " + name + ". Recording first one only.")
        
        # organize field names/values to be stored in database	
        fieldnames = ["fullpath", "filename", "configtext", "keeptext", "excludetext", "db_checksum", \
                          "phlawddb"] + params_valid.keys()
        fieldvalues = [runsource.configfile_path, runsource.configfile_name, runsource.configfile_text, \
                           runsource.keepfile_text, runsource.excludefile_text, \
                           runsource.db_size,runsource.db_path] + params_valid.values()

        # check phlawd parameters and source db file size to avoid importing duplicates
        cur.execute("SELECT db_checksum, id FROM phlawdrun WHERE " \
                         "clade = ? AND search = ? AND gene = ? AND mad = ? AND coverage = ? AND identity = ?;", \
                         (params_valid["clade"],params_valid["search"],params_valid["gene"],params_valid["mad"], \
                         params_valid["coverage"],params_valid["identity"]))
        results = dict(cur.fetchall())

        # check if this phlawdrun has already been imported
        duplicate_accession_ids = []
        if runsource.db_size in results.keys():

            # it's a duplicate, use preexisting dbid
            phlawdrun_id = results[runsource.db_size]

            print "This phlawd run has already been stored. Checking for associated stored sequences."
            cur.execute("SELECT accession_id FROM sequences WHERE phlawdrun_id = ?;",(phlawdrun_id,))
            duplicate_accession_ids = [item[0] for item in cur.fetchall()]

            print "Found %s sequence records already imported from this phlawd run, " \
                              "these will not be imported again." % len(duplicate_accession_ids)
        else:
            # store this phlawdrun
            insert_query = "INSERT INTO phlawdrun (" + ",".join(fieldnames) + ") " \
                           "VALUES (" + ",".join(["?"] * len(fieldvalues)) + ");" 
            cur.execute(insert_query,tuple(fieldvalues))
            con.commit()

            # get id for this phlawdrun
            cur.execute("SELECT last_insert_rowid();")
            phlawdrun_id = cur.fetchone()[0]
        
        con.close()

        this_phlawdrun = self.retrieve_phlawdrun_by_id(phlawdrun_id)
        this_phlawdrun.import_sequences_from_source_db(seqs_to_exclude=duplicate_accession_ids)

        return this_phlawdrun

    def install_empty_recorddb_tables(self):
	con = sqlite3.connect(self.dbname)
        # we use safecursor's pexecute method to attempt CREATE calls; it prints alerts but 
        # doesn't terminate the program if the table/index we're trying to create already exists.
	cur = con.cursor(sqlite3_extensions.safecursor)

	cur.pexecute("CREATE TABLE sequences (" \
	      "id INTEGER PRIMARY KEY, " \
	      "ncbi_id INTEGER, " \
	      "phlawdrun_id INTEGER, " \
	      "accession_id VARCHAR(128), " \
	      "gene, " \
	      "seq LONGTEXT, " \
	      "seq_aligned LONGTEXT);")

	cur.pexecute("CREATE INDEX sequences_ncbi_id on sequences(ncbi_id);")
	cur.pexecute("CREATE INDEX sequences_accession_id on sequences(accession_id);")
	cur.pexecute("CREATE INDEX sequences_phlawdrun_id on sequences(phlawdrun_id);")

	# most field names match phlawd parameters (e.g. clade, search, db, coverage, identity, etc.)
	cur.pexecute("CREATE TABLE phlawdrun (" \
		"id INTEGER PRIMARY KEY, " \
		"db_checksum INT, " \
		"fullpath TEXT, " \
		"filename VARCHAR(256), "\
		"clade VARCHAR(256), " \
		"clade_id INT, " \
		"search TEXT, "
		"gene VARCHAR(256), " \
		"mad REAL, " \
		"coverage REAL, " \
		"identity REAL, " \
                "phlawddb TEXT, " \
		"db TEXT, " \
		"configtext TEXT, " \
		"keeptext LONGTEXT, " \
		"excludetext TEXT);")

	cur.pexecute("CREATE INDEX phlawdrun_clade_id on phlawdrun(clade);")
	cur.pexecute("CREATE INDEX phlawnrun_gene on phlawdrun(gene);")
	con.commit()

	cur.pexecute("CREATE TABLE matrix (" \
		"id INTEGER PRIMARY KEY, " \
		"name TEXT UNIQUE, " \
		"description LONGTEXT, " \
		"decisiveness REAL, " \
                "date INTEGER, " \
                "parent_id INTEGER);")
	con.commit()

        cur.pexecute("CREATE TABLE matrix_type (" \
                "id INTEGER PRIMARY KEY, " \
                "name TEXT, " \
                "description);")
        con.commit()

        matrix_types = list( \
            ("default", "the most inclusive matrix, consisting of all sequences from all phlawdruns"),
            ("intermediate", "a matrix used for sampling optimization, often corresponding to a single " \
                 "clade or locus. used to generate sampling matrices (for decisivator) and bootstrap " \
                 "sets (for roguenarok). not used for final tree-searching"), \
            ("final", "the final matrix, generated through iterative subsampling optimization of the " \
                 "default (all-inclusive) matrix, to be used for end-product tree-searching"))

        # for matrix_type in matrix_types:
             # insert matrix_type into db

	cur.pexecute("CREATE TABLE sequence_matrix_include_map(" \
		"id INTEGER PRIMARY KEY, " \
		"matrix_id INTEGER, " \
		"sequence_id INTEGER);")
	con.commit()

	cur.pexecute("CREATE TABLE sequence_matrix_exclude_map(" \
		"id INTEGER PRIMARY KEY, " \
		"matrix_id INTEGER, " \
		"sequence_id INTEGER, " \
                "exclude_criterion_id);")
	con.commit()

        cur.pexecute("CREATE TABLE exclude_criterion(" \
                "id INTEGER PRIMARY KEY, " \
                "name TEXT, " \
                "description TEXT);")
        con.commit()
        
        exclude_criteria = list( \
            ("rogue", "identified as a rogue taxon by roguenarok"),
            ("indecisive", "excluded at the recommendation of decisivator"),
            ("orphan", "identified by sister-clade comparison as part of a clade depauperate for this locus"))

        # for criterion in exclude criteria
            # insert criterion into db 

#	cur.pexecute("CREATE TABLE flag (" \
#		"id INTEGER PRIMARY KEY, " \
#		"sequence_matrix_map_id, " \
#		"flagger_id, " \
#		"reason_id, " \
#		"timestamp);")
#	con.commit()
#
#	cur.pexecute("CREATE TABLE flag_reason (" \
#		"id INTEGER PRIMARY KEY, " \
#		"name, " \
#		"description);")
#	con.commit()

	con.close()

    def retrieve_matrix_by_name(self, matrix_name):
        the_matrix = Matrix(self.dbname, matrix_name=matrix_name)
        return the_matrix

    def retrieve_matrix_by_id(self, matrix_id):
        the_matrix = Matrix(self.dbname, matrix_id=matrix_id)
        return the_matrix

    def retrieve_phlawdrun_by_id(self, phlawdrun_id):
        the_phlawdrun = PhlawdRun(self.dbname, phlawdrun_id=phlawdrun_id)
        return the_phlawdrun

    def update_matrix_list(self):
        con = sqlite3.connect(self.dbname)
        cur = con.cursor()

        cur.execute("SELECT name, id FROM matrix;")
        self.matrices = [(row[0], row[1]) for row in cur.fetchall()]

    def update_phlawdrun_list(self):
        con = sqlite3.connect(self.dbname)
        cur = con.cursor()

        # store a list of all phlawdruns
        cur.execute("SELECT filename, id FROM phlawdrun;")
        self.phlawdruns = [(row[0], row[1]) for row in cur.fetchall()]
        con.close()

    def update_table_list(self):
        con = sqlite3.connect(self.dbname)
        cur = con.cursor()

        # store a list of all tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        self.tables = [row[0] for row in cur.fetchall()]
        con.close()

    def wipe(self):
        ###### need to test that this method works to clear a newly created phlawd database
        con = sqlite3.connect(self.dbname)
        cur = con.cursor(sqlite3_extensions.safecursor)

        # in case of foreign key constraints, we may need to make more than one pass
        # to clear all tables. how best to do this is not clear, but for now we have
        # no fk constraints so we make only one pass

        # attempt to drop each table except taxonomy
        for t_name in self.tables:
            if t_name != "taxonomy":
                query = "DROP TABLE %s" % t_name 
                cur.pexecute(query)      # pexecute method ignores non-problematic errors
                con.commit()

        con.close()

        self.install_empty_recorddb_tables()
        self.update_table_list()

class PhlawdRun():
    def __init__(self, dbname, phlawdrun_id):
        self.dbname = dbname
        self.database_id = phlawdrun_id

        con = sqlite3.connect(self.dbname)
        cur = con.cursor()

        # get relevant info about this phlawdrun to store with sequences 
        cur.execute("SELECT filename, fullpath, clade, clade_id, search, gene, mad, coverage, " \
                        "identity, db, phlawddb, configtext, keeptext, excludetext FROM phlawdrun " \
                        "WHERE phlawdrun.id = ?;", (phlawdrun_id, ))
        r = cur.fetchone()
        self.configfile_name = r[0]
        self.configfile_path = r[1]
        self.search_clade_name = r[2]
        self.search_clade_id = r[3]
        self.search_terms = r[4]
        self.gene_name = r[5]
        self.mad = r[6]
        self.coverage = r[7]
        self.identity = r[8]
        self.source_dbname = r[9]
        self.phlawd_dbname = r[10]
        self.configfile_text = r[11]
        self.keepfile_text = r[12]
        self.excludefile_text = r[13]

        con.close()

    def import_sequences_from_source_db(self, seqs_to_exclude=[]):
        # open db connections
        phlawddb_con = sqlite3.connect(self.phlawd_dbname)
        recorddb_con = sqlite3.connect(self.dbname)
        pcur = phlawddb_con.cursor()
        cur = recorddb_con.cursor()

        # get aligned sequences from source db
        pcur.execute("SELECT sequence_id,sequence FROM sequence_profile_map WHERE " \
                             "sequence_profile_map.profile_id = (SELECT MAX(id) FROM profile_alignments);")
        aligned_seq_records = pcur.fetchall()	

        n_source_records = len(aligned_seq_records)
        print "Found " + str(n_source_records) + " aligned sequences in source db."

        n_stored = 0
        if n_source_records == len(seqs_to_exclude):
            # we have already imported all these records
            dups_encountered = n_source_records
        else:
            # attempt to import sequence records into record db
            dups_encountered = 0

            # do batch processing to save memory
            pinterval = 1000
            for j in range((n_source_records / pinterval) + 1):
                start = j * pinterval
                end = ((j + 1) * pinterval)
                if end > n_source_records:
                    end = n_source_records

                print "processing records " + str(start + 1) + " to " + str(end)

                # gather info for records from source db
                seqs_to_store = list()
                for s in aligned_seq_records[start:end]:
                    seq_id = s[0]

                    # get additional sequence metadata from the sequences table in source db
                    pcur.execute("SELECT ncbi_id, sequence, accession FROM sequences WHERE sequences.id = ?",(seq_id,))
                    r = pcur.fetchone()

                    newrecord = dict()
                    newrecord["ncbi_id"] = r[0]
                    newrecord["gene"] = self.gene_name
                    newrecord["seq"] = r[1]
                    newrecord["accession_id"] = r[2]
                    newrecord["seq_aligned"] = s[1]
                    newrecord["phlawdrun_id"] = self.database_id

                    seqs_to_store.append(newrecord)

                # store records in record db (if they aren't already there)
                for record in seqs_to_store:
                    if record["accession_id"] not in seqs_to_exclude:
                        insert_query = "INSERT INTO sequences (" + ",".join(record.keys()) + ") " \
                                       "VALUES (" + ",".join(["?"] * len(record)) + ");"
                        cur.execute(insert_query,tuple(record.values()))
                        n_stored += 1
                    else:
                        dups_encountered += 1

                sqlite3_extensions.patient_commit(recorddb_con)

                j += 1

        # all done
        print "Added " + str(n_stored) + " new sequence records to the db."

        recorddb_con.close()
        phlawddb_con.close()

class PhlawdRun_Source():
    def __init__(self, path_to_phlawd_config_file):
        self.configfile_path = os.path.realpath(path_to_phlawd_config_file)
        self.configfile_name = os.path.basename(self.configfile_path)
        self.reference_path = os.path.dirname(self.configfile_path).rstrip("/") + "/"
        
        self.update_parameters()

        # attempt to resolve path to db where run data is stored
        db_path_raw = self.reference_path + self.configfile_name.split(".phlawd")[0] + ".db"
        if not os.path.exists(db_path_raw):
            message = "Could not find a matching '.db' file in the same directory."
            raise IOError(message)
        self.db_path = os.path.realpath(os.path.join(self.reference_path, db_path_raw))

        # check if the source db has anything in it
	con = sqlite3.connect(self.db_path)
	cur = con.cursor()
	cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
	db_tablenames = [item[0] for item in cur.fetchall()]
	con.close()

	if "sequences" not in db_tablenames:
            message = "The database for this phlawd run doesn't seem to be valid."
            raise IOError(message)

        # db size checksum is used to make sure we don't import the same phlawdrun more than once
        self.db_size = os.path.getsize(self.db_path)

    def update_parameters(self):
        # extract parameters from phlawd config file
        self.parameters = dict()
        self.configfile_text = ""
        configfile = open(self.configfile_path,"rb")
        for l in configfile:
            line = l.strip()
            if len(line) > 0:
                if line[0] != "#":
                    elem = re.split(r"\=", line)
                    self.parameters[string.lower(elem[0].strip())] = elem[1].strip()
                self.configfile_text += line + "\n"
        configfile.close()

        # attempt to read contents of keepfile
        try:
            self.keepfile_path = os.path.realpath(os.path.join(self.reference_path, self.parameters["knownfile"]))
            keepfile = open(self.keepfile_path,"rb")
            self.keepfile_text = "".join(keepfile.readlines())
            keepfile.close()
        except KeyError:
            self.keepfile_text = ""
            message = "There does not seem to be a keepfile declaration in this config file."
            raise KeyError(message)
        except IOError:
            self.keepfile_text = ""
            message = "Could not access the keepfile at %s" % self.keepfile_path
            raise IOError(message)

        # attempt to read contents of excludefile
        try:
            self.excludefile_path = os.path.realpath(os.path.join(self.reference_path, self.parameters["excludelistfile"]))
            excludefile = open(self.excludefile_path,"rb")
            self.excludefile_text = "".join(excludefile.readlines())
            excludefile.close()
        except KeyError:
            self.excludefile_text = ""
        except IOError:
            self.excludefile_text = ""
            message = "Could not access the excludefile at %s" % self.excludefile_path
            raise IOError(message)

class Matrix():
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
        
        # if we got a name, check if it matches a matrix, get that matrix's db id 
        if self.matrix_name is not None:
            cur.execute("SELECT id FROM matrix where name = ?;",(self.matrix_name,))
            r = cur.fetchone()
            if r != None:
                self.matrix_id = r[0]

        if self.matrix_id is not None:
            cur.execute("SELECT name FROM matrix where id = ?;",(self.matrix_id,))
            r = cur.fetchone()
            if r != None:
                self.matrix_name = r[0]

        con.close()
        
        if self.matrix_id == None:
            message = "Could not find that matrix."
            raise NameError(message)

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

        self.phlawdruns = [(p_filename + self.r_char + str(p_db_id), p_db_id, p_filename) for p_db_id, \
                               p_filename in cur.fetchall()]

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

    def export_to_csv(self, path_prefix=""):
        # writes the sampling matrix calculated by the update_sampling_matrix method to a file

        if self.sampling_matrix is None:
            self.update_sampling_matrix()

        # open a file to write
        temp_path = self.matrix_name + self.sampling_matrix_suffix + ".csv"
        if os.path.exists(path_prefix):
            temp_path = path_prefix.strip("/") + "/" + temp_path

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
        for phlawdrun_uid, phlawdrun_id in [(p_uid, p_dbid) for p_uid, p_dbid, p_name in self.phlawdruns]:
            cur.execute("SELECT seq_aligned FROM sequences WHERE sequences.id IN (" \
                "SELECT MIN(id) FROM sequences WHERE sequences.phlawdrun_id = ?);", (phlawdrun_id,))
            part_length = len(cur.fetchone()[0])
            
            self.partitions.append((phlawdrun_uid, part_length))

        con.close()

        self.n_sites = sum(zip(*self.partitions)[1])
        self.partitions.sort()

    def export_alignment(self, path_prefix=""):
        # writes an alignment containing all sequences referenced by this matrix to a file.
        # format is "relaxed phylip" (i.e. RAxML), also writes a RAxML partition file.
        # does not yet contain support for excluding flagged sequences

        # open a file to write alignment
        temp_path = self.matrix_name + self.alignment_suffix + ".phy"
        #### note, this will overwrite any file, and defaults to the current directory,
        #### as do other matrix output methods. should fix this 
        if os.path.exists(path_prefix):
            temp_path = path_prefix.strip("/") + "/" + temp_path
        self.alignment_file_path = os.path.realpath(temp_path)
        outfile = open(self.alignment_file_path, mode="wb")

        # write header line for phylip format
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
        
        self.export_partition_file(path_prefix)

    def export_partition_file(self, path_prefix):
        # writes a RAxML format partition file
        
        self.update_partition_info()

        # open a file to write partitions
        temp_path = self.matrix_name + self.partition_suffix + ".part"
        if os.path.exists(path_prefix):
            temp_path = path_prefix.strip("/") + "/" + temp_path
        self.partition_file_path = os.path.realpath(temp_path)
        partfile = open(self.partition_file_path, mode="wb")

        # write each partition's info to file
        last_position = 0
        for part_name, part_length in self.partitions:
            start = last_position + 1
            end = last_position + part_length
            line = "DNA, %s = %s-%s\n" % (part_name, start, end)
            partfile.write(line)
            last_position = end

        partfile.close()
