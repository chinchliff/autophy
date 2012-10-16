import os, 

class external_phlawdrun():
    def __init__(self, path_to_phlawd_config_file):
        self.configfile_path = os.path.realpath(path_to_phlawd_config_file)
        self.configfile_name = os.path.basename(self.configfile_path)
        self.reference_path = os.path.dirname(self.configfile_path)
        
        self.update_parameters()

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
            self.keepfile_path = os.path.realpath(os.path.join(self.reference_path,self.parameters["knownfile"]))
            keepfile = open(self.keepfile_path,"rb")
            self.keepfile_text = "".join(keepfile.readlines())
            keepfile.close()
        except KeyError:
            self.keepfile_text = ""
            message = "There does not seem to be a keepfile declaration in this config file."
            raise KeyError(message)
        except IOError:
            self.keepfile_text = ""
            message = "Could not access the keepfile at %s\nDoes it still exist?" % self.keepfile_oath
            raise IOError(message)

        # attempt to read contents of excludefile
        try:
            self.excludefile_path = os.path.realpath(os.path.join(self.reference_path,self.parameters["excludelistfile"]))
            excludefile = open(self.excludefile_path,"rb")
            self.excludefile_text = "".join(excludefile.readlines())
            excludefile.close()
        except KeyError:
            self.excludefile_text = ""
        except IOError:
            self.excludefile_text = ""
            message = "Could not access the excludefile at %s\nDoes it still exist?" % self.excludefile_path
            raise IOError(message)
