import sys,sqlite3,os
from Bio import SeqIO

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "python get_family_order_distributions.py db outfile [working dir]"
        sys.exit(0)
    conn = sqlite3.connect(sys.argv[1])
    c = conn.cursor()

    try:
        os.chdir(sys.argv[3])
    except IndexError:
        pass
    
    # init variables to store processed data
    genes = {}
    allo = []

    for i in os.listdir("."):

        # for every final phlawd output file, open it, create an empty dict to store info
        if i[-9:] == "FINAL.aln":
            print i
            infile = open(i,"r")
            genes[i] = {}

            # for every sequence in this file
            for j in SeqIO.parse(infile,"fasta"):

                # get left and right values for the otu, if we can't find it in the db, use zeroes
                sql = "SELECT left_value,right_value from taxonomy where ncbi_id = "+str(j.id)+";"
                c.execute(sql)
                left = 0
                right = 0
                for h in c:
                    left = h[0]
                    right = h[1]

                # get the family for this otu
                sql = "SELECT name from taxonomy where left_value < "+str(left)+" and right_value > "+ \
                    str(right)+" and node_rank = 'family' and name_class = 'scientific name';"
                c.execute(sql)
                
                # if we can't find a family (wtf?) substitute this otu id. apparently for some
                # ncbi taxa, no family has been assigned (e.g. Foetidia, ncbi_id = 79568)
                nm = ""
                for h in c:
                    nm = str(h[0])
#                print nm
                if len(nm) == 0:
                    nm = j.id

                # if we haven't seen this family/unassigned otu id yet,
                # record it, set the count to zero
                if nm not in allo:
                    allo.append(nm)
                if nm not in genes[i]:
                    genes[i][nm] = 0

                genes[i][nm] += 1

            # done with this gene
            infile.close()

    # done counting records 
    conn.close()

    outfile = open(sys.argv[2],"w")

    # build/write the header line (names of families/unassigned otus)
    st = ""
    for i in allo:
        st += "\t"+i
    outfile.write(st+"\n")

    # write gene name, then family/otu counts for each gene
    for i in genes:
        outfile.write(i)
        for j in allo:
            if j in genes[i]:
                outfile.write("\t"+str(genes[i][j]))
            else:
                outfile.write("\t0")
        outfile.write("\n")

    # done
    outfile.close()
            
