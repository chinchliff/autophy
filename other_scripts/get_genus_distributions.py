import sys,os
from Bio import SeqIO

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "python get_genus_distributions.py outfile [working dir]"
        sys.exit(0)

    try:
        os.chdir(sys.argv[2])
    except IndexError:
        pass
    
    genes = {}
    allo = []

    for i in os.listdir("."):
        if i[-12:] == "FINAL.aln.rn":
            print i
            infile = open(i,"r")
            genes[i] = {}
            for j in SeqIO.parse(infile,"fasta"):
                nm = str(j.id).split("_")[0]
                if nm not in allo:
                    allo.append(nm)
                if nm not in genes[i]:
                    genes[i][nm] = 0
                genes[i][nm] += 1
            infile.close()

    outfile = open(sys.argv[1],"w")
    st = ""
    for i in allo:
        st += "\t"+i
    outfile.write(st+"\n")
    for i in genes:
        outfile.write(i)
        for j in allo:
            if j in genes[i]:
                outfile.write("\t"+str(genes[i][j]))
            else:
                outfile.write("\t0")
        outfile.write("\n")
    outfile.close()
            
