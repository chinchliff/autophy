import sqlite3,sys

"""
this will rebuild (or build) the left right values for the taxonomy 

assumes that python sqlite library would be around
"""

class tree_utils:
	def __init__(self,cur,parent_ncbi_map):
		self.updcmd = ""
		self.count = 1
		self.parent_ncbi_map = parent_ncbi_map
		self.cur = cur

	def rebuild_tree(self,gid,lft):
		rgt = lft + 1
		res = []
		if gid in self.parent_ncbi_map:
			res = self.parent_ncbi_map[gid]
		for i in res:
			if i == gid:
				continue
			else:
				rgt = self.rebuild_tree(i,rgt)
		self.updcmd = "update taxonomy set left_value = "+str(lft)+", right_value = "+str(rgt)+" where ncbi_id = "+str(gid)+";" #and name_class = scientific name
		self.cur.execute(self.updcmd)
		if self.count % 100000 == 0:
			print (self.count)
		self.count += 1
		return rgt + 1
	
if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("usage: rebuild_tree_sqlite database")
		sys.exit(0)
	con = sqlite3.connect(sys.argv[1])
	curup = con.cursor()
	cmd = "select ncbi_id,parent_ncbi_id from taxonomy where name_class = 'scientific name';"
	ncbi_parent_map = {}
	parent_ncbi_map = {}
	for i in curup.execute(cmd):
		if int(i[0]) in ncbi_parent_map:
			ncbi_parent_map[int(i[0])].append(int(i[1]))
		else:
			ncbi_parent_map[int(i[0])] = [int(i[1])]
		if int(i[1]) in parent_ncbi_map:
			parent_ncbi_map[int(i[1])].append(int(i[0]))
		else:
			parent_ncbi_map[int(i[1])] = [int(i[0])]
	#get the root and send to rebuild
	curup.close()
	cur = con.cursor()
	t = tree_utils(cur,parent_ncbi_map)
	t.rebuild_tree(1,1)
	con.commit()
	con.close()
