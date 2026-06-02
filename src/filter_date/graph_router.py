import os
import csv
import fcntl

# Estructura DSU simple en memoria
class DSU:
    def __init__(self):
        self.parent = {}
    def find(self, i):
        if i not in self.parent: self.parent[i] = i
        if self.parent[i] == i: return i
        self.parent[i] = self.find(self.parent[i])
        return self.parent[i]
    def union(self, i, j, target="A"):
        root_i, root_j = self.find(i), self.find(j)
        
        if root_i == target:
            self.parent[root_j] = root_i
            return root_i
        elif root_j == target:
            self.parent[root_i] = root_j
            return root_j
        else:
            self.parent[root_i] = root_j
            return root_j

class GraphRouterCSV:
    def __init__(self, num_nodes):
        self.num_nodes = num_nodes
        self.dsu = DSU()
        self.log_file = "/output/uniones.csv"
        self._load_from_log() 

    def _load_from_log(self):
        if os.path.exists(self.log_file):
            with open(self.log_file, "r") as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                try:
                    reader = csv.reader(f)
                    for row in reader:
                        if row: self.dsu.union(row[0], row[1])
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)

    def get_node(self, client_id,to_bank, to_account, from_bank, from_account):
        rep_to = f"{to_bank}:{to_account}" #A
        rep_fr = f"{from_bank}:{from_account}" #B
        
        root = self.dsu.union(rep_fr, rep_to)
        
        with open(self.log_file, "a", newline="") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                writer = csv.writer(f)
                writer.writerow([rep_fr, rep_to])
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
            
        return f"Q4Graph{hash(root) % self.num_nodes}"