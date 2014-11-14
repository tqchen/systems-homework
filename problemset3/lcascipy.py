from pyspark import SparkContext
import numpy as np
import scipy.sparse as sp
import csv
import sys
import time
import pickle
import os.path

class Graph:
    def __init__(self, dtuple=None, dpath = None):
        if dtuple != None:
            self.year, self.indptr, self.indices, self.nodesid = dtuple
            return        
        assert dpath != None
        pdata = np.loadtxt(dpath+'/papers.csv', delimiter=',', skiprows=1, dtype=int)    
        year = np.zeros(np.max(pdata[:,0]) + 1, dtype=int)
        year[:] = -1
        year[pdata[:,0]] = pdata[:,1]
        cdata = np.loadtxt(dpath+'/cites.csv', delimiter=',', skiprows=1, dtype=int) 
        cdata = cdata[np.apply_along_axis(lambda x: x[0] < year.size and x[1] < year.size and year[x[0]]>=0 and year[x[1]]>=0 and year[x[0]] >= year[x[1]] and x[0] != x[1], 1, cdata)]    
        csr = sp.csr_matrix((np.zeros(cdata[:,0].size), (cdata[:,0], cdata[:,1]))) 
        self.year = year.astype('int32')
        self.indptr = csr.indptr.astype('int32')
        self.indices = csr.indices.astype('int32')
        self.nodesid = pdata[:,0].astype('int32')
    def get_tuple(self):
        return (self.year, self.indptr, self.indices, self.nodesid)
    def get_link(self,i):
        return self.indices[self.indptr[i]:self.indptr[i+1]]
    def nodes(self):
        return self.nodesid
    def shortest_path(self,start):
        dist={} 
        level=0 
        qexpand = set([ start ])
        while len(qexpand) != 0:
            vnext = set()
            for v in qexpand:
                if v not in dist:
                    dist[v]=level
                    vnext.update(self.get_link(v))
            level = level + 1
            qexpand = vnext
        return dist
        
def shortest_path(gtuple, start):
    g = Graph(dtuple = gtuple)
    return g.shortest_path(start)

def get_year(gtuple, rid):
    g = Graph(dtuple = gtuple)
    return g.year[rid]

def load_graph(dpath):
    pklname = dpath+'/npygraph.pkl'
    if os.path.exists(pklname):
        g = pickle.load(open(pklname, 'rb'))
    else:
        g = Graph(dpath=sys.argv[1])    
        fo = file(pklname, 'wb')        
        pickle.dump(g, fo, 2)
        fo.close()
    return g

def map_dist(dist, k):
    return [(t, (k, d)) for t,d in dist.iteritems()]

def map_pairs(lst, year, rootid):
    lst = [x for x in lst]
    ret = []    
    lst = sorted(lst, key = lambda x:x[0])
    for i in range(len(lst)):
        for j in range(i+1,len(lst)):
            maxd = max(lst[i][1], lst[j][1])
            ret.append(((lst[i][0],lst[j][0]), (rootid, maxd, year)))
    return ret

def cmp_key(x):
    rid, d, y = x
    return (d, -y, rid)
    
if len(sys.argv) < 3:
    print 'Usage:<path> <sn> [out-textfile-path]'
    exit(-1)

tstart = time.time()
spark = SparkContext("local", "SparkLCA")
N = int(sys.argv[2])
g = load_graph(sys.argv[1])
if len(sys.argv) > 3:
    out_hdfs = sys.argv[3]
else:
    out_hdfs = None

print 'finish loading graph data %f secs elapsed' % (time.time()-tstart)
seeds = spark.parallelize([p for p in g.nodes() if p <= N])
gtuple = spark.broadcast(g.get_tuple())
print 'finish broadcasting, %f secs elapsed' % (time.time()-tstart)

cite_depth = seeds.flatMap(lambda k: map_dist(shortest_path(gtuple.value, k), k))
dist_root = cite_depth.groupByKey()
pairs_rdd = dist_root.flatMap(lambda x: map_pairs(x[1], get_year(gtuple.value, x[0]), x[0]))
lca_rdd = pairs_rdd.reduceByKey(lambda x, y: x if cmp_key(x) < cmp_key(y) else y)
lca = lca_rdd.map(lambda x: x[0] + x[1])

print 'finish calculation, %f secs elapsed' % (time.time()-tstart)

if out_hdfs is None:
    lca = lca.collect()
    with open(sys.argv[1]+'/result-%d.csv' % N, 'wb') as resultsfile:
        writer = csv.writer(resultsfile)
        writer.writerow(['p1', 'p2', 'a', 'depth', 'year'])
        writer.writerows(sorted(lca))
        print ("Wrote %d results to results.csv %f sec elapsed" % (len(lca), time.time()-tstart))
else:
    lca.saveAsTextFile(out_hdfs)    
    print 'wrote results to %s, %f secs elapsed' % (out_hdfs, time.time()-tstart)
#print "Wrote {r} results to results.csv".format(r=len(lca))
