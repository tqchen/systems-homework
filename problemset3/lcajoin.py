from pyspark import SparkContext
import numpy as np
import scipy.sparse as sp
import sys
import csv
import time
import pickle
import os.path

class Graph:
    def __init__(self, dtuple=None, dpath = None):
        if dtuple != None:
            self.year, self.indptr, self.indices, self.nodesid, self.in_edges = dtuple
            return        
        assert dpath != None
        pdata = np.loadtxt(dpath+'/papers.csv', delimiter=',', skiprows=1, dtype=int)    
        year = np.zeros(np.max(pdata[:,0]) + 1, dtype=int)
        year[:] = -1
        year[pdata[:,0]] = pdata[:,1]
        cdata = np.loadtxt(dpath+'/cites.csv', delimiter=',', skiprows=1, dtype=int) 
        cfilter = lambda x: x[0] < year.size and x[1] < year.size and year[x[0]]>=0\
            and year[x[1]]>=0 and year[x[0]] >= year[x[1]] and x[0] != x[1]
        cdata = cdata[np.apply_along_axis(cfilter, 1, cdata)]
        csr = sp.csr_matrix((np.zeros(cdata[:,0].size), (cdata[:,0], cdata[:,1]))) 
        in_edges = np.zeros(len(year), dtype='int32')
        for i in xrange(cdata.shape[0]):
            in_edges[cdata[i,1]] += 1        
        self.year = year.astype('int32')
        self.indptr = csr.indptr.astype('int32')
        self.indices = csr.indices.astype('int32')
        self.nodesid = pdata[:,0].astype('int32')
        self.in_edges = in_edges
    def get_tuple(self):
        return (self.year, self.indptr, self.indices, self.nodesid, self.in_edges)
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
        
def shortest_path(gtuple, k):
    g = Graph(dtuple = gtuple)
    dist = g.shortest_path(k)
    for t, d in dist.iteritems():
        # for node with in_edge == 1, cannot be LCA
        if g.in_edges[t] > 1 or t == k:
            yield (t, (k, d))

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

def map_join_pair(x, year):
    rootid = x[0]
    a, b = x[1]
    if a[0] < b[0]:
        maxd = max(a[1], b[1])        
        yield ((a[0],b[0]), (rootid, maxd, year))
    
def cmp_key(x):
    rid, d, y = x
    return (d, -y, rid)
    
if len(sys.argv) < 3:
    print 'Usage:<path> <sn> [out-textfile-path]'
    exit(-1)

if len(sys.argv) > 3 and sys.argv[3] == 'local':
    spark = SparkContext('local', appName = 'SparkLCA')
else:
    spark = SparkContext(appName = 'SparkLCA')

tstart = time.time()
N = int(sys.argv[2])
g = load_graph(sys.argv[1])

if len(sys.argv) > 3 and sys.argv[3] != 'local':
    out_hdfs = sys.argv[3]
else:
    out_hdfs = None

print 'finish loading graph data %f secs elapsed' % (time.time()-tstart)
seeds = spark.parallelize([p for p in g.nodes() if p <= N])
gtuple = spark.broadcast(g.get_tuple())
print 'finish broadcasting, %f secs elapsed' % (time.time()-tstart)

cite_depth = seeds.flatMap(lambda k: shortest_path(gtuple.value, k))
pairs_rdd = cite_depth.join(cite_depth).flatMap(lambda x: map_join_pair(x, get_year(gtuple.value, x[0])))
                
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
