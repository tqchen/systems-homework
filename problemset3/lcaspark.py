from pyspark import SparkContext
import networkx as nx
import networkx
import csv
import sys

def load_csv(filename):
    """Helper function to load CSV data from a GZipped file"""
    csvfile = csv.reader(open(filename))
    header_line = csvfile.next()
    print('Reading from CSV file %s with headers %s' % (filename, header_line));
    #print 'Reading from CSV file {fn} with headers {h}'.format(fn=filename, h=header_line)
    return csvfile

def load_graph(dpath):
    g = nx.DiGraph()    
    for pid, year in load_csv(dpath+'/papers.csv'):
        g.add_node(int(pid), year=int(year))    

    for p1, p2 in load_csv(dpath+'/cites.csv'):
        try:
            src = int(p1)
            dst = int(p2)
            if g.node[src]['year'] >= g.node[dst]['year'] and src != dst:
                g.add_edge(src, dst)
        except KeyError:
            pass
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
    
if len(sys.argv) < 2:
    print 'Usage:<path>'
    exit(-1)

spark = SparkContext("local", "SparkLCA")
g = load_graph(sys.argv[1])
N = 50

print 'finish loading graph data'
seeds = spark.parallelize([p for p in g.nodes() if p <= N])
distg = spark.broadcast(g)
print 'start working'
cite_depth = seeds.flatMap(lambda k: map_dist(nx.single_source_shortest_path_length(distg.value, k), k))
dist_root = cite_depth.groupByKey()
pairs_rdd = dist_root.flatMap(lambda x: map_pairs(x[1], distg.value.node[x[0]]['year'], x[0]))
lca_rdd = pairs_rdd.reduceByKey(lambda x, y: x if cmp_key(x) < cmp_key(y) else y)

lca = lca_rdd.map(lambda x: x[0] + x[1]).collect()
with open('results.csv', 'wb') as resultsfile:
    writer = csv.writer(resultsfile)
    writer.writerow(['p1', 'p2', 'a', 'depth', 'year'])
    writer.writerows(sorted(lca))
    for p1, p2, a, 
print ("Wrote %d results to results.csv" % (len(lca)))
#print "Wrote {r} results to results.csv".format(r=len(lca))
