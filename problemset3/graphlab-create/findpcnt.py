import graphlab as gl
import loadgraph as load
import time

gl.set_runtime_config('GRAPHLAB_CACHE_FILE_LOCATIONS', '/mnt/data/tmp')

dpath = '/mnt/data/'


def node_update_fn(src, edge, dst):
    src['out_edges'] += 1 
    dst['in_edges'] += 1
    return (src, edge, dst)
    
def find_stats(g):
    start = time.time()
    g.vertices['in_edges'] = 0
    g.vertices['out_edges'] = 0
    g = g.triple_apply(node_update_fn, ['in_edges', 'out_edges'])
    print 'Triple apply all finished in: %f secs' % (time.time() - start)
    return g


def cnt_update_fn(src, edge, dst):
    if dst['out_edges'] == dst['counter']:
        src['counter'] += 1
        src['parent-cnt'] += dst['parent-cnt'] + 1
    return (src, edge, dst)

def find_cnt(g):
    start = time.time()
    num_left = len(g.vertices)
    it = 0
    g.vertices['counter'] = 0
    g.vertices['parent-cnt'] = 0
    while (num_left > 0):
        g = g.triple_apply(cnt_update_fn, ['counter', 'parent-cnt'])
        num_left = g.vertices.apply(lambda x: x['counter'] != x['out_edges']).sum()
        print 'Iteration %d: num_vertices left = %d, %f secs elapsed' % (it, num_left, time.time() - start)
        it = it + 1
    print 'Triple apply findp finished in: %f secs' % (time.time() - start)
    return g
    
sg = load.load_data(dpath)
sg = find_stats(sg)
sg = find_cnt(sg)
