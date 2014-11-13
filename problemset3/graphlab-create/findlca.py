import graphlab as gl
import loadgraph as load
import time

gl.set_runtime_config('GRAPHLAB_CACHE_FILE_LOCATIONS', '/mnt/data/tmp')

dpath = '/mnt/data/'

def childs_update_fn(src, edge, dst):
    pdst = dst['childs']
    psrc = src['childs']
    for pid, d in psrc.iteritems():
        if pid not in pdst:
            pdst[pid] = d + 1
            dst['changed'] = True
        else:
            if pdst[pid] > d + 1:
                pdst[pid] = d + 1  
                dst['changed'] = True            
    dst['childs'] = pdst
    return (src, edge, dst)

def find_childs(g, maxn):
    start = time.time()
    num_changed = len(g.vertices)
    it = 0
    g.vertices['childs'] = g.vertices['__id'].apply(lambda x: {x:0} if x < maxn else {})
    while (num_changed > 0):
        g.vertices['changed'] = 0
        g = g.triple_apply(childs_update_fn, ['childs', 'changed'])
        num_changed = g.vertices['changed'].sum()
        print 'Iteration %d: num_vertices changed = %d, %d records, %f secs elapsed' % (it, num_changed, g.vertices['childs'].apply(lambda x:len(x)).sum(), time.time() - start)
        it = it + 1
    print 'Triple apply all finished in: %f secs' % (time.time() - start)
    return g
    
sg = load.load_data(dpath)
gc1k = find_childs(sg, 1000)

