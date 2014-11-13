import graphlab as gl
import time
gl.set_runtime_config('GRAPHLAB_CACHE_FILE_LOCATIONS', '/mnt/data/tmp')

dpath = '/mnt/data/'

def load_data(dpath, maxn=None):
    cites = gl.SFrame.read_csv(dpath+'cites.csv', column_type_hints = [int, int])
    paper = gl.SFrame.read_csv(dpath+'papers.csv', column_type_hints = [int, int])
    if maxn is not None:
        paper = paper[paper['id'] < maxn]
        cites = cites[cites.apply(lambda x: x['p1'] < maxn and x['p2'] < maxn)]
    sg = gl.SGraph(vertices = paper, edges = cites, vid_field = 'id', src_field = 'p1', dst_field = 'p2')
    return sg

def findp_update_fn(src, edge, dst):
    pdst = dst['parent']
    psrc = src['parent']
    for pid, d in pdst.items():
        if pid not in psrc:
            psrc[pid] = d + 1
            src['changed'] = True
        else:
            if psrc[pid] > d + 1:
                psrc[pid] = d + 1  
                src['changed'] = True
    src['parent'] = psrc    
    return (src, edge, dst)

def find_parents(g):
    start = time.time()
    num_changed = len(g.vertices)
    it = 0
    g.vertices['parent'] = g.vertices['__id'].apply(lambda x: {x: 0})    
    while (num_changed > 0):
        g.vertices['changed'] = 0
        g = g.triple_apply(findp_update_fn, ['parent', 'changed'])
        num_changed = g.vertices['changed'].sum()
        print 'Iteration %d: num_vertices changed = %d, %f secs elapsed' % (it, num_changed, time.time() - start)
        it = it + 1
    print 'Triple apply findp finished in: %f secs' % (time.time() - start)
    return g
    
sg = load_data(dpath)
sg = find_parents(sg)
