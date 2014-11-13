import time
import graphlab as gl


def load_data(dpath, maxn=None):
    cites = gl.SFrame.read_csv(dpath+'cites.csv', column_type_hints = [int, int])
    paper = gl.SFrame.read_csv(dpath+'papers.csv', column_type_hints = [int, int])
    bad = cites.apply(lambda x:x['p1'] == x['p2'])
    print '%d self-citation detected, delete' % bad.sum()
    cites = cites[bad == 0]
    if maxn is not None:
        paper = paper[paper['id'] < maxn]
        cites = cites[cites.apply(lambda x: x['p1'] < maxn and x['p2'] < maxn)]
    sg = gl.SGraph(vertices = paper, edges = cites, vid_field = 'id', src_field = 'p1', dst_field = 'p2')
    return sg
