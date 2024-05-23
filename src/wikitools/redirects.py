from wikitools.tools import chunks
from wikitools.api import *
import pickle

async def basic_info(session, titles=None, pageids=None, revids=None,
                     titles_redirect_map=None, norm_map=None, id_map=None, params={}, function=None, f_args={}, debug=False):
    
    query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                           revids=revids, generator=False,
                                           norm_map=norm_map, titles_redirect_map=titles_redirect_map,
                                           params=params)

    data = await iterate_async_query(session, query_list, function, args=f_args, debug=debug)

    return data

async def parse_redirects(data):
    """
    Parse redirect data from Wikipedia API response.

    Parameters
    ----------
    data : dict
        Data from Wikipedia API.

    Returns
    -------
    dict
        Map with redirect name keys and true article name values.

    """
    redirects = {}
    norms = {}
    ids = {}
    for page in await data:
        redirects.update({x['from']: x['to']
                          for x in page['query'].get('redirects', {})})
        redirects.update({l.get('title', None): None
                          for l in page['query'].get('pages', [])
                          if 'missing' in l})
        norms.update({x['from']: x['to']
                        for x in page['query'].get('normalized', {})})
        ids.update({x['title']: x.get('pageid', -1)
                        for x in page['query'].get('pages', [])
                        if 'missing' not in x})
        
    return redirects, norms, ids

async def fix_redirects(session, titles=None, pageids=None, revids=None,
                     redirect_map=None, norm_map=None, id_map=None):
    """
    Map redirect name to true Wikipedia article name.

    Parameters
    ----------
    articles : iterable
        Wikipedia article names.
    existingmap : dict, optional
        If a (partial) existing map exists, combine. The default is {}.

    Returns
    -------
    dict
        Map with redirect name keys and true article name values.

    """
    query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                        revids=revids, generator=False,
                                        norm_map=norm_map, redirect_map=redirect_map,
                                        params={'redirects':''})

    data = await iterate_async_query(session, query_list, parse_redirects, debug=True)
    redirects = {key: val for d in data for key, val in d[0].items()}
    norms = {key: val for d in data for key, val in d[1].items()}
    ids = {key: val for d in data for key, val in d[2].items()}

    redirect_map.update(redirects)
    norm_map.update(norms)
    id_map.update(ids)


async def parse_fetched_redirects(data):
    """
    Parse redirect data from Wikipedia API response.

    Parameters
    ----------
    data : dict
        Data from Wikipedia API.

    Returns
    -------
    dict
        Map with redirect name keys and true article name values.
    """
    f_redirects = {}
    ids = {}
    for page in await data:
        if 'missing' in page:
            continue
        new_rds = {page['title']: [x['title'] for x in page.get('redirects', {})]}
        for k, v in new_rds.items():
            if k in f_redirects:
                f_redirects[k].extend(v)
            else:
                f_redirects[k] = [k] + v
        ids.update({x['title']: x.get('pageid', -1)
                for x in page.get('redirects', [])})
        
    return f_redirects, ids

async def get_redirects(session, titles=None, pageids=None, revids=None,
                     redirect_map=None, norm_map=None, id_map=None,
                     collected_redirects=None):
    """
    Map redirect name to true Wikipedia article name.

    Parameters
    ----------
    articles : iterable
        Wikipedia article names.
    existingmap : dict, optional
        If a (partial) existing map exists, combine. The default is {}.

    Returns
    -------
    dict
        Map with redirect name keys and true article name values.

    """

    # TODO: titles - collected_redirects 
    query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                        revids=revids, generator=False,
                                        norm_map=norm_map, redirect_map=redirect_map,
                                        params={'prop':'redirects', 'rdlimit': 'max'})

    data = await iterate_async_query(session, query_list, parse_fetched_redirects, debug=False)

    f_redirects = {key: val for d in data for key, val in d[0].items()}
    reverse_redirects = {x: k for k, v in f_redirects.items() for x in v}
    ids = {key: val for d in data for key, val in d[1].items()}

    collected_redirects.update(f_redirects)
    redirect_map.update(reverse_redirects)
    id_map.update(ids)



class page_maps():
    def __init__(self, titles_redirect_map={}, pageids_redirect_map={},
                 norm_map={}, id_map={}, revid_map={}, collected_title_redirects={},
                 collected_pageid_redirects={}):
        self.titles_redirect_map = titles_redirect_map
        self.pageids_redirect_map = pageids_redirect_map
        self.norm_map = norm_map
        self.id_map = id_map
        self.revid_map = revid_map
        self.collected_title_redirects = collected_title_redirects
        self.collected_pageid_redirects = collected_pageid_redirects

    async def fix_redirects(self, session, titles=None, pageids=None, revids=None):
        if titles:
            titles = [self.norm_map.get(a, a) for a in titles]
            titles = [self.titles_redirect_map.get(a, a) for a in titles]
            titles = list(dict.fromkeys([a for a in titles if a]))
            titles = [a for a in titles if a not in self.titles_redirect_map]
            if not titles:
                return
        elif pageids:
            pageids = [self.pageids_redirect_map.get(int(a), int(a)) for a in pageids]
            pageids = list(dict.fromkeys([a for a in pageids if a is not None]))
            pageids = [a for a in pageids if a not in self.pageids_redirect_map]
            if not pageids:
                return

        query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                            revids=revids, generator=False,
                                            norm_map=self.norm_map,
                                            titles_redirect_map=self.titles_redirect_map,
                                            pageids_redirect_map=self.pageids_redirect_map,
                                            params={'redirects':''})

        data = await iterate_async_query(session, query_list, parse_redirects, debug=True)
        redirects = {key: val for d in data for key, val in d[0].items()}
        norms = {key: val for d in data for key, val in d[1].items()}
        ids = {key: val for d in data for key, val in d[2].items()}
        missing_ids = [k for k in redirects.keys() if k not in self.id_map]
        mdata = await basic_info(session, titles=missing_ids, function=parse_redirects,
                                 titles_redirect_map=self.titles_redirect_map,
                                 norm_map=self.norm_map, debug=True)
        update_ids = {key: val for d in mdata for key, val in d[2].items()}
        update_ids.update({x: -1 for x in missing_ids if x not in update_ids})

        self.titles_redirect_map.update(redirects)
        self.norm_map.update(norms)
        self.id_map.update(ids)
        self.id_map.update(update_ids)
        self.pageids_redirect_map.update({self.id_map[k] if k is not None else None: self.id_map[v] if v is not None else None
                                          for k, v in redirects.items()})
        
    async def get_redirects(self, session, titles=None, pageids=None, revids=None):
        if titles:
            if type(titles) == str:
                titles = [titles]
            titles = [self.norm_map.get(a, a) for a in titles]
            titles = [self.titles_redirect_map.get(a, a) for a in titles]
            titles = list(dict.fromkeys([a for a in titles if a]))
            titles = [a for a in titles if a not in self.collected_title_redirects]
            if not titles:
                return
        elif pageids:
            if (type(pageids) == int) | (type(pageids) == str):
                pageids = [int(pageids)]
            pageids = [self.pageids_redirect_map.get(int(a), int(a)) for a in pageids]
            pageids = list(dict.fromkeys([a for a in pageids if a is not None]))
            pageids = [a for a in pageids if a not in self.collected_pageid_redirects]
            if not pageids:
                return

        # TODO: handle revisions

        await self.fix_redirects(session, titles=titles, pageids=pageids, revids=revids)

        query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                            revids=revids, generator=False,
                                            norm_map=self.norm_map,
                                            titles_redirect_map=self.titles_redirect_map,
                                            pageids_redirect_map=self.pageids_redirect_map,
                                            params={'prop':'redirects', 'rdlimit': 'max'})

        data = await iterate_async_query(session, query_list, parse_fetched_redirects, debug=False)

        f_redirects = {key: val for d in data for key, val in d[0].items()}
        reverse_redirects = {x: k for k, v in f_redirects.items() for x in v}
        ids = {key: val for d in data for key, val in d[1].items()}

        self.collected_title_redirects.update(f_redirects)
        self.titles_redirect_map.update(reverse_redirects)
        self.id_map.update(ids)

        self.pageids_redirect_map.update({self.id_map[k]: self.id_map[v]
                                          for k, v in reverse_redirects.items()})
        self.collected_pageid_redirects.update({self.id_map[k]: [self.id_map[x] for x in v]
                                          for k, v in f_redirects.items()})
    
    def return_maps(self):
        return self.titles_redirect_map, self.pageids_redirect_map, self.norm_map, self.id_map, self.collected_title_redirects, self.collected_pageid_redirects

    def save_maps(self, path):
        #untested
        with open(path, 'wb') as f:
            pickle.dump(self.return_maps(), f)

    def load_maps(self, path):
        #untested
        with open(path, 'rb') as f:
            self.titles_redirect_map, self.pageids_redirect_map, self.norm_map, self.id_map, self.collected_title_redirects, self.collected_pageid_redirects = pickle.load(f)
    
    def __str__(self):
        return f"Redirects: {len(self.titles_redirect_map)}, Norms: {len(self.norm_map)}, IDs: {len(self.id_map)}, Existing: {len(self.collected_title_redirects)}"