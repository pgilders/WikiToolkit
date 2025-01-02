from .tools import chunks
from .api import *
import pickle

async def basic_info(wtsession, titles=None, pageids=None, revids=None,
                     pagemaps=None, params={}, function=None, f_args={}, debug=False, async_args={}):
    """Runs a basic (customisable) API query for Wikipedia article information.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        titles (list, optional): The article titles to collect info for. Defaults to None.
        pageids (list, optional): The article IDs to collect info for. Defaults to None.
        revids (list, optional): The revision IDs to collect info for. Defaults to None.
        pagemaps (PageMaps, optional): The PageMaps object to map redirects with. Defaults to None.
        params (dict, optional): Query parameters. Defaults to {}.
        function (function, optional): Function to parse API output. Defaults to None.
        f_args (dict, optional): Arguments for parsing function. Defaults to {}.
        debug (bool, optional): Whether to produce debug output. Defaults to False.
        async_args (dict, optional): Arguments for the async query functions. Defaults to {}.

    Returns:
        list: Information from Wikipedia API.
    """
    
    # Construct the query list
    query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                           revids=revids, generator=False,
                                           pagemaps=pagemaps,
                                           params=params)

    # Execute the async query and parse the data
    data = await iterate_async_query(wtsession.mw_session, query_list, function, f_args=f_args, debug=debug, **async_args)

    return data

async def parse_redirects(data):
    """Parse redirect data from Wikipedia API response.

    Args:
        data (list): Data from the Wikipedia API.

    Returns:
        tuple: redirects, normalized titles, page ID maps.
    """
    redirects = {}
    norms = {}
    ids = {}
    for page in await data:
        # Extract redirects from the API response
        redirects.update({x['from']: x['to']
                          for x in page['query'].get('redirects', {})})
        # Handle missing pages
        redirects.update({l.get('title', None): None
                          for l in page['query'].get('pages', [])
                          if 'missing' in l})
        # Extract normalized titles from the API response
        norms.update({x['from']: x['to']
                        for x in page['query'].get('normalized', {})})
        # Extract page IDs from the API response
        ids.update({x['title']: x.get('pageid', -1)
                        for x in page['query'].get('pages', [])
                        if 'missing' not in x})
        
    return redirects, norms, ids

async def fix_redirects(wtsession, titles=None, pageids=None, revids=None,
                        pagemaps=None, async_args={}):
    """Gets the canonical page name for a list of articles. Updates the redirect map, norm map, and ID map in place.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        titles (list, optional): article titles to find canonical page for. Defaults to None.
        pageids (list, optional): article page IDs to find canonical page for. Defaults to None.
        revids (list, optional): article revision IDs to find canonical page for. Defaults to None.
        pagemaps (PageMaps, optional): The PageMaps object to update. Defaults to None.
        async_args (dict, optional): Arguments for the async query functions. Defaults to {}.
    """
    # Create a new PageMaps object if none was provided
    if pagemaps is None:
        pagemaps = PageMaps()
        rp = True
    else:
        rp = False

    # Construct the query list
    query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                        revids=revids, generator=False,
                                        pagemaps=pagemaps,
                                        params={'redirects':''})

    # Execute the async query and parse the data
    data = await iterate_async_query(wtsession.mw_session, query_list, parse_redirects, debug=True, **async_args)

    # Update the redirect map, norm map, and ID map with the extracted data
    await pagemaps.update_maps(wtsession, data)

    # Return the updated pagemaps object if none was provided
    if rp:
        return pagemaps

async def parse_fetched_redirects(data):
    """Parses fetched redirects from the Wikipedia API.

    Args:
        data (list): Data from the Wikipedia API.

    Returns:
        tuple: collected redirects, page IDs.
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

async def get_redirects(wtsession, titles=None, pageids=None, revids=None,
                        pagemaps=None, async_args={}):
    """Gets all redirects for a list of articles. Updates the collected redirects, redirect map, and ID map in place.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        titles (list, optional): article titles to find all redirects for. Defaults to None.
        pageids (list, optional): article page IDs to find all redirects for. Defaults to None.
        revids (list, optional): article revision IDs to find all redirects for. Defaults to None.
        pagemaps (PageMaps, optional): The PageMaps object to update. Defaults to None.
        async_args (dict, optional): Arguments for the async query functions. Defaults to {}.
    """
    # Create a new PageMaps object if none was provided
    if pagemaps is None:
        pagemaps = PageMaps()
        rp = True
    else:
        rp = False

    # Fix redirects in input titles first
    await pagemaps.fix_redirects(wtsession, titles=titles, pageids=pageids, revids=revids)

    # Construct the query list
    query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                        revids=revids, generator=False,
                                        pagemaps=pagemaps,
                                        params={'prop':'redirects', 'rdlimit': 'max'})

    # Execute the async query and parse the data
    data = await iterate_async_query(wtsession.mw_session, query_list, parse_fetched_redirects, debug=False, **async_args)

    # Update the collected redirects, redirect map, and ID map with the extracted data
    pagemaps.update_collected_redirect_maps(data)

    # Return the updated pagemaps object if none was provided
    if rp:
        return pagemaps

async def parse_wikidata(data):
    """Parses Wikidata IDs from the Wikipedia API.

    Args:
        data (list): Data from the Wikipedia API.

    Returns:
        dict: Titles to Wikidata IDs.
    """
    wikidata_ids = {v['title']: v['pageprops'].get('wikibase_item', None) if 'pageprops' in v else None
                             for v in await data}
    return wikidata_ids

class PageMaps:
    """A class for fixing, collecting, and managing redirect and ID data.
    """
    def __init__(self, titles_redirect_map=None, pageids_redirect_map=None,
                 norm_map=None, id_map=None, revid_map=None, wikidata_id_map=None,
                 collected_title_redirects=None,
                 collected_pageid_redirects=None, wikidata_map=None):
        """Initialise the PageMaps object.

        Args:
            titles_redirect_map (dict, optional): A dictionary of redirects to their canonical title. Defaults to None.
            pageids_redirect_map (dict, optional): A dictionary of redirect page IDs to their canonical page IDs. Defaults to None.
            norm_map (dict, optional): A dictionary of non-normalised to normalised titles. Defaults to None.
            id_map (dict, optional): A dictionary of titles to page IDs. Defaults to None.
            revid_map (dict, optional): A dictionary of revision IDs to their canonical page IDs. Defaults to None.
            wikidata_id_map (dict, optional): A dictionary of titles to Wikidata IDs. Defaults to None.
            collected_title_redirects (dict, optional): A dictionary of canonical titles to all their redirects. Defaults to None.
            collected_pageid_redirects (dict, optional): A dictionary of canonical page IDs to all their redirect page IDs. Defaults to None.
        """
        self.titles_redirect_map = titles_redirect_map if titles_redirect_map is not None else {}
        self.pageids_redirect_map = pageids_redirect_map if pageids_redirect_map is not None else {}
        self.norm_map = norm_map if norm_map is not None else {}
        self.id_map = id_map if id_map is not None else {}
        self.revid_map = revid_map if revid_map is not None else {} # not really used
        self.wikidata_id_map = wikidata_id_map if wikidata_id_map is not None else {}
        self.collected_title_redirects = collected_title_redirects if collected_title_redirects is not None else {}
        self.collected_pageid_redirects = collected_pageid_redirects if collected_pageid_redirects is not None else {}

    def filter_input(self, collected, titles=None, pageids=None, revids=None):
        """Filter the input titles or page IDs based on the already processed data.

        Args:
            collected (iterable): The already processed titles or page IDs.
            titles (list, optional): Article titles to filter. Defaults to None.
            pageids (list, optional): Article page IDs to filter. Defaults to None.
            revids (list, optional): Article revision IDs to filter. Defaults to None.

        Raises:
            ValueError: If more than one of titles, pageids, or revids is specified.
            ValueError: If revids are specified (not yet supported).

        Returns:
            tuple: Filtered titles, filtered page IDs, filtered revision IDs.
        """
        # Filter out already processed titles and page IDs
        if not bool(titles)^bool(pageids)^bool(revids):
            raise ValueError('Must specify exactly one of titles, pageids or revids')

        if titles:
            if type(titles) == str:
                titles = [titles]
            titles = [self.norm_map.get(a, a) for a in titles]
            titles = [self.titles_redirect_map.get(a, a) for a in titles]
            titles = list(dict.fromkeys([a for a in titles if a]))
            titles = [a for a in titles if a not in collected]
        elif pageids:
            if (type(pageids) == int) | (type(pageids) == str):
                pageids = [int(pageids)]
            pageids = [self.pageids_redirect_map.get(int(a), int(a)) for a in pageids]
            pageids = list(dict.fromkeys([a for a in pageids if a is not None]))
            pageids = [a for a in pageids if a not in collected]
        else:
            raise ValueError('Revision IDs not yet supported')
        
        return titles, pageids, revids

    async def update_maps(self, wtsession, data):
        """Updates the redirect maps, norm map, and ID map with the extracted API data.

        Args:
            wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
            data (list): Wikipedia API data.
        """
        
        # Extract the redirects, normalized titles, and page IDs from the data
        redirects = {key: val for d in data for key, val in d[0].items()}
        norms = {key: val for d in data for key, val in d[1].items()}
        ids = {key: val for d in data for key, val in d[2].items()}
        missing_ids = [k for k in redirects.keys() if (k not in ids)|(k not in self.id_map)]

        missing_ids = [k for k in redirects.keys() if k not in self.id_map]
        mdata = await basic_info(wtsession, titles=missing_ids, function=parse_redirects,
                                 pagemaps=self, debug=True)
        update_ids = {key: val for d in mdata for key, val in d[2].items()}
        update_ids.update({x: -1 for x in missing_ids if x not in update_ids})

        # update the maps
        self.titles_redirect_map.update(redirects)
        self.norm_map.update(norms)
        self.id_map.update(ids)
        self.id_map.update(update_ids)
        self.pageids_redirect_map.update({self.id_map[k] if k is not None else None: self.id_map[v] if v is not None else None
                                          for k, v in redirects.items()})
    
    async def update_collected_redirect_maps(self, data):
        """Updates the collected redirects, redirect maps, and ID maps with the extracted API data.

        Args:
            data (list): Data from the Wikipedia API.
        """
        f_redirects = {key: val for d in data for key, val in d[0].items()}
        reverse_redirects = {x: k for k, v in f_redirects.items() for x in v}
        ids = {key: val for d in data for key, val in d[1].items()}

        # Update the collected redirects, redirect map, and ID map with the extracted data
        self.titles_redirect_map.update(reverse_redirects)
        self.id_map.update(ids)
        self.pageids_redirect_map.update({self.id_map[k]: self.id_map[v]
                                          for k, v in reverse_redirects.items()})
        self.collected_title_redirects.update(f_redirects)
        self.collected_pageid_redirects.update({self.id_map[k]: [self.id_map[x] for x in v]
                                          for k, v in f_redirects.items()})
    
    async def update_wikidata_id_map(self, data):
        """Updates the Wikidata map with the extracted API data.

        Args:
            data (list): Data from the Wikipedia API.
        """
        wikidata_ids = {key: val for d in data for key, val in d.items()}
        self.wikidata_id_map.update(wikidata_ids)
        
    async def fix_redirects(self, wtsession, titles=None, pageids=None, revids=None, async_args={}):
        """Gets the canonical page name for a list of articles. Updates the redirect map, norm map, and ID map in place.

        Args:
            wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
            titles (list, optional): article titles to find canonical page for. Defaults to None.
            pageids (list, optional): article page IDs to find canonical page for. Defaults to None.
            revids (list, optional): article revision IDs to find canonical page for. Defaults to None.
            async_args (dict, optional): Arguments for the async query functions. Defaults to {}.
        """

        # Filter out already processed titles and page IDs
        if titles:
            collected = self.id_map
        elif pageids:
            collected = self.id_map.values()
        titles, pageids, revids = self.filter_input(collected, titles=titles,
                                                    pageids=pageids, revids=revids)
        # If no new titles or page IDs are left, finish
        if not any([titles, pageids, revids]):
            return
            
        # Construct the query list
        query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                            revids=revids, generator=False,
                                            pagemaps=self,
                                            params={'redirects':''})

        # Execute the async query and parse the data
        data = await iterate_async_query(wtsession.mw_session, query_list, parse_redirects, debug=True, **async_args)
        await self.update_maps(wtsession, data)
        
    async def get_redirects(self, wtsession, titles=None, pageids=None, revids=None, async_args={}):
        """Gets all redirects for a list of articles. Updates the collected redirects, redirect map, and ID map in place.

        Args:
            wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
            titles (list, optional): article titles to find all redirects for. Defaults to None.
            pageids (list, optional): article page IDs to find all redirects for. Defaults to None.
            revids (list, optional): article revision IDs to find all redirects for. Defaults to None.
            async_args (dict, optional): Arguments for the async query functions. Defaults to {}.
        """

        # Filter out already processed titles and page IDs
        if titles:
            collected = self.collected_title_redirects
        elif pageids:
            collected = self.collected_pageid_redirects
        titles, pageids, revids = self.filter_input(collected, titles=titles,
                                                    pageids=pageids, revids=revids)
        # If no new titles or page IDs are left, finish
        if not any([titles, pageids, revids]):
            return

        # TODO: handle revisions
        await self.fix_redirects(wtsession, titles=titles, pageids=pageids, revids=revids)

        query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                    revids=revids, generator=False,
                                    pagemaps=self,
                                    params={'prop':'redirects', 'rdlimit': 'max'})

        # Execute the async query and parse the data
        data = await iterate_async_query(wtsession.mw_session, query_list, parse_fetched_redirects, debug=False, **async_args)

        # Update the collected redirects, redirect map, and ID map with the extracted data
        await self.update_collected_redirect_maps(data)

    async def get_wikidata_ids(self, wtsession, titles=None, pageids=None, revids=None, async_args={}):
        """Gets the Wikidata item for a list of articles. Updates the Wikidata map in place.

        Args:
            wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
            titles (list, optional): article titles to find Wikidata item for. Defaults to None.
            pageids (list, optional): article page IDs to find Wikidata item for. Defaults to None.
            revids (list, optional): article revision IDs to find Wikidata item for. Defaults to None.
            async_args (dict, optional): Arguments for the async query functions. Defaults to {}.
        """
        # Filter out already processed titles and page IDs
        if titles:
            collected = self.wikidata_id_map
        elif pageids:
            raise ValueError('Page IDs not yet supported in this method') # TODO: not yet supported
        titles, pageids, revids = self.filter_input(collected, titles=titles,
                                                    pageids=pageids, revids=revids)
        # If no new titles or page IDs are left, finish
        if not any([titles, pageids, revids]):
            return
        
        # TODO: handle revisions
        await self.fix_redirects(wtsession, titles=titles, pageids=pageids, revids=revids)

        # Construct the query list
        query_list, key, ix = querylister(titles=titles, pageids=pageids,
                                            revids=revids, generator=False,
                                            pagemaps=self,
                                            params={'prop':'pageprops', 'ppprop':'wikibase_item'})

        # Execute the async query and parse the data
        data = await iterate_async_query(wtsession.mw_session, query_list, parse_wikidata, debug=False, **async_args)
        # Update the Wikidata map with the extracted data
        await self.update_wikidata_id_map(data)

    def return_maps(self):
        """Return the page maps.

        Returns:
            dict: A dictionary containing the titles redirect map, pageids redirect map, norm map, id map,
              collected title redirects, and collected pageid redirects.
        """
        return {'titles_redirect_map': self.titles_redirect_map,
                'pageids_redirect_map': self.pageids_redirect_map,
                'norm_map': self.norm_map, 'id_map': self.id_map,
                'wikidata_id_map': self.wikidata_id_map,
                'collected_title_redirects': self.collected_title_redirects,
                'collected_pageid_redirects': self.collected_pageid_redirects}

    def save_maps(self, path):
        """Saves the page maps to a file.

        Args:
            path (srt): File path to save the maps to.
        """
        #untested
        with open(path, 'wb') as f:
            pickle.dump(self.return_maps(), f)

    def load_maps(self, path):
        """Reads the page maps from a file.

        Args:
            path (str): File path to read the maps from.
        """
        #untested
        with open(path, 'rb') as f:
            data = pickle.load(f)
        self.titles_redirect_map = data['titles_redirect_map']
        self.pageids_redirect_map = data['pageids_redirect_map']
        self.norm_map = data['norm_map']
        self.id_map = data['id_map']
        self.collected_title_redirects = data['collected_title_redirects']
        self.collected_pageid_redirects = data['collected_pageid_redirects']
    
    def __str__(self):
        """Return a string representation of the PageMaps object.

        Returns:
            str: A string containing information about the number of titles in the titles_redirect_map,
                 the number of norms in the norm_map, the number of IDs in the id_map, and the number
                 of collected_title_redirects.
        """
        return f"Redirects: {len(self.titles_redirect_map)}, Norms: {len(self.norm_map)}, IDs: {len(self.id_map)}, Existing: {len(self.collected_title_redirects)}"