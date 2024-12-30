from .api import *
from .redirects import *
import mwapi
import time


async def parse_links(data, prop):
    """Parse links from API data.

    Args:
        data (list): Data from the API.
        prop (str): The kind of link data to parse.

    Returns:
        tuple|dict: The parsed link data, including redirects, normalisations and pageids where appropriate.
    """

    # For regular in/out-links
    if prop in ['links', 'linkshere']:
        # Initialize variables
        links = []
        redirects = {}
        norms = {}
        ids = {}

        for page in await data:
            if 'query' not in page:
                if 'title' in page:
                    links.append(page)
                else:
                    links = None
            
            else:
                # Collect links
                links.extend(page['query'].get('pages', []))
                
                # Collect redirects
                redirects.update({x['from']: x['to']
                                for x in page['query'].get('redirects', {})})
                redirects.update({l['title']: None
                                for l in page['query'].get('pages', [])
                                if 'missing' in l})
                
                # Collect normalizations
                norms.update({x['from']: x['to']
                                for x in page['query'].get('normalized', {})})
                
                # Collect page ids
                ids.update({x['title']: x.get('pageid', -1)
                                for x in page['query'].get('pages', [])})

        return links, redirects, norms, ids

    # Handle other types of link data (langlinks, iwlinks, extlinks)
    links = {}
    for page in await data:
        new_links = {(page['pageid'], page['title']): page.get(prop, [])}
        for k, v in new_links.items():
            if k in links:
                links[k].extend(v)
            else:
                links[k] = v

    # Handle langlinks data
    if prop == 'langlinks':
        langlinks = {art: {} for art in links.keys()}
        for k, v in links.items():
            for l in v:
                if l['lang'] in langlinks[k]:
                    langlinks[k][l['lang']].append(l['title'])
                else:
                    langlinks[k][l['lang']] = [l['title']]
        return langlinks
    else:
        return links

async def get_links(wtsession, mode='out', titles=None, pageids=None, pagemaps=None, namespaces=[0], update_maps=False, batchsize=200, async_args={}):
    """Get links to/from a list of articles from the API. Runs asynchronously.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        mode (str / list, optional): The kind of links to get. Defaults to 'out'.
        titles (list, optional): The article titles to collect links for. Must specify exactly one of titles or pageids. Defaults to None.
        pageids (list, optional): The article IDs to collect links for. Must specify exactly one of titles or pageids. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        namespaces (list, optional): The wiki namespaces to collect links from. Defaults to [0].
        update_maps (bool, optional): Whether to update maps on link collection. Defaults to False.
        batchsize (int, optional): How many pages to collect links for at a time - for rate limiting purposes. Defaults to 200.
        async_args (dict, optional): Arguments for the async query functions. Defaults to {}.

    Raises:
        ValueError: If titles and pageids are not specified or if both are specified.

    Returns:
        dict: A dictionary of links (format depends on mode(s)).
    """

    # Check if titles or pageids are specified
    if not (bool(titles) ^ bool(pageids)):
        raise ValueError('Must specify exactly one of titles or pageids')
    if pagemaps is None:
        if update_maps:
            raise ValueError('Must provide PageMaps object if update_maps is True')
        print('Warning: No PageMaps object provided, this is not recommended practice') # TODO: make this a proper warning
        pagemaps = PageMaps()

    # Convert namespaces to string representation
    if namespaces == 'all':
        ns = '*'
    else:
        ns = '|'.join([str(x) for x in namespaces])

    # Define dictionary for different modes of link data
    modedict = {'out': {'pg':'generator', 'pval': 'links', 'ns': 'gplnamespace', 'limit': 'gpllimit'},
                'in': {'pg':'generator', 'pval': 'linkshere', 'ns': 'glhnamespace', 'limit': 'glhlimit'},
                'lang': {'pg':'prop', 'pval': 'langlinks', 'limit': 'lllimit'},
                'interwiki': {'pg':'prop', 'pval': 'iwlinks', 'limit': 'iwlimit'},
                'ext': {'pg':'prop', 'pval': 'extlinks', 'limit': 'ellimit'}}
    
    # Convert mode to list if it is a string
    if type(mode) == str:
        if mode == 'all':
            mode = ['out', 'in', 'lang', 'interwiki', 'ext']
        else:
            mode = [mode]
    
    # Collect links for each mode
    return_dict = {}
    for m in mode:
        print('Getting %s-links' % m)
        # Define parameters for the API query
        params = {modedict[m]['pg']: modedict[m]['pval'],
                  modedict[m]['limit']: 'max',
                  'redirects':update_maps}
        if m in ['out', 'in']:
            params[modedict[m]['ns']] = ns

        # Iterate through the articles in batches, try/except block to handle rate errors
        n = 0
        links = {}
        size = len(titles) if titles else len(pageids)
        while n < size:
            # Get the batch of articles (titles or pageids)
            if titles:
                b_titles = titles[n:n+batchsize]
                b_pageids = None
            else:
                b_titles = None
                b_pageids = pageids[n:n+batchsize]
            try:
                # Create a list of query arguments
                query_args_list, key, ix = querylister(b_titles, b_pageids,
                                                    generator=(m in ['out', 'in']),
                                                    pagemaps=pagemaps,
                                                    params=params)
                # Query the API for the links
                data = await iterate_async_query(wtsession.mw_session, query_args_list,
                                                function=parse_links, f_args=[modedict[m]['pval']],
                                                debug=update_maps&(m in ['out', 'in']), **async_args)

                # Parse the data for regular out/in-links and update the maps if necessary
                if m in ['out', 'in']:
                    data_keys = [x[key] for x in query_args_list]
                    missing = [data_keys[n] for n in range(len(data)) if data[n][0] is None]
                    b_links = dict(zip(data_keys, [x[0] for x in data]))
                    if update_maps:
                        update_data = [x[1:] for x in data]
                        missing_data = [({x: None for x in missing}, {}, {x: -1 for x in missing})]
                        await pagemaps.update_maps(wtsession, update_data + missing_data)
                else:
                    b_links = {k[ix]: val for d in data for k, val in d.items()}

            # handle rate error, this doesn't all run automatically, so need to address further issues
            # except (ValueError, ConnectionError, ClientConnectorError, ConnectionResetError) as v: ????
            except Exception as v: # TODO: Be more specific here
                print(v)
                #split arts in half and try again
                batchsize = max(batchsize // 2, 1)
                time.sleep(10)
                print('%.2f%% complete' % (100*n/size))
                print('Trying again at n=%d with batchsize=%d' % (n, batchsize))
                continue

            # Update the links dictionary with the links from the batch
            links.update(b_links)

            # Update the index to start collecting the next batch
            n += batchsize
            # Update the batchsize if necessary
            if batchsize < 200:
                batchsize = min(batchsize * 2, 200)
                print('Increasing batchsize to %d' % batchsize)

        # Add the full links of each type to the return dictionary
        return_dict[m] = links
    
    # Return the dictionary of links
    if len(return_dict) == 1:
        return return_dict[mode[0]]
    else:
        return return_dict

async def pipeline_get_links(project, user_agent, titles=None, pageids=None, pagemaps=None,
                             gl_args={'update_maps':True}, asynchronous=True, session_args={'formatversion':2}):
    """Runs full pipeline for getting links from the API - creating a session, collecting redirects, collecting links. Runs asynchronously.

    Args:
        project (str): The Wikimedia project to query.
        user_agent (str): The user agent string to use.
        titles (list, optional): The article titles to collect links for. Must specify exactly one of titles or pageids. Defaults to None.
        pageids (list, optional): The article IDs to collect links for. Must specify exactly one of titles or pageids. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        gl_args (dict, optional): Arguments to supply the get_links function. Defaults to {'update_maps':True}.
        asynchronous (bool, optional): Whether to collect asynchronously. Defaults to True.
        session_args (dict, optional): Arguments for the mwapi session. Defaults to {'formatversion':2}.

    Raises:
        ValueError: If asynchronous is False (unsupported).

    Returns:
        dict: A dictionary of links, and optionally the pagemaps object.
    """

    # Create a PageMaps object if not provided
    if pagemaps is None:
        return_pm = True
        pagemaps = PageMaps()
    else:
        return_pm = False

    # Construct the URL based on the project
    url = f'https://{project}.org'

    # Create an async session if asynchronous is True
    if asynchronous:
        wtsession = WTSession(project, user_agent, mw_session_args=session_args)
    else:
        raise ValueError('Only async supported at present.')
        wtsession = mwapi.Session(url, user_agent=user_agent, **session_args)

    # Perform necessary operations if asynchronous is True
    if asynchronous:
        # Fix redirects if titles are provided
        if titles:
            await pagemaps.fix_redirects(wtsession, titles=titles)
        
        # Get links using the async session
        links = await get_links(wtsession, titles=titles, pageids=pageids, pagemaps=pagemaps, **gl_args)
        
        # Close the async session
        await wtsession.close()
    else:
        raise ValueError('Only async supported at present.')
    
    if return_pm:
        return links, pagemaps
    else:
        return links