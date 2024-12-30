import datetime
import mwapi
from .tools import chunks
from .api import *
from .redirects import *

async def parse_revision(data):
    """Parse single revision data from the API.

    Args:
        data (list): Data from the API.

    Returns:
        dict: revision data.
    """
    rev_info = {}
    for page in await data:
        rev_info[(page['pageid'], page['title'])] = page.get('revisions', [None])[0]
    return rev_info

async def get_revision(session, titles=None, pageids=None, date=None,
                       pagemaps=None, props=['timestamp', 'ids'],
                       return_props=None, async_args={}):
    """Get data for a particular revision of a page.

    Args:
        session (wikitoolkit.WTSession): The wikitoolkit session manager.
        titles (list, optional): Article titles. Defaul ts to None.
        pageids (list, optional): Page IDs. Defaults to None.
        date (str): Date to retrieve revision for. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        props (list, optional): Revision properties to collect. Defaults to ['timestamp', 'ids'].
        return_props (list, optional): Revision properties to return. Defaults to None.
        async_args (dict, optional): Arguments for the async query functions. Defaults to {}.

    Returns:
        dict: Revision data.
    """
    # Check if titles or pageids are provided
    if not (bool(titles) ^ bool(pageids)):
        raise ValueError('Must specify exactly one of title or pageid')
    
    # Check if pagemaps is provided
    if pagemaps is None:
        print('Warning: No PageMaps object provided, this is not recommended practice') # TODO: make this a proper warning
        pagemaps = PageMaps()    

    # Set default date if not provided
    if not date:
        date = datetime.datetime.now().isoformat()                   
    
    # Set query parameters
    params = {'prop': 'revisions', 'rvdir': 'older',
         'rvstart': date, 'rvlimit': 1, 'rvslots': 'main',
         'rvprop': '|'.join(props)}

    query_args_list, key, ix = querylister(titles, pageids, generator=True,
                pagemaps=pagemaps, params=params)

    # Execute the API query and parse the revision data
    data = await iterate_async_query(session.mw_session, query_args_list, function=parse_revision, continuation=False, **async_args)
    
    # Organize the revision data based on titles or pageids
    if titles:
        revision =  {k[1]: v for d in data for k, v in d.items()}
    elif pageids:
        revision =  {k[0]: v for d in data for k, v in d.items()}

    # Filter the revision data based on return_properties
    if return_props:
        if len(return_props) == 1:
            revision = {k: v[return_props[0]] for k, v in revision.items()}
        else:
            revision = {k: v for k, v in revision.items() if k in return_props}

    return revision

async def parse_revisions(data):
    """Parse revisions from the API.

    Args:
        data (list): Data from the API.

    Returns:
        dict: revisions data.
    """
    revisions = {}
    for page in await data:
        if ('pageid' not in page) | ('title' not in page):
            continue
        if (page['pageid'], page['title']) in revisions:
            revisions[(page['pageid'], page['title'])].extend(page.get('revisions', []))
        else:
            revisions[(page['pageid'], page['title'])] = page.get('revisions', [])

    return revisions

async def get_revisions(wtsession, titles=None, pageids=None, start=None, stop=None,
                  pagemaps=None, props=['timestamp', 'ids'], async_args={}):
    """Get revisions for a page between two dates.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        titles (list, optional): Article titles. Defaults to None.
        pageids (list, optional): Page IDs. Defaults to None.
        start (str): Start date. Defaults to None.
        stop (str): Stop date. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        props (list, optional): Revision properties to collect. Defaults to ['timestamp', 'ids'].
        async_args (dict, optional): Arguments for the async query functions. Defaults to {}.
    
    Returns:
        dict: Revisions data.
    """
    # Check if titles or pageids are provided
    if not (bool(titles) ^ bool(pageids)):
        raise ValueError('Must specify exactly one of title or pageid')
    
    # Check if pagemaps is provided
    if pagemaps is None:
        print('Warning: No PageMaps object provided, this is not recommended practice') # TODO: make this a proper warning
        pagemaps = PageMaps()    

    # Set default stop date if not provided
    if not stop: # TODO: tidy date types str format
        stop = datetime.datetime.now()
    
    # Set default start date if not provided
    if not start:
        start = (stop - datetime.timedelta(days=30)).isoformat()
        stop = stop.isoformat()                
    
    # Set query parameters
    params = {'prop': 'revisions', 'rvdir': 'newer',
         'rvstart': start, 'rvend': stop, 'rvlimit': 'max', 'rvslots': 'main',
         'rvprop': '|'.join(props)}
    
    query_args_list, key, ix = querylister(titles, pageids, generator=True,
                pagemaps=pagemaps, params=params)

    # Execute the API query and parse the revision data
    data = await iterate_async_query(wtsession.mw_session, query_args_list, function=parse_revisions, debug=False, **async_args)

    # Organize the revision data based on titles or pageids
    if titles:
        revisions =  {k[1]: v for d in data for k, v in d.items()}
    elif pageids:
        revisions =  {k[0]: v for d in data for k, v in d.items()}
    
    return revisions

async def parse_revisions_data(data):
    """Parse revisions data from the API.

    Args:
        data (list): Data from the API.

    Returns:
        dict: revisions data.
    """
    revisions_data = {}
    for page in await data:
        revisions_data.update({x['revid']: {k: v for k, v in x.items() if k!='revid'}
                                    for x in page['revisions']})
    return revisions_data

async def get_revisions_data(wtsession, revids, pagemaps=None, props=['timestamp', 'ids'], async_args={}):
    """Get data on specific revisions.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        revids (list): The revision IDs to collect data for.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        props (list, optional): Revision properties to collect. Defaults to ['timestamp', 'ids'].
        async_args (dict, optional): Arguments for the async query functions. Defaults to {}.

    Returns:
        dict: Revisions data
    """
    # Check if pagemaps is provided
    if pagemaps is None:
        print('Warning: No PageMaps object provided, this is not recommended practice') # TODO: make this a proper warning
        pagemaps = PageMaps()    
        
    if (type(revids) == int)| (type(revids) == str):
        revids = [revids]                 
    
    params = {'prop': 'revisions', 'rvslots': 'main', 'rvprop': '|'.join(props)}
    query_args_list, key, ix = querylister(revids=revids, pagemaps=pagemaps,
                                           params=params)

    data = await iterate_async_query(wtsession.mw_session, query_args_list, function=parse_revisions_data, debug=False, **async_args)
    revisions_data = {k:v for d in data for k, v in d.items()}

    return revisions_data

async def parse_revisions_content(data):
    """Parse revisions content from the API.

    Args:
        data (list): Data from the API.

    Returns:
        dict: revisions content.
    """
    revisions_content = {}
    for page in await data:
        revisions_content.update({x['revid']: x['slots']['main']['content']
                                for x in page['revisions']})
    return revisions_content

async def get_revisions_content(wtsession, revids, pagemaps=None, async_args={}):
    """Get revision content for a list of revision IDs.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        revids (list): The revision IDs to collect data for.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        async_args (dict, optional): Arguments for the async query functions. Defaults to {}.
    Returns:
        dict: The content of the revisions.
    """
    # Check if pagemaps is provided
    if pagemaps is None:
        print('Warning: No PageMaps object provided, this is not recommended practice') # TODO: make this a proper warning
        pagemaps = PageMaps()    
        
    # Check if revids is a single value and convert it to a list
    if (type(revids) == int) | (type(revids) == str):
        revids = [revids]                 
    
    # Construct the query arguments list for each chunk of revids
    params = {'prop': 'revisions', 'rvslots': 'main', 'rvprop': 'ids|content'}
    query_args_list, key, ix = querylister(revids=revids, pagemaps=pagemaps,
                                           params=params)    

    # Execute the API query and parse the revisions content
    data = await iterate_async_query(wtsession.mw_session, query_args_list, function=parse_revisions_content, debug=False, **async_args)
    
    # Combine the revisions content from different chunks into a single dictionary
    revisions_content = {k:v for d in data for k, v in d.items()}

    return revisions_content



async def pipeline_revisions(project, user_agent, mode, titles=None, pageids=None,
                             revids=None, pagemaps=None, rf_args={},
                             asynchronous=True, session_args={'formatversion':2}):
    """Runs full pipeline for getting revisions data from the API - creating a session, collecting redirects, collecting revisions. Runs asynchronously.

    Args:
        project (str): The Wikimedia project to query.
        user_agent (str): The user agent string to use.
        mode (str): The mode to use for collecting revisions. Must be one of 'single', 'range', 'data', or 'content'.
        titles (list, optional): The article titles to collect revision data for. Must specify exactly one of titles or pageids or revisions. Defaults to None.
        pageids (list, optional): The article IDs to collect revision data for. Must specify exactly one of titles or pageids or revisions. Defaults to None.
        revids (list, optional): The revisions IDs to collect revision data for. Must specify exactly one of titles or pageids or revisions. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        rf_args (dict, optional): Arguments to supply the revisions function. Defaults to {}.
        asynchronous (bool, optional): Whether to collect asynchronously. Defaults to True.
        session_args (dict, optional): Arguments for the mwapi session. Defaults to {'formatversion':2}.

    Raises:
        ValueError: If asynchronous is False (unsupported at present).

    Returns:
        dict: The revisions data, and optionally the pagemaps object.
    """

    # Check if pagemaps is provided
    if pagemaps is None:
        return_pm = True
        pagemaps = PageMaps()
    else:
        return_pm = False

    # Create an async session if asynchronous is True
    if asynchronous:
        wtsession = WTSession(project, user_agent, mw_session_args=session_args)
    else:
        raise ValueError('Only async supported at present.')
        wtsession = mwapi.Session(url, user_agent=user_agent, **session_args)

    mode_dict = {'single': get_revision, 'range': get_revisions,
            'data':get_revisions_data, 'content': get_revisions_content}

    # Perform necessary operations if asynchronous is True
    if asynchronous:
        # Fix redirects if titles are provided
        if titles:
            await pagemaps.fix_redirects(wtsession, titles=titles)
        # Declare the article list type based on the mode
        article_list = {'titles': titles, 'pageids': pageids, 'revids': revids}
        article_list = {k: v for k, v in article_list.items() if v}
        # Get revision data using the async session
        revisions = await mode_dict[mode](wtsession, pagemaps=pagemaps,
                                          **article_list, **rf_args)
        # Close the async session
        await wtsession.close()
    else:
        raise ValueError('Only async supported at present.') 

    if return_pm:
        return revisions, pagemaps
    else:
        return revisions

