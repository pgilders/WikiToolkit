import datetime
from wikitools.tools import chunks
from wikitools.api import *

async def parse_revision(data):
    rev_info = {}
    for page in await data:
        rev_info[(page['pageid'], page['title'])] = page['revisions'][0]
    return rev_info

async def get_revision(session, titles=None, pageids=None, date=None, props=['timestamp', 'ids'], return_props=None):
    """Get data for a particular revision of a page.

    Args:
        title (str): Article title.
        pageid (int): Page ID of article.
        date (_type_): Date to retrieve revision for.
        props (list, optional): Revision properties to collect. Defaults to ['timestamp', 'ids', 'content'].

    Returns:
        _type_: DataFrame with revision data.
    """
    if not (bool(titles) ^ bool(pageids)):
        raise ValueError('Must specify exactly one of title or pageid')
    if not date:
        date = datetime.datetime.now().isoformat()                   
    
    r = {'prop': 'revisions', 'rvdir': 'older',
         'rvstart': date, 'rvlimit': 1, 'rvslots': 'main',
         'rvprop': '|'.join(props)}
    if titles:
        query_args_list = [{**{'titles':t}, **r} for t in titles]
    elif pageids:
        query_args_list = [{**{'pageids':p}, **r} for p in pageids]
    
    data = await iterate_async_query(session, query_args_list, function=parse_revision, continuation=False)
    if titles:
        revision =  {k[1]: v for d in data for k, v in d.items()}
    elif pageids:
        revision =  {k[0]: v for d in data for k, v in d.items()}

    if return_props:
        if len(return_props) == 1:
            revision = {k: v[return_props[0]] for k, v in revision.items()}
        else:
            revision = {k: v for k, v in revision.items() if k in return_props}

    return revision

async def parse_revisions(data):
    revisions = {}
    for page in await data:
        if (page['pageid'], page['title']) in revisions:
            revisions[(page['pageid'], page['title'])].extend(page.get('revisions', []))
        else:
            revisions[(page['pageid'], page['title'])] = page.get('revisions', [])

    return revisions

async def get_revisions(session, titles=None, pageids=None, start=None, stop=None,
                  props=['timestamp', 'ids']):
    """Get revisions for a page between two dates.

    Args:
        title (str): Article title.
        pageid (int): Page ID of article.
        start (_type_): Start date.
        stop (_type_): Stop date.
        props (list, optional): Revision properties to collect. Defaults to ['timestamp', 'ids'].

    Returns:
        _type_: DataFrame with revision data.
    """
    if not (bool(titles) ^ bool(pageids)):
        raise ValueError('Must specify exactly one of title or pageid')
    if not stop:
        stop = datetime.now()
    if not start:
        start = stop - timedelta(days=30)                   
    
    r = {'prop': 'revisions', 'rvdir': 'newer',
         'rvstart': start, 'rvend': stop, 'rvlimit': 'max', 'rvslots': 'main',
         'rvprop': '|'.join(props)}
    if titles:
        query_args_list = [{**{'titles':t}, **r} for t in titles]
    elif pageids:
        query_args_list = [{**{'pageids':p}, **r} for p in pageids]

    data = await iterate_async_query(session, query_args_list, function=parse_revisions, debug=False)

    if titles:
        revisions =  {k[1]: v for d in data for k, v in d.items()}
    elif pageids:
        revisions =  {k[0]: v for d in data for k, v in d.items()}
    
    return revisions


async def parse_revisions_data(data):
    revisions_data = {}
    for page in await data:
        revisions_data.update({x['revid']: {k: v for k, v in x.items() if k!='revid'}
                                    for x in page['revisions']})
    return revisions_data

async def get_revisions_data(session, revids, props=['timestamp', 'ids']):
    """Get revisions for a page between two dates.

    Args:
        title (str): Article title.
        pageid (int): Page ID of article.
        start (_type_): Start date.
        stop (_type_): Stop date.
        props (list, optional): Revision properties to collect. Defaults to ['timestamp', 'ids'].

    Returns:
        _type_: DataFrame with revision data.
    """
    if (type(revids) == int)| (type(revids) == str):
        revids = [revids]                 
    
    tar_chunks = list(chunks(sorted(set(revids)), 50))
    query_args_list = [{'revids': '|'.join([str(x) for x in chunk]),
                        'prop': 'revisions',
                        'rvslots': 'main', 'rvprop': '|'.join(props)}
                        for chunk in tar_chunks]

    data = await iterate_async_query(session, query_args_list, function=parse_revisions_data, debug=False)
    revisions_data = {k:v for d in data for k, v in d.items()}

    return revisions_data

async def parse_revisions_content(data):
    revisions_content = {}
    for page in await data:
        revisions_content.update({x['revid']: x['slots']['main']['content']
                                for x in page['revisions']})
    return revisions_content

async def get_revisions_content(session, revids):
    """Get revisions for a page between two dates.

    Args:
        title (str): Article title.
        pageid (int): Page ID of article.
        start (_type_): Start date.
        stop (_type_): Stop date.
        props (list, optional): Revision properties to collect. Defaults to ['timestamp', 'ids'].

    Returns:
        _type_: DataFrame with revision data.
    """
    if (type(revids) == int)| (type(revids) == str):
        revids = [revids]                 
    
    tar_chunks = list(chunks(sorted(set(revids)), 50))
    query_args_list = [{'revids': '|'.join([str(x) for x in chunk]),
                        'prop': 'revisions',
                        'rvslots': 'main', 'rvprop': 'ids|content'}
                        for chunk in tar_chunks]

    data = await iterate_async_query(session, query_args_list, function=parse_revisions_content, debug=False)
    revisions_content = {k:v for d in data for k, v in d.items()}

    return revisions_content