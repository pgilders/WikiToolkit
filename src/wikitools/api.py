import requests
import pandas as pd
import numpy as np
import mwapi
import asyncio
import aiohttp
from mwapi.errors import APIError
from wikitools.tools import *
# from wikitools.redirects import *

def query(session, query_args):
    return session.get(action='query', **query_args)

def iterate_query(continued, debug=False):
    try:
        for portion in continued:
            if debug:
                yield portion
            elif 'query' in portion:
                yield portion['query']['pages']
            else:
                print("MediaWiki returned empty result batch.")
    except APIError as error:
        raise ValueError("MediaWiki returned an error:", str(error))

def query_continued(session, query_args, debug=False):
    continued = session.get(action='query', continuation=True, **query_args)
    yield from iterate_query(continued, debug)

async def query_async(session, query_args, continuation=True, debug=False):
    continued = await asyncio.create_task(session.get(action='query',
                                                        continuation=continuation,
                                                        **query_args))
    if not continuation:
        if debug:
            return continued
        elif 'query' in continued:
            return continued['query']['pages']
        else:
            print("MediaWiki returned empty result batch.")
            return None
    
    pages = []
    try:
        async for portion in continued:
            if debug:
                pages.append(portion)
            elif 'query' in portion:
                for page in portion['query']['pages']:
                    pages.append(page)
            else:
                print("MediaWiki returned empty result batch.")
    except APIError as error:
        raise ValueError("MediaWiki returned an error:", str(error))
    return pages

async def iterate_async_query(session, query_args_list, function=None, args=[], continuation=True, debug=False):

    if function:
        tasks = [function(query_async(session, query_args, continuation, debug), *args)
                  for query_args in query_args_list]    
    else:
        tasks = [query_async(session, query_args, continuation, debug) for query_args in query_args_list]
    results = await asyncio.gather(*tasks)

    return results

def query_static(request, lang='en'):
    """
    Query Wikipedia API with specified parameters.

    Parameters
    ----------
    request : dict
        API call parameters.

    Raises
    ------
    ValueError
        Raises error if returned by API.

    Yields
    ------
    dict
        Subsequent dicts of json API response.

    """
    request['action'] = 'query'
    request['format'] = 'json'
    lastContinue = {}
    while True:
        # Clone original request
        req = request.copy()
        # Modify with values from the 'continue' section of the last result.
        req.update(lastContinue)
        # Call API
        result = requests.get(
            'https://%s.wikipedia.org/w/api.php' % lang, params=req).json()
        if 'error' in result:
            print('ERROR')
            # print(result['error'])
            raise ValueError(result['error'])
        if 'warnings' in result:
            print(result['warnings'])
        if 'query' in result:
            yield result['query']
        if 'continue' not in result:
            break
        lastContinue = result['continue']


def parse(request, lang='en'):
    """
    Query Wikipedia API with specified parameters.

    Parameters
    ----------
    request : dict
        API call parameters.

    Raises
    ------
    ValueError
        Raises error if returned by API.

    Yields
    ------
    dict
        Subsequent dicts of json API response.

    """
    request['action'] = 'parse'
    request['format'] = 'json'
    lastContinue = {}
    while True:
        # Clone original request
        req = request.copy()
        # Modify with values from the 'continue' section of the last result.
        req.update(lastContinue)
        # Call API
        result = requests.get(
            'https://%s.wikipedia.org/w/api.php' % lang, params=req).json()
        if 'error' in result:
            print('ERROR')
            # print(result['error'])
            raise ValueError(result['error'])
        if 'warnings' in result:
            print(result['warnings'])
        if 'parse' in result:
            yield result['parse']
        if 'continue' not in result:
            break
        lastContinue = result['continue']


def qores(revid, lang='enwiki', model=''):
    """Queries ORES API for a revision.

    Args:
        revid (int): Revision ID.
        lang (str, optional): Wiki version. Defaults to 'enwiki'.
        model (str, optional): ORES model to use. Defaults to ''.

    Returns:
        dict: JSON response from ORES API.
    """

    result = requests.get('https://ores.wikimedia.org/v3/scores/%s/%d/%s'
                          %(lang, revid, model)).json()
    return result


def querylister(titles=None, pageids=None, revids=None, generator=False,
                norm_map={}, titles_redirect_map={}, pageids_redirect_map={},
                params={}):
    if not ((titles is not None) ^ (pageids is not None) ^ (revids is not None)):
        raise ValueError('Must specify exactly one of titles, pageids or revids')
    if generator:
        cs = 1
    else:
        cs = 50

    if titles is not None:
        titles = process_articles(titles=titles, norm_map=norm_map, titles_redirect_map=titles_redirect_map)
        tar_chunks = list(chunks(list(set(titles)), cs))
        key = 'titles'
        ix = 1
    elif pageids is not None:
        # add redirects here?
        pageids = process_articles(pageids=pageids, pageids_redirect_map=pageids_redirect_map)
        tar_chunks = list(chunks([str(x) for x in set(pageids)], cs))
        key = 'pageids'
        ix = 0
    else:
        tar_chunks = list(chunks([str(x) for x in set(revids)], cs))
        key = 'revids'
        ix = -1
    
    query_args_list = [{key: '|'.join(chunk), **params}
                       for chunk in tar_chunks]
    
    return query_args_list, key, ix
