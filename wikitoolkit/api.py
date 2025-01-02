import requests
import asyncio
import aiohttp
from aiohttp import ClientResponseError
import mwapi
from mwapi.errors import APIError
from mwviews.api import PageviewsClient
from .tools import *

def query(session, query_args):
    """Run a query to the MediaWiki API.

    Args:
        session (wikitoolkit.WTSession.session): The wikitoolkit session.
        query_args (dict): The query arguments.

    Returns:
        _type_: _description_
    """
    return session.get(action='query', **query_args)

def iterate_query(continued, debug=False):
    """Iterate through a continued query.

    Args:
        continued (_type_): The continued query.
        debug (bool, optional): Whether to print debug output. Defaults to False.

    Raises:
        ValueError: Error returned by API.

    Yields:
        _type_: _description_
    """
    try:
        for portion in continued:
            if debug:
                yield portion  # Yield the portion if debug mode is enabled
            elif 'query' in portion:
                yield portion['query']['pages']  # Yield the pages if they exist in the portion
            else:
                print("MediaWiki returned empty result batch.")  # Print a message if the result batch is empty
    except APIError as error:
        raise ValueError("MediaWiki returned an error:", str(error))  # Raise a ValueError if there is an API error

def query_continued(session, query_args, debug=False):
    """Run a continued query to the MediaWiki API.

    Args:
        session (wikitoolkit.WTSession.session): The wikitoolkit session.
        query_args (dict): The query arguments.
        debug (bool, optional): Whether to print debug output. Defaults to False.

    Yields:
        _type_: _description_
    """
    continued = session.get(action='query', continuation=True, **query_args)
    yield from iterate_query(continued, debug)

async def query_async(session, query_args, continuation=True, debug=False, httpmethod='GET', posturl=None, max_retries=12, concurrency=100):
    """Create an async query to the MediaWiki API with semaphore and retry handling.

    Args:
        session (wikitoolkit.WTSession.session): The wikitoolkit session.
        query_args (dict): The query arguments.
        continuation (bool, optional): Whether to use continuation. Defaults to True.
        debug (bool, optional): Whether to print debug output. Defaults to False.
        httpmethod (str, optional): HTTP method to use. Defaults to 'GET'.
        posturl (str, optional): URL for POST requests. Defaults to None.
        max_retries (int, optional): Maximum number of retries. Defaults to 5.
        concurrency (int, optional): Maximum number of concurrent tasks. Defaults to 100.

    Raises:
        ValueError: Error returned by API.

    Returns:
        list: List of pages returned by API.
    """


    retries = 0
    backoff = 0.1  # Start with a 0.1-second delay
    semaphore = asyncio.Semaphore(concurrency)  # Limit the number of concurrent tasks

    async with semaphore:  # Limit the number of concurrent tasks
        while retries < max_retries:
            try:
                # Perform the query
                if httpmethod == 'GET':
                    continued = await asyncio.create_task(session.get(action='query',
                                                                      continuation=continuation,
                                                                      **query_args))
                elif httpmethod == 'POST':
                    async with session.post(url=posturl, json=query_args) as response:
                        if (response.status == 429)|(response.status == 503):
                            raise ClientResponseError(response.request_info, response.history, status=response.status)
                        continued = await response.json()
                    return continued
                else:
                    raise ValueError("Invalid HTTP method.")

                # Handle continuation
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

            except ValueError as error:
                await asyncio.sleep(backoff)
                retries += 1
                backoff *= 2  # Exponential backoff
                # raise ValueError("MediaWiki returned an error:", str(error))

            except ClientResponseError as error:
                if (error.status == 429)|(error.status == 503):
                    # Handle 429 Too Many Requests
                    if error.headers:
                        retry_after = int(error.headers.get("Retry-After", backoff)) if "Retry-After" in error.headers else backoff
                    else:
                        retry_after = backoff
                    # print(f"Received {error.status}. Retrying after {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    retries += 1
                    backoff *= 2  # Exponential backoff
                else:
                    raise  # Re-raise other client errors

            except Exception as e:
                raise
                print(f"Unexpected error: {e}")
                raise

        # Raise an error if max retries exceeded
        raise ValueError(f"Max retries exceeded for query: {query_args}")

async def iterate_async_query(session, query_args_list, function=None, f_args=[], continuation=True, debug=False, httpmethod='GET', posturl=None, max_retries=10, concurrency=100):
    """Iterate through a list of queries asynchronously.

    Args:
        session (wikitoolkit.WTSession.session): The wikitoolkit session.
        query_args_list (list): List of queries to run.
        function (function, optional): Function to parse API output. Defaults to None.
        f_args (dict, optional): Arguments for parsing function. Defaults to [].
        continuation (bool, optional): Whether to use continuation. Defaults to True.
        debug (bool, optional): Whether to print debug output. Defaults to False.
        max_retries (int, optional): Maximum number of retries. Defaults to 5.
        concurrency (int, optional): Maximum number of concurrent tasks. Defaults to 100.

    Returns:
        list: List of results from queries
    """
    # Create a list of tasks to be executed asynchronously
    if function:
        tasks = [function(query_async(session, query_args, continuation, debug, httpmethod, posturl, max_retries, concurrency), *f_args)
                  for query_args in query_args_list]    
    else:
        tasks = [query_async(session, query_args, continuation, debug, httpmethod, posturl, max_retries, concurrency) for query_args in query_args_list]
    
    # Execute the tasks asynchronously and gather the results
    results = await asyncio.gather(*tasks)

    return results

def query_static(request, lang='en'):
    """
    Query the Wikipedia API with specified parameters.

    Args:
        request (dict): API call parameters.
        lang (str, optional): Wiki version. Defaults to 'en'.

    Raises:
        ValueError: Raises an error if returned by the API.

    Yields:
        dict: Subsequent dictionaries of JSON API response.
    """
    # Set the action and format for the request
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


def parse_static(request, lang='en'):
    """
    Query the Wikipedia API with specified Parse parameters.

    Args:
        request (dict): API call parameters.
        lang (str, optional): Wiki version. Defaults to 'en'.

    Raises:
        ValueError: Raises an error if returned by the API.

    Yields:
        dict: Subsequent dictionaries of JSON API response.
    """
    # Set the action and format for the request
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
                pagemaps=None, params={}):
    """Creates a list of queries for the Wikipedia API. Normalises and redirects article titles or pageids.

    Args:
        titles (list, optional): The article titles to collect info for. Defaults to None.
        pageids (list, optional): The article IDs to collect info for. Defaults to None.
        revids (list, optional): The revision IDs to collect info for. Defaults to None.
        generator (bool, optional): Whether to use API generator option. Defaults to False.
        pagemaps (PageMaps, optional): The PageMaps object to map redirects with. Defaults to None.
        params (dict, optional): Query parameters. Defaults to {}.

    Raises:
        ValueError: Must specify exactly one of titles, pageids or revids

    Returns:
        tuple: List of query arguments, key for query, and an identifying index.
    """
    # Check if exactly one of titles, pageids, or revids is specified
    if not ((titles is not None) ^ (pageids is not None) ^ (revids is not None)):
        raise ValueError('Must specify exactly one of titles, pageids or revids')
    
    # Determine the chunk size based on the generator flag
    if generator:
        cs = 1
    else:
        cs = 50

    if titles is not None:
        # Process article titles and handle redirects
        titles = process_articles(titles=titles, pagemaps=pagemaps)
        tar_chunks = list(chunks(list(set(titles)), cs))
        key = 'titles'
        ix = 1
    elif pageids is not None:
        # Process page IDs and handle redirects
        pageids = process_articles(pageids=pageids, pagemaps=pagemaps)
        tar_chunks = list(chunks([str(x) for x in set(pageids)], cs))
        key = 'pageids'
        ix = 0
    else:
        # Process revision IDs
        tar_chunks = list(chunks([str(x) for x in set(revids)], cs))
        key = 'revids'
        ix = -1
    
    # Create a list of query arguments for each chunk
    query_args_list = [{key: '|'.join(chunk), **params}
                       for chunk in tar_chunks]
    
    return query_args_list, key, ix

class WTSession:
    """Session manager for querying the MediaWiki APIs.

    Args:
        project (str): The project name.
        user_agent (str): The user agent string.
        headers (dict, optional): HTTP headers. Defaults to {}.
        mw_session_args (dict, optional): mwapi session arguments. Defaults to {'formatversion':2}.
        lw_session_args (dict, optional): aiohttp session arguments. Defaults to {}.
        pv_client_args (dict, optional): PageviewsClient arguments. Defaults to {}.
        r_session_args (dict, optional): requests session arguments. Defaults to {}.
    """
    def __init__(self, project, user_agent, headers={},
                 mw_session_args={'formatversion':2},
                 lw_session_args={}, pv_client_args={}):
        mw_url = f'https://{project}.org'
        self.user_agent = user_agent
        self.mw_session = mwapi.AsyncSession(mw_url, user_agent=user_agent, **mw_session_args)
        self.pv_client = PageviewsClient(user_agent=user_agent, **pv_client_args)
        self.lw_session = aiohttp.ClientSession('https://api.wikimedia.org',
                                                headers=headers.update({'user-agent': user_agent}),
                                                **lw_session_args)
        self.r_session = requests.Session()
        self.r_session.headers.update({'user-agent': user_agent})
        self.r_session.headers.update(headers)

    async def close(self):
        """Close the session objects."""
        await self.mw_session.session.close()
        await self.lw_session.close()

    def __str__(self):
        return f"WTSession object with user agent {self.user_agent}"