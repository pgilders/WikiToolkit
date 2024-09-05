from wikitools.tools import *
from wikitools.redirects import *
import mwapi
from mwviews.api import PageviewsClient
from mwviews import *


def api_article_views(client, project, articles, redirects=True, norm_map=None,
                      redirect_map=None, access='all-access', agent='all-agents',
                      granularity='daily', start=None, end=None, replace_nones=True):
    """Get pageviews for articles from the mwviews API.

    Args:
        client (mwviews.api.PageviewsClient): mwviews client
        project (str): The wiki project.
        articles (list): List of article titles to get pageviews for.
        redirects (bool, optional): Whether to include redirects (and group pageviews by them). Defaults to True.
        norm_map (dict, optional): The map for title normalisation. Defaults to None.
        redirect_map (dict, optional): The map for title redirects. Defaults to None.
        access (str, optional): access method (desktop, mobile-web, mobile-app, all-access). Defaults to 'all-access'.
        agent (str, optional): user agent type (spider, user, bot, all-agents). Defaults to 'all-agents'.
        granularity (str, optional): daily or monthly counts. Defaults to 'daily'.
        start (str|date, optional): The start date to get pageviews from. Defaults to None.
        end (str|date, optional): The end date to get pageviews to. Defaults to None.
        replace_nones (bool, optional): Whether to replace None values (page not existing) with 0. Defaults to True.

    Raises:
        ValueError: Redirects requested but no norm_map or redirect_map provided.

    Returns:
        dict: Pageviews for articles.
    """

    # Check if redirects are requested and if redirect_map is provided
    if (redirects) and (not redirect_map):
        raise ValueError('Redirects requested but no norm_map or redirect_map provided.')
    else:
        # If norm_map is not provided, set it to an empty dictionary
        if not norm_map:
            norm_map = {}
        # If redirect_map is not provided, set it to an empty dictionary
        if not redirect_map:
            redirect_map = {}

    # Process the articles using the provided norm_map and redirect_map
    articles = process_articles(articles, norm_map=norm_map, titles_redirect_map=redirect_map)
    
    # Get the article views using the mwviews client
    rdpv = client.article_views(project, articles, access, agent, granularity,
                                start, end)

    # If redirects are requested, group the pageviews by redirects
    if redirects:
        grouped_rdpv = {}
        # Loop through dates
        for date, pv in rdpv.items():
            # Sum values in pv based on reverse_existing_redirects
            pv_grouped = {}
            for article, value in pv.items():
                try:
                    pv_grouped[redirect_map[article.replace('_', ' ')]] += int(value or 0)
                except KeyError:
                    pv_grouped[redirect_map[article.replace('_', ' ')]] = int(value or 0)
            grouped_rdpv[date] = pv_grouped
        return grouped_rdpv
    else:
        if replace_nones:
            # Replace None values with 0 if replace_nones is True
            rdpv = {date: {art.replace('_', ' '): int(val or 0) for art, val in pv.items()}
                    for date, pv in rdpv.items()}
        else:
            rdpv = {date: {art.replace('_', ' '): val for art, val in pv.items()}
                    for date, pv in rdpv.items()}
                
        return rdpv
    
async def pipeline_api_article_views(project, articles, user_agent, id_map=None,
                               redirect_map=None, norm_map=None, existing_redirects=None,
                               asynchronous=True, session_args={'formatversion':2},
                               client_args={}, aav_args={}):
    """Full process for getting pageviews for articles from the mwviews API. Resolves (and groups by) redirects and normalises titles.

    Args:
        project (str): The wiki project.
        articles (list): List of article titles to get pageviews for.
        user_agent (str): User agent for API requests.
        id_map (dict, optional): The map for title to pageids. Defaults to None.
        redirect_map (dict, optional): The map for title redirects. Defaults to None.
        norm_map (dict, optional): The map for title normalisation. Defaults to None.
        existing_redirects (dict, optional): All redirects for selected articles. Defaults to None.
        asynchronous (bool, optional): Whether to use asynchronous pipeline. Defaults to True.
        session_args (dict, optional): Arguments for mwapi session. Defaults to {'formatversion':2}.
        client_args (dict, optional): Arguments for mwviews client. Defaults to {}.
        aav_args (dict, optional): Arguments for api_article_views. Defaults to {}.

    Raises:
        ValueError: If synchronous pipeline is requested (not supported).

    Returns:
        dict: Pageviews for articles, and optionally id_map, redirect_map, norm_map, and existing_redirects.
    """
    
    # Create dictionaries to store the maps and check if they are None
    return_maps = {'id_map': id_map, 'redirect_map': redirect_map,
                   'norm_map': norm_map, 'existing_redirects': existing_redirects}
    return_bools = {k: v is None for k, v in return_maps.items()}

    # If any of the maps are None, initialize them as empty dictionaries
    for k, v in return_maps.items():
        if v is None:
            return_maps[k] = {}

    # Construct the URL based on the project
    url = f'https://{project}.org'

    # Create the session and client objects based on the asynchronous flag
    if asynchronous:
        async_session = mwapi.AsyncSession(url, user_agent=user_agent, **session_args)
    else:
        session = mwapi.Session(url, user_agent=user_agent, **session_args)
    client = PageviewsClient(user_agent=user_agent, **client_args)

    # If asynchronous, fix redirects and get existing redirects
    if asynchronous:
        await fix_redirects(async_session, articles, return_maps['redirect_map'],
                             return_maps['norm_map'], return_maps['id_map'])
        await get_redirects(async_session, articles, return_maps['existing_redirects'],
                            return_maps['redirect_map'], return_maps['norm_map'])
    else:
        raise ValueError('Only async supported at present.')
    
    # Call the api_article_views function to get the pageviews
    pageviews = api_article_views(client, project, articles, norm_map=return_maps['norm_map'],
                                  redirect_map=return_maps['redirect_map'], **aav_args)

    # Create the return dictionary with pageviews and optional maps
    return_dict = {'pageviews': pageviews}
    if any(return_bools.values()):
        for k, v in return_bools.items():
            if v:
                return_dict[k] = return_maps[k]
        return return_dict
    else:
        return pageviews
