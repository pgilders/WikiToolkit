from .tools import *
from .redirects import *
import mwapi
from mwviews.api import PageviewsClient
from mwviews import *


def api_article_views(wtsession, project, articles, redirects=True, pagemaps=None,
                      access='all-access', agent='all-agents', granularity='daily',
                      start=None, end=None, replace_nones=True, process=True):
    """Get pageviews for articles from the mwviews API.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        project (str): The wiki project.
        articles (list): List of article titles to get pageviews for.
        redirects (bool, optional): Whether to include redirects (and group pageviews by them). Defaults to True.
        pagemaps (PageMaps, optional): The PageMaps object to map redirects with. Defaults to None.
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

    # Check if pagemaps are provided
    if not pagemaps:
        if redirects:
            print('Warning: Redirects requested but no pagemaps provided. Redirects will not be found and combined.')
        pagemaps = PageMaps()

    # Process the articles using the provided norm_map and redirect_map
    if process:
        articles = process_articles(articles, pagemaps=pagemaps)
    
    # Get the article views using the mwviews client
    rdpv = wtsession.pv_client.article_views(project, articles, access, agent, granularity,
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
                    pv_grouped[pagemaps.titles_redirect_map.get(article.replace('_', ' '),
                                                                article.replace('_', ' '))
                               ] += int(value or 0)
                except KeyError:
                    pv_grouped[pagemaps.titles_redirect_map.get(article.replace('_', ' '),
                                                                article.replace('_', ' '))
                               ] = int(value or 0)
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
    
async def pipeline_api_article_views(project, user_agent, articles, pagemaps=None,
                               asynchronous=True, session_args={'formatversion':2},
                               client_args={}, aav_args={}):
    """Full process for getting pageviews for articles from the mwviews API. Resolves (and groups by) redirects and normalises titles.

    Args:
        project (str): The wiki project.
        articles (list): List of article titles to get pageviews for.
        user_agent (str): User agent for API requests.
        pagemaps (PageMaps, optional): The PageMaps object to map redirects with. Defaults to None.
        asynchronous (bool, optional): Whether to use asynchronous pipeline. Defaults to True.
        session_args (dict, optional): Arguments for mwapi session. Defaults to {'formatversion':2}.
        client_args (dict, optional): Arguments for mwviews client. Defaults to {}.
        aav_args (dict, optional): Arguments for api_article_views. Defaults to {}.

    Raises:
        ValueError: If synchronous pipeline is requested (not supported).

    Returns:
        dict: Pageviews for articles, and optionally id_map, redirect_map, norm_map, and existing_redirects.
    """
    
    # Check if pagemaps are provided
    if not pagemaps:
        rp = True
        pagemaps = PageMaps()
    else:
        rp = False

    # Construct the URL based on the project
    url = f'https://{project}.org'

    # Create the session and client objects based on the asynchronous flag
    if asynchronous:
        wtsession = WTSession(project, user_agent, mw_session_args=session_args,
                            pv_client_args=client_args)
    else:
        raise ValueError('Only async supported at present.')

    # If asynchronous, fix redirects and get existing redirects
    if asynchronous:
        await pagemaps.get_redirects(wtsession, articles)
        await wtsession.close()
    else:
        raise ValueError('Only async supported at present.')
    
    # get all the redirects into article list
    articles = process_articles(articles, pagemaps=pagemaps)
    articles = [y for x in articles for y in pagemaps.collected_title_redirects[x]]
    
    # Call the api_article_views function to get the pageviews
    pageviews = api_article_views(wtsession, project, articles, pagemaps=pagemaps,
                                  process=False, **aav_args)
    if rp:
        return pageviews, pagemaps
    else:
        return pageviews