from wikitools.tools import *
from wikitools.redirects import *
import asyncio
import aiohttp
import mwapi
from mwviews.api import PageviewsClient
from mwapi.errors import APIError
from mwviews import *


def api_article_views(client, project, articles, redirects=True, norm_map=None, redirect_map=None, access='all-access', agent='all-agents', granularity='daily',
            start=None, end=None, replace_nones=True):

    if (redirects) and (not redirect_map):
        raise ValueError('Redirects requested but no norm_map or redirect_map provided.')
    else:
        if not norm_map:
            norm_map = {}
        if not redirect_map:
            redirect_map = {}

    articles = process_articles(articles, norm_map=norm_map, titles_redirect_map=redirect_map)
    
    rdpv = client.article_views(project, articles, access, agent, granularity,
                                start, end)

    if redirects:
        grouped_rdpv = {}
        # loop through dates
        for date, pv in rdpv.items():
            # sum values in pv based on reverse_existing_redirects
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
    
    return_maps = {'id_map': id_map, 'redirect_map': redirect_map,
                   'norm_map': norm_map, 'existing_redirects': existing_redirects}
    return_bools = {k: v is None for k, v in return_maps.items()}

    for k, v in return_maps.items():
        if v is None:
            return_maps[k] = {}

    url = f'https://{project}.org'

    if asynchronous:
        async_session = mwapi.AsyncSession(url, user_agent=user_agent, **session_args)
    else:
        session = mwapi.Session(url, user_agent=user_agent, **session_args)
    client = PageviewsClient(user_agent=user_agent, **client_args)

    if asynchronous:
        await fix_redirects(async_session, articles, return_maps['redirect_map'],
                             return_maps['norm_map'], return_maps['id_map'])
        await get_redirects(async_session, articles, return_maps['existing_redirects'],
                            return_maps['redirect_map'], return_maps['norm_map'])
    else:
        raise ValueError('Only async supported at present.')
    
    pageviews = api_article_views(client, project, articles, norm_map=return_maps['norm_map'],
                                  redirect_map=return_maps['redirect_map'], **aav_args)


    return_dict = {'pageviews': pageviews}
    if any(return_bools.values()):
        for k, v in return_bools.items():
            if v:
                return_dict[k] = return_maps[k]
        return return_dict
    else:
        return pageviews
