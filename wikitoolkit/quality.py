import datetime
import aiohttp
import mwapi
from .tools import chunks
from .api import *
from .redirects import *
from .revisions import *


# TODO
# async def parse_quality(data, model):
#     """Parse quality from the API.

#     Args:
#         data (list): Data from the API.

#     Returns:
#         dict: quality data.
#     """

#     quals = {}
#     if model == 'articlequality':
#         print(data)
#         # quals.update(data)
#         for qual in await data:
#             print(qual)
#             if 'revision_id' in qual:
#                 quals[int(qual['revision_id'])] = qual['score']
#     elif model.split('-')[0] == 'revertrisk':
#         for qual in await data:
#             if 'revision_id' in qual:
#                 quals[int(qual['revision_id'])] = qual['output']['probabilities']['true']
#     elif model[2:6] == 'wiki':
#         if model.split('-')[1] in ['articlequality', 'draftquality']:
#             for qual in await data:
#                 for k, v in qual['enwiki']['scores'].items():
#                     quals[int(k)] = v[model.split('-')[1]]['score']['probability']

#             # quals = {int(k): v[model.split('-')[1]]['score']['probability']
#             #     for x in data for k, v in x['enwiki']['scores'].items()}
#         else:
#             for qual in await data:
#                 for k, v in qual[model[:6]]['scores'].items():
#                     quals[int(k)] = v[model.split('-')[1]]['score']['probability']['true']
#             # quals = {int(k): v[model.split('-')[1]]['score']['probability']['true']
#             #         for x in data if model[:6] in x for k, v in x[model[:6]]['scores'].items()}
#     else:
#         raise ValueError("Model not recognized")

#     return quals

# async def get_revisions_quality(session, revids, lang, models='articlequality'):

#     if type(revids) == int:
#         revids = [revids]
#     query_args_list = [{"rev_id": x, "lang": lang} for x in revids]

#     if type(models) == str:
#         models = [models]
#     revisions = {int(x): {} for x in revids}
#     for model in models:
#         quals = await iterate_async_query(session, query_args_list, function=parse_quality, f_args=[model],
#                                           httpmethod='POST',
#                                           posturl=f'/service/lw/inference/v1/models/{model}:predict')
    
#         for k, v in revisions.items():
#             if k in quals:
#                 v[model] = quals[k]

#     return revisions

async def get_revisions_quality(wtsession, revids, lang, models='articlequality'):
    """Get quality scores for revisions.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        revids (list): list of revision IDs.
        lang (str): language code.
        models (str|list, optional): The quality model(s) to use. Defaults to 'articlequality'.

    Raises:
        ValueError: If model not recognized.

    Returns:
        dict: revisions and their quality scores.
    """
    # Ensure revids is a list
    if type(revids) == int:
        revids = [revids]

    # Ensure models is a list
    if type(models) == str:
        models = [models]

    # Create a list of query arguments for each revision ID
    query_args_list = [{"rev_id": x, "lang": lang} for x in revids]
    
    # Initialize a dictionary to store revisions
    revisions = {int(x): {} for x in revids}
    
    # Iterate over each model to get quality scores
    for model in models:
        # Perform asynchronous query to get quality scores
        quals = await iterate_async_query(wtsession.lw_session, query_args_list, httpmethod='POST',
                                          posturl=f'/service/lw/inference/v1/models/{model}:predict')
        
        # Parse the quality scores based on the model type
        if model == 'articlequality':
            quals = {int(qual['revision_id']): qual['score'] for qual in quals
                     if 'revision_id' in qual}
        elif model.split('-')[0] == 'revertrisk':
            quals = {int(qual['revision_id']): qual['output']['probabilities']['true']
                     for qual in quals if 'revision_id' in qual}
        elif model[2:6] == 'wiki':
            if model.split('-')[1] in ['articlequality', 'draftquality']:
                quals = {int(k): v[model.split('-')[1]]['score']['probability']
                         for x in quals for k, v in x['enwiki']['scores'].items()}
            else:
                quals = {int(k): v[model.split('-')[1]]['score']['probability']['true']
                         for x in quals if model[:6] in x for k, v in x[model[:6]]['scores'].items()}
        else:
            raise ValueError("Model not recognized")
        
        # Update the revisions dictionary with the quality scores
        for k, v in revisions.items():
            if k in quals:
                v[model] = quals[k]

    return revisions


async def get_articles_quality(wtsession, titles=None, pageids=None, lang=None, date=None, start=None, stop=None, pagemaps=None, models='articlequality'):
    """Get quality scores for articles.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        titles (list, optional): List of titles to collect data for. Defaults to None.
        pageids (list, optional): List of page IDs to collect data for. Defaults to None.
        lang (str): language code.
        date (str, optional): Date to retrieve revision quality for. Defaults to None.
        start (str, optional): Start date. Defaults to None.
        stop (str, optional): Stop date. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        models (str|list, optional): The quality model(s) to use. Defaults to 'articlequality'.

    Raises:
        ValueError: If date or start and stop are not provided.

    Returns:
        dict: Articles and their quality scores.
    """
    # Check if a specific date is provided or if start and stop dates are not provided
    if date or not (start or stop):
        # Get the latest revision for the given titles or page IDs at the specified date
        revisions = await get_revision(wtsession, titles=titles, pageids=pageids, date=date,
                            pagemaps=pagemaps, props=['timestamp', 'ids'],
                            return_props=None)
        # Create a dictionary mapping each article to its revision ID
        rev_id_dict = {k: [int(v['revid'])] for k, v in revisions.items()}
    elif start or stop:
        # Get all revisions for the given titles or page IDs within the specified date range
        revisions = await get_revisions(wtsession, titles=titles, pageids=pageids, start=start, stop=stop,
                            pagemaps=pagemaps, props=['timestamp', 'ids'])
        # Create a dictionary mapping each article to a list of revision IDs
        rev_id_dict = {k: [int(v['revid']) for v in vlist] for k, vlist in revisions.items()}
    else:
        # Raise an error if neither date nor start and stop dates are provided
        raise ValueError('Either date or start and stop must be provided')
    
    # Flatten the list of revision IDs
    revids = [x for sublist in rev_id_dict.values() for x in sublist]
    
    # Get quality scores for the revisions
    quals = await get_revisions_quality(wtsession, revids, lang, models)

    # Update the revisions dictionary with the quality scores
    for k, v in revisions.items():
        if not (start or stop):
            # Update the single revision with its quality scores
            v.update(quals[v['revid']])
        else:
            # Update each revision in the list with its quality scores
            for rev in v:
                if rev['revid'] in quals:
                    rev.update(quals[rev['revid']])

    return revisions

async def pipeline_quality(project, user_agent, titles=None, pageids=None,
                           revids=None, pagemaps=None, models='articlequality',
                           qf_args={},
                           asynchronous=True, session_args={'formatversion':2}):
    """Runs full pipeline for getting article quality from the API - creating a session, collecting redirects, collecting quality. Runs asynchronously.


    Args:
        project (str): The Wikimedia project to query.
        user_agent (str): The user agent string to use.
        titles (list, optional): List of titles to collect data for. Defaults to None.
        pageids (list, optional): List of page IDs to collect data for. Defaults to None.
        revids (list): list of revision IDs.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        models (str|list, optional): The quality model(s) to use. Defaults to 'articlequality'.
        qf_args (dict, optional): Arguments for the quality function. Defaults to {}.
        asynchronous (bool, optional): Whether to collect asynchronously. Defaults to True.
        session_args (dict, optional): Arguments for the mwapi session. Defaults to {'formatversion':2}.

    Raises:
        ValueError: If asynchronous is False. Not yet implemented.

    Returns:
        dict: A dictionary of quality scores, and optionally the pagemaps object.
    """

    if pagemaps is None:
        return_pm = True
        pagemaps = PageMaps()
    else:
        return_pm = False

    # Construct the URL based on the project
    url = f'https://{project}.org'
    lang = project.split('.')[0]

    if type(models) == str:
        models = [models]

    # Create async sessions if asynchronous is True
    if asynchronous:
        wtsession = WTSession(project, user_agent, mw_session_args=session_args)

        # Fix redirects if titles are provided
        if titles:
            await pagemaps.fix_redirects(wtsession, titles=titles)
        # Declare the article list type based on the mode
        article_list = {'titles': titles, 'pageids': pageids, 'revids': revids}
        article_list = {k: v for k, v in article_list.items() if v}
        # Get revision quality data using the async session
        if revids:
            quality = {rev_id: {} for rev_id in revids}
            # go directly to revisions
            for model in models:
                quals = await get_revisions_quality(wtsession, revids, lang, model)
                for rev_id, qual in quals.items():
                    quality[rev_id][model] = qual
        else:
            # first resolve titles/pageids to revids
            quality = await get_articles_quality(wtsession, titles=titles, pageids=pageids,
                                lang=lang, **qf_args, pagemaps=pagemaps,
                                models=models)

        # Close the async session
        await wtsession.close()
    
    else:
        raise ValueError('Only async supported at present.')
        wtsession = mwapi.Session(url, user_agent=user_agent, **session_args)

    if return_pm:
        return quality, pagemaps
    else:
        return quality