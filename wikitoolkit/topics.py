# use liftwing api for topics
from .api import *
from .redirects import *
import mwapi
import aiohttp


async def get_articles_topics(wtsession, titles=None, revids=None, pagemaps=None, lang='en', model='outlink-topic-model', tf_args={}):
    """Get topic scores for articles using the lift wing API.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        titles (list, optional): List of titles to collect data for. Defaults to None.
        revids (list, optional): List of revision IDs to collect data for. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        lang (str, optional): language code. Defaults to 'en'.
        model (str, optional): The model to use for generating topics. Defaults to 'outlink-topic-model'.
        tf_args (dict, optional): Arguments for the topics function. Defaults to {}.

    Raises:
        ValueError: Exactly one of titles or revids must be provided.
        ValueError: outlink-topic-model requires titles.
        ValueError: ORES models require revids.

    Returns:
        dict: The topic scores for the articles.
    """
    # Ensure that exactly one of titles or revids is provided
    if (titles is None) == (revids is None):
        raise ValueError("One of titles or revids must be provided")
    if pagemaps is None:
        print('Warning: No PageMaps object provided, this is not recommended practice') # TODO: make this a proper warning
        pagemaps = PageMaps()

    # Handle the outlink-topic-model case
    if model == 'outlink-topic-model':
        # outlink-topic-model requires titles, not revids
        if revids:
            raise ValueError("outlink-topic-model requires titles")
        
        # Prepare the query arguments for each title
        titles = process_articles(titles=titles, pagemaps=pagemaps)
        query_args_list = [{"page_title": x, "lang": lang, **tf_args} for x in titles]
        
        # Perform the asynchronous query to get topics
        topics = await iterate_async_query(wtsession.lw_session, query_args_list, httpmethod='POST',
                                           posturl=f'/service/lw/inference/v1/models/{model}:predict')
        
        # Process the results into a dictionary
        topics = {x['prediction']['article'].split('wikipedia.org/wiki/')[1].replace('_', ' '):
                  {y['topic']: y['score'] for y in x['prediction']['results']} for x in topics}
    else:
        # Other models require revids, not titles
        if titles:
            raise ValueError(f"{model} requires revids")
        
        # Prepare the query arguments for each revision ID
        query_args_list = [{"rev_id": x, **tf_args} for x in revids]
        
        # Perform the asynchronous query to get topics
        topics = await iterate_async_query(wtsession.lw_session, query_args_list, httpmethod='POST',
                                           posturl=f'/service/lw/inference/v1/models/{model}:predict')
        
        # Process the results into a dictionary
        topics = {int(list(x[model.split('-')[0]]['scores'].keys())[0]):
                  x[model.split('-')[0]]['scores'][list(x[model.split('-')[0]]['scores'].keys())[0]]
                  [model.split('-')[1]]['score']['probability']
                  for x in topics}
    return topics


async def pipeline_topics(project, user_agent, titles=None, pageids=None, revids=None,
                          pagemaps=None, model='outlink-topic-model', tf_args={}, asynchronous=True, session_args={'formatversion':2}):
    """Runs full pipeline for getting topic data from the API - creating a session, collecting redirects, collecting topics. Runs asynchronously.

    Args:
        project (str): The Wikimedia project to query.
        user_agent (str): The user agent string to use.
        titles (list, optional): The article titles to collect revision data for. Must specify exactly one of titles or pageids or revisions. Defaults to None.
        pageids (list, optional): The article IDs to collect revision data for. Must specify exactly one of titles or pageids or revisions. Defaults to None.
        revids (list, optional): The revisions IDs to collect revision data for. Must specify exactly one of titles or pageids or revisions. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        model (str, optional): The model to use for generating topics. Defaults to 'outlink-topic-model'.
        tf_args (dict, optional): Arguments for the topics function. Defaults to {}.
        asynchronous (bool, optional): Whether to collect asynchronously. Defaults to True.
        session_args (dict, optional): Arguments for the mwapi session. Defaults to {'formatversion':2}.

    Raises:
        ValueError: If page IDs are provided. Not yet implemented.
        ValueError: If asynchronous is False. Not yet implemented.

    Returns:
        dict: The topic scores for the articles, and optionally the pagemaps object.
    """

    # Check if pagemaps is provided, if not, initialize a new PageMaps object
    if pagemaps is None:
        return_pm = True  # Flag to indicate that pagemaps should be returned
        pagemaps = PageMaps()  # Initialize a new PageMaps object
    else:
        return_pm = False  # Flag to indicate that pagemaps should not be returned

    # Construct the URL based on the project
    url = f'https://{project}.org'
    lang = project.split('.')[0]  # Extract the language code from the project

    # Create async sessions if asynchronous is True
    if asynchronous:
        wtsession = WTSession(project, user_agent, mw_session_args=session_args)

        # Fix redirects if titles are provided
        if titles:
            await pagemaps.fix_redirects(wtsession, titles=titles)
        elif pageids:
            raise ValueError("pageids not yet implemented")  # Raise error if pageids are provided
        
        # Get the topics for the articles
        topics = await get_articles_topics(wtsession, titles=titles, revids=revids, pagemaps=pagemaps,
                                           lang=lang, model=model, tf_args=tf_args)
        
        # Close the async sessions
        await wtsession.close()

    else:
        raise ValueError("Synchronous not yet implemented")  # Raise error if synchronous is requested

    if return_pm:
        return topics, pagemaps
    else:
        return topics