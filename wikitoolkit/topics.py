# use liftwing api for topics
from .api import *
from .redirects import *
import mwapi
import aiohttp
import time

ccols = ['Culture.Media.Media*',
                    'Culture.Media.Television',
                    'Culture.Media.Films',
                    'Geography.Regions.Europe.Europe*',
                    'Geography.Regions.Americas.North_America',
                    'Geography.Regions.Europe.Western_Europe',
                    'Geography.Regions.Europe.Northern_Europe',
                    'Culture.Media.Entertainment',
                    'Culture.Visual_arts.Visual_arts*',
                    'Culture.Biography.Biography*',
                    'History_and_Society.Society',
                    'Culture.Literature',
                    'Culture.Internet_culture',
                    'Culture.Biography.Women',
                    'Geography.Regions.Europe.Southern_Europe',
                    'Culture.Media.Music',
                    'STEM.STEM*',
                    'History_and_Society.Business_and_economics',
                    'Culture.Visual_arts.Comics_and_Anime',
                    'Culture.Performing_arts',
                    'Culture.Media.Software',
                    'Culture.Media.Video_games',
                    'Culture.Philosophy_and_religion',
                    'Culture.Media.Books',
                    'STEM.Computing',
                    'Culture.Visual_arts.Fashion',
                    'Geography.Regions.Asia.Asia*',
                    'Geography.Regions.Africa.Africa*',
                    'History_and_Society.Politics_and_government',
                    'History_and_Society.History',
                    'History_and_Society.Military_and_warfare',
                    'Geography.Regions.Europe.Eastern_Europe',
                    'STEM.Technology',
                    'Geography.Regions.Asia.East_Asia',
                    'Geography.Regions.Oceania',
                    'Culture.Sports',
                    'Geography.Regions.Americas.South_America',
                    'History_and_Society.Transportation',
                    'STEM.Medicine_&_Health',
                    'Geography.Regions.Asia.West_Asia',
                    'Geography.Regions.Asia.South_Asia',
                    'STEM.Biology',
                    'Geography.Regions.Americas.Central_America',
                    'Geography.Regions.Africa.Southern_Africa',
                    'Geography.Geographical',
                    'Geography.Regions.Asia.Southeast_Asia',
                    'Culture.Linguistics',
                    'Culture.Food_and_drink',
                    'History_and_Society.Education',
                    'Culture.Media.Radio',
                    'Geography.Regions.Asia.North_Asia',
                    'Culture.Visual_arts.Architecture',
                    'STEM.Libraries_&_Information',
                    'Geography.Regions.Africa.Northern_Africa',
                    'STEM.Space',
                    'STEM.Earth_and_environment',
                    'STEM.Physics',
                    'STEM.Mathematics',
                    'Geography.Regions.Africa.Eastern_Africa',
                    'Geography.Regions.Africa.Central_Africa',
                    'Geography.Regions.Asia.Central_Asia',
                    'Geography.Regions.Africa.Western_Africa',
                    'STEM.Chemistry',
                    'STEM.Engineering']

def get_articles_topics_sync(wtsession, titles=None, revids=None, pagemaps=None, lang='en', model='outlink-topic-model', tf_args={},
                                max_retries=5, backoff_time=1, backoff_factor=2):
    """Get topic scores for articles using the lift wing API.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        titles (list, optional): List of titles to collect data for. Defaults to None.
        revids (list, optional): List of revision IDs to collect data for. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        lang (str, optional): language code. Defaults to 'en'.
        model (str, optional): The model to use for generating topics. Defaults to 'outlink-topic-model'.
        tf_args (dict, optional): Arguments for the topics function. Defaults to {}.
        max_retries (int, optional): Maximum number of retries for the async query functions. Defaults to 5.
        backoff_time (int, optional): Time to wait between retries. Defaults to 1.
        backoff_factor (int, optional): Factor to increase backoff time. Defaults to 2.

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

    murl = 'https://api.wikimedia.org' + f'/service/lw/inference/v1/models/{model}:predict'         
    # Handle the outlink-topic-model case
    if model == 'outlink-topic-model':
        # outlink-topic-model requires titles, not revids
        if revids:
            raise ValueError("outlink-topic-model requires titles")
        
        # Prepare the query arguments for each title
        titles = process_articles(titles=titles, pagemaps=pagemaps)
        if 'threshold' in tf_args:
            tf_args['threshold'] = float(tf_args['threshold']) # Ensure that the threshold is a float - weird bug at 0 otherwise
        
        if 'Main Page' in titles:
            titles.remove('Main Page')
            topics = {'Main Page': {k: None for k in ccols}}
        else:
            topics = {}

        remaining_retries = max_retries
        while (remaining_retries > 0) & (len(titles) > 0):
            query_args_list = [{"page_title": x, "lang": lang, **tf_args} for x in titles]
            
            # Perform the synchronous query to get topics        
            gtopics = [wtsession.r_session.post(murl, json=query_args).json()
                        for query_args in query_args_list]
            
            # Process the results into a dictionary
            gtopics = {x['prediction']['article'].split('wikipedia.org/wiki/')[1].replace('_', ' '):
                    {y['topic']: y['score'] for y in x['prediction']['results']} for x in gtopics
                    if 'prediction' in x}
            topics.update(gtopics)

            # Check for any failed queries
            titles = [x for x in titles if x not in gtopics.keys()]
            if titles:
                time.sleep(backoff_time)
                backoff_time *= backoff_factor
                remaining_retries -= 1
        if titles:
            topics.update({x: {k: None for k in ccols} for x in titles})

    else:
        # Other models require revids, not titles
        if titles:
            raise ValueError(f"{model} requires revids")
        
        # Prepare the query arguments for each revision ID
        query_args_list = [{"rev_id": x, **tf_args} for x in revids]
        
        # Perform the synchronous query to get topics        
        topics = [session.post(murl, json=query_args).json()
                    for query_args in query_args_list]

        # Process the results into a dictionary
        topics = {int(list(x[model.split('-')[0]]['scores'].keys())[0]):
                  x[model.split('-')[0]]['scores'][list(x[model.split('-')[0]]['scores'].keys())[0]]
                  [model.split('-')[1]]['score']['probability']
                  for x in topics}
    return topics

async def get_articles_topics(wtsession, titles=None, revids=None, pagemaps=None, lang='en', model='outlink-topic-model', tf_args={},
                                max_retries=5, backoff_time=1, backoff_factor=2, async_args={}):
    """Get topic scores for articles using the lift wing API.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        titles (list, optional): List of titles to collect data for. Defaults to None.
        revids (list, optional): List of revision IDs to collect data for. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        lang (str, optional): language code. Defaults to 'en'.
        model (str, optional): The model to use for generating topics. Defaults to 'outlink-topic-model'.
        tf_args (dict, optional): Arguments for the topics function. Defaults to {}.
        max_retries (int, optional): Maximum number of retries for the async query functions. Defaults to 5.
        backoff_time (int, optional): Time to wait between retries. Defaults to 1.
        backoff_factor (int, optional): Factor to increase backoff time. Defaults to 2.
        async_args (dict, optional): Arguments for the async query functions. Defaults to {}.

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
        if 'threshold' in tf_args:
            tf_args['threshold'] = float(tf_args['threshold']) # Ensure that the threshold is a float - weird bug at 0 otherwise
        
        if 'Main Page' in titles:
            titles.remove('Main Page')
            topics = {'Main Page': [None]*64}
        else:
            topics = {}

        remaining_retries = max_retries
        while (remaining_retries > 0) & (len(titles) > 0):
            query_args_list = [{"page_title": x, "lang": lang, **tf_args} for x in titles]
            
            # Perform the asynchronous query to get topics
            gtopics = await iterate_async_query(wtsession.lw_session, query_args_list, httpmethod='POST',
                                            posturl=f'/service/lw/inference/v1/models/{model}:predict',
                                            **async_args)
            # Process the results into a dictionary
            gtopics = {x['prediction']['article'].split('wikipedia.org/wiki/')[1].replace('_', ' '):
                    {y['topic']: y['score'] for y in x['prediction']['results']} for x in gtopics
                    if 'prediction' in x}
            topics.update(gtopics)

            # Check for any failed queries
            titles = [x for x in titles if x not in gtopics.keys()]
            if titles:
                await asyncio.sleep(backoff_time)
                backoff_time *= backoff_factor
                remaining_retries -= 1
        if titles:
            topics.update({x: {k: None for k in ccols} for x in titles})

    else:
        # Other models require revids, not titles
        if titles:
            raise ValueError(f"{model} requires revids")
        
        # Prepare the query arguments for each revision ID
        query_args_list = [{"rev_id": x, **tf_args} for x in revids]
        
        # Perform the asynchronous query to get topics
        topics = await iterate_async_query(wtsession.lw_session, query_args_list, httpmethod='POST',
                                           posturl=f'/service/lw/inference/v1/models/{model}:predict',
                                           **async_args)
        
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