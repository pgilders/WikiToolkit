from .api import *
from .redirects import *
from .tools import *
import mwapi
import datetime
import pandas as pd
import os


async def get_clickstreams(wtsession, mode='both', linktypes='link', titles=None, pageids=None, pagemaps=None,
                           date=None, datapath='data/',
                           daterange=None, consolidate_redirects=True):
    """Get parse_clickstreams to/from a list of articles from the API. Runs asynchronously.

    Args:
        wtsession (wikitoolkit.WTSession): The wikitoolkit session manager.
        mode (str / list, optional): The kind of parse_clickstreams to get. Defaults to 'both'.
        titles (list, optional): The article titles to collect clickstreams for. Must specify exactly one of titles or pageids. Defaults to None.
        pageids (list, optional): The article IDs to collect clickstreams for. Must specify exactly one of titles or pageids. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        namespaces (list, optional): The wiki namespaces to collect clickstreams from. Defaults to [0].
        update_maps (bool, optional): Whether to update maps on link collection. Defaults to False.
        batchsize (int, optional): How many pages to collect clickstreams for at a time - for rate limiting purposes. Defaults to 200.

    Raises:
        ValueError: If titles and pageids are not specified or if both are specified.

    Returns:
        dict: A dictionary of clickstreams (format depends on mode(s)).
    """

    # Check if titles or pageids are specified
    if not (bool(titles) ^ bool(pageids)):
        raise ValueError('Must specify exactly one of titles or pageids')
    if not bool(date) ^ bool(daterange):
        raise ValueError('Must specify exactly one of date or daterange')
    if pagemaps is None:
        print('Warning: No PageMaps object provided, this is not recommended practice') # TODO: make this a proper warning
        pagemaps = PageMaps()
    
    # get titles
    if pageids:
        reverse_id_map = {v: k for k, v in pagemaps.id_map.items()}
        titles = [reverse_id_map[pid] for pid in pageids]

    # get redirects
    if consolidate_redirects:
        await pagemaps.get_redirects(wtsession, titles)
    else:
        await pagemaps.fix_redirects(wtsession, titles)
    titles = [pagemaps.norm_map.get(t, t) for t in titles]
    titles = [pagemaps.titles_redirect_map.get(t, t) if pagemaps.titles_redirect_map.get(t, t) else t
                for t in titles]
    if consolidate_redirects:
        titles_rds = {t: pagemaps.collected_title_redirects.get(t, [t]) for t in titles}
        titles_rds_u = {k.replace(' ', '_'): [x.replace(' ', '_') for x in v]
                    for k, v in titles_rds.items()}
    else:
        titles_rds_u = {t.replace(' ', '_'): [t.replace(' ', '_')] for t in titles}
    titles_rds_u_rev = {x: k for k, v in titles_rds_u.items() for x in v}
    titles_u = list({t.replace(' ', '_') for t in titles_rds_u_rev.values()})

    # get month range
    lang = wtsession.mw_session.api_url.split('.')[0].split('//')[1]
    if daterange:
        start_date = daterange[0][:8]+'01'
        end_date = daterange[1][:8]+'01'
        monthlist = pd.date_range(start_date, end_date, freq='MS').strftime('%Y-%m').tolist()
        monthlist = [m[:7] for m in monthlist]
    else:
        monthlist = [date[:7]]

    final_clickstream_df = pd.DataFrame(columns=['prev', 'curr', 'type'])
    # download clickstream data
    for ym in monthlist:
        urlpath = f'https://dumps.wikimedia.org/other/clickstream/{ym}/clickstream-{lang}wiki-{ym}.tsv.gz'
        if not os.path.exists(datapath):
            os.makedirs(datapath)
        if not os.path.exists(f'{datapath}clickstream-{lang}wiki-{ym}.tsv.gz'):
            print(f'Downloading clickstream data for {ym}...')
            download_large_file(urlpath, f'{datapath}clickstream-{lang}wiki-{ym}.tsv.gz')

        # read the clickstream data with pandas
        clickstream_df = pd.read_csv(f'{datapath}clickstream-{lang}wiki-{ym}.tsv.gz',
                                    sep='\t', header=None, compression='gzip', quoting=3,
                                    names=['prev', 'curr', 'type', 'n_'+ym])

        # filter the clickstream data for the specified link types
        if linktypes != 'all':
            if type(linktypes) == str:
                linktypes = [linktypes]
            clickstream_df = clickstream_df[clickstream_df['type'].isin(linktypes)]
            
        # apply the redirect maps
        clickstream_df['prev'] = clickstream_df['prev'].apply(lambda x: titles_rds_u_rev.get(x, x))
        clickstream_df['curr'] = clickstream_df['curr'].apply(lambda x: titles_rds_u_rev.get(x, x))
        
        # sum
        clickstream_df = clickstream_df.fillna(0).groupby(['prev', 'curr', 'type']).sum().reset_index()
        clickstream_df = clickstream_df.replace(0, pd.NA)
        # filter the clickstream data for the specified titles
        if mode == 'both':
            clickstream_df = clickstream_df[clickstream_df['prev'].isin(titles_u) |
                                            clickstream_df['curr'].isin(titles_u)]
        elif mode == 'in':
            clickstream_df = clickstream_df[clickstream_df['curr'].isin(titles_u)]
        elif mode == 'out':
            clickstream_df = clickstream_df[clickstream_df['prev'].isin(titles_u)]
        else:
            raise ValueError('Invalid mode specified')
        
        final_clickstream_df = final_clickstream_df.merge(clickstream_df, how='outer',
                                                        on=['prev', 'curr', 'type'])
    
    #  get redirects of alts
    if consolidate_redirects:
        allarts_u = list(set(final_clickstream_df['prev'].tolist() + final_clickstream_df['curr'].tolist()))
        allarts = [x.replace('_', ' ') for x in allarts_u]
        await pagemaps.get_redirects(wtsession, allarts)
        all_titles = [pagemaps.norm_map.get(t, t) for t in allarts]
        all_titles = [pagemaps.titles_redirect_map.get(t, t) if pagemaps.titles_redirect_map.get(t, t) else t
                       for t in all_titles]
        all_titles_rds = {t: pagemaps.collected_title_redirects.get(t, [t]) for t in all_titles}
        all_titles_rds_u = {k.replace(' ', '_'): [x.replace(' ', '_') for x in v]
                    for k, v in all_titles_rds.items()}
        all_titles_rds_u_rev = {x: k for k, v in all_titles_rds_u.items() for x in v}

        # apply the redirect maps
        final_clickstream_df['prev'] = final_clickstream_df['prev'].apply(lambda x: all_titles_rds_u_rev.get(x, x))
        final_clickstream_df['curr'] = final_clickstream_df['curr'].apply(lambda x: all_titles_rds_u_rev.get(x, x))

        # sum
        final_clickstream_df = final_clickstream_df.fillna(0).groupby(['prev', 'curr', 'type']).sum().reset_index()
        final_clickstream_df = final_clickstream_df.replace(0, pd.NA)

    final_clickstream_df['prev'] = final_clickstream_df['prev'].str.replace('_', ' ')
    final_clickstream_df['curr'] = final_clickstream_df['curr'].str.replace('_', ' ')

    return final_clickstream_df.reset_index(drop=True).to_dict(orient='records')


async def pipeline_get_clickstreams(project, user_agent, titles=None, pageids=None, pagemaps=None,
                             gc_args={'date':datetime.datetime.today().isoformat()},
                             asynchronous=True, session_args={'formatversion':2}):
    """Runs full pipeline for getting clickstreams from the API - creating a session, collecting redirects, collecting clickstreams. Runs asynchronously.

    Args:
        project (str): The Wikimedia project to query.
        user_agent (str): The user agent string to use.
        titles (list, optional): The article titles to collect clickstreams for. Must specify exactly one of titles or pageids. Defaults to None.
        pageids (list, optional): The article IDs to collect clickstreams for. Must specify exactly one of titles or pageids. Defaults to None.
        pagemaps (wikitools.PageMap, optional): PageMap object to track redirects. Defaults to None.
        gc_args (dict, optional): Arguments to supply the get_clickstreams function. Defaults to {'date':datetime.datetime.today().isoformat()}.
        asynchronous (bool, optional): Whether to collect asynchronously. Defaults to True.
        session_args (dict, optional): Arguments for the mwapi session. Defaults to {'formatversion':2}.

    Raises:
        ValueError: If asynchronous is False (unsupported).

    Returns:
        dict: A list of clickstream data, and optionally the pagemaps object.
    """

    # Create a PageMaps object if not provided
    if pagemaps is None:
        return_pm = True
        pagemaps = PageMaps()
    else:
        return_pm = False

    # Construct the URL based on the project
    url = f'https://{project}.org'

    # Create an async session if asynchronous is True
    if asynchronous:
        wtsession = WTSession(project, user_agent, mw_session_args=session_args)
    else:
        raise ValueError('Only async supported at present.')
        wtsession = mwapi.Session(url, user_agent=user_agent, **session_args)

    # Perform necessary operations if asynchronous is True
    if asynchronous:
        # Fix redirects if titles are provided
        if titles:
            await pagemaps.fix_redirects(wtsession, titles=titles)
        
        # Get clickstreams using the async session
        clickstreams = await get_clickstreams(wtsession, titles=titles, pageids=pageids, pagemaps=pagemaps, **gc_args)
        
        # Close the async session
        await wtsession.close()
    else:
        raise ValueError('Only async supported at present.')
    
    if return_pm:
        return clickstreams, pagemaps
    else:
        return clickstreams