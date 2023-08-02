import requests
import time
import io
import gzip
import os
import datetime
import glob
import pandas as pd
from scipy import stats
from wikitools.tools import round_sig
from wikitools.api import fix_redirects

#%% Downloading and reading tables

def download_table(f_url, filepath, headers={}, ret=True):
    """_summary_

    Args:
        f_url (str): Url for download.
        filepath (str): Filepath to save to. Does not save if None.
        headers (dict, optional): HTTP request headers. Defaults to {}.
        ret (bool, optional): Whether to return compressed file. Defaults to True.

    Returns:
        : Compressed downloaded file.
    """
    print('Downloading data')
    status = 0
    nn = 0
    while status != 200:
        response = requests.get(f_url, headers=headers)
        status = response.status_code
        if status == 404:
            print('Link doesn\'t exist')
            return None
        elif status != 200:
            nn += 1
            if nn >= 5:
                print('Max attempts exceeded, breaking')
                return None
                # raise
            print(response.status_code)
            time.sleep(10)

    compressed_file = io.BytesIO(response.content)
    if filepath:
        with open(filepath,  'wb') as f:
            f.write(response.content)
    if ret:
        return compressed_file


def read_zip(filepath):
    """Reads (doesn't unzip) a gzip file.

    Args:
        filepath (str): Filepath to read.

    Returns:
        _type_: Compressed file.
    """
    with open(filepath,  'rb') as f:
        compressed_file = io.BytesIO(f.read())
    return compressed_file


def import_table(compressed_file, columns):
    """Unzips and imports a table.

    Args:
        compressed_file (_type_): Compressed file.
        columns (list): Column names.

    Returns:
        DataFrame : Pandas DataFrame with desired columns.
    """

    # print('Reading df')
    if not compressed_file:
        return pd.DataFrame(columns=columns)
    txt = gzip.GzipFile(fileobj=compressed_file)

    try:
        df = pd.read_table(txt, names=columns, quoting=3,
                           keep_default_na=False, delim_whitespace=True)
    except pd.errors.ParserError:
        df = pd.read_table(txt, names=columns, quoting=3,
                           keep_default_na=False, delim_whitespace=True,
                           engine='python')

    # print('df read')
    return df


def download_read_table(f_url, filepath, columns, headers={}):
    """Downloads, unzips, saves, and imports a table.

    Args:
        f_url (str): URL for download.
        filepath (str): Filepath to save to. Does not save if None.
        columns (list): Column names.
        headers (dict, optional): HTTP request headers. Defaults to {}.

    Returns:
        DataFrame : Pandas DataFrame of table from URL.
    """


    if not os.path.exists(filepath):
        print('downloading')
        compressed_file = download_table(f_url, filepath, headers)
    else:
        try:
            compressed_file = read_zip(filepath)
        except OSError:
            compressed_file = download_table(f_url, filepath, headers)

    try:
        df = import_table(compressed_file, columns)
    except EOFError:
        print('Error reading zip file, redownloading from %s' % f_url)
        compressed_file = download_table(f_url, filepath, headers)
        df = import_table(compressed_file, columns)
    return df

#%% Clickstream

def download_clickstream_month(lang, month, data_path, headers):
    """Download clickstream data for a given month.

    Args:
        lang (str): Language code (e.g. 'en').
        month (str): Month to download.
        data_path (str): Folder to save data to.
        headers (dict): HTTP headers to use.
    """
    url = 'https://dumps.wikimedia.org/other/clickstream/'
    fn = 'clickstream-%swiki-%s.tsv.gz' % (lang, month)
    f_url = url + '%s/%s' % (month, fn)
    filepath = '%s/%s' % (data_path, fn)
    if not os.path.exists(filepath):
        download_table(f_url, filepath, headers, ret=False)


def download_clickstream_months(lang, months, data_path, headers):
    """Download clickstream data for a list of months.

    Args:
        lang (str): Language code (e.g. 'en').
        months (list): Month to download.
        data_path (str): Folder to save data to.
        headers (dict): HTTP headers to use.
    """

    months_got = glob.glob('%s/clickstream-%swiki*.tsv.gz' %(data_path, lang))
    months = sorted(set(months) - set(months_got))
    
    # pv_space_estimate(hours, data_path)

    t0 = time.time()
    for n, month in enumerate(months):
        tr = time.time()
        download_clickstream_month(lang, month, data_path, headers)
        av = (time.time() - t0)/(n+1)
        print('Progress=%.2f%%, Elapsed=%.2fh, Last=%.1fs, Average=%.1fs, Remaining~%.2fh'
              % (100*n/len(months), (time.time()-t0)/3600, time.time()-tr, av,
                 av*(len(months) - (n+1))/3600))

def get_clickstream_month(lang, month, data_path, headers={}):
    """Downloads (if necessary) and reads clickstream data for a given month.

    Args:
        lang (str): Language code (e.g. 'en').
        month (str): Month to download/read.
        data_path (str): Folder to read data from / save data to.
        headers (dict): HTTP headers to use.

    Returns:
        _type_: Dataframe of pageviews for requested month.
    """
    url = 'https://dumps.wikimedia.org/other/clickstream/'
    fn = 'clickstream-%swiki-%s.tsv.gz' % (lang, month)
    f_url = url + '%s/%s' % (month, fn)
    filepath = '%s/%s' % (data_path, fn)
    if data_path:
        filepath = '%s/%s' % (data_path, fn)
    else:
        filepath = None
    cols = ['source', 'target', 'type', 'n_%s' % month]
    df = download_read_table(f_url, filepath, cols, None, headers)
    return df

def get_clickstreams(langcsartdict, redirects, data_path, csdf_path,
                     cs_df_dict={}, fix_rd=True, agg_rd=True, dl_first=True,
                     mode='OR', **kwargs):
    """Gets clickstream data for a selection of languages/articles/months.

    Args:
        langcsartdict (dict): Language, month, and articles to collect clickstream data for.
        redirects (dict): Redirects to fix.
        data_path (str): Folder to save data to / read data from.
        csdf_path (str): Filepath to save clickstream data to.
        cs_df_dict (dict, optional): An existing dict of clickstream dataframes
        by language. Defaults to {}.
        fix_rd (bool, optional): Whether to fix redirects. Defaults to True.
        agg_rd (bool, optional): Whether to aggregate redirects. Defaults to True.
        dl_first (bool, optional): Whether to download data first. Defaults to True.
        mode (str, optional): 'OR' mode gets all links to/from target articles.
        'AND' mode only gets links between target articles. Defaults to 'OR'.

    Returns:
        dict: Dictionary of clickstream dataframes by language.
    """    

    url = 'https://dumps.wikimedia.org/other/clickstream/'
    if csdf_path:
        pd.Series({'save_cs': bool(data_path), 'save_df': bool(csdf_path),
                   'fix_rd': fix_rd, 'agg_rd': agg_rd, mode: 'OR'}
                   ).to_hdf(csdf_path, key='df_info')
    if dl_first:
        for lang, artdict in langcsartdict.items():
            if lang not in ['de', 'en', 'es', 'fa', 'fr', 'it', 'ja', 'pl',
                            'pt', 'ru', 'zh']:
                print('No clickstream data for %s' % lang)
                continue
            download_clickstream_months(lang, artdict.keys(), data_path,
                                        **kwargs)
        print('All files downloaded')

    l_redirects = {}
    for lang, artdict in langcsartdict.items():
        if lang not in ['de', 'en', 'es', 'fa', 'fr', 'it', 'ja', 'pl', 'pt',
                        'ru', 'zh']:
            print('No clickstream data for %s' % lang)
            continue
        redirects_r = {x: k for k, v in redirects[lang].items() for x in v}
        l_redirects[lang] = redirects_r.copy()
        for month, arts in artdict.items():
            print(lang, month)
            df = get_clickstream_month(lang, month, data_path, **kwargs)

            df['n_%s' % month] = df['n_%s' % month].astype(float)

            # fix redirects
            if fix_rd:
                df['source'] = df['source'].apply(lambda x:
                                                  redirects_r.get(x, x))
                df['target'] = df['target'].apply(lambda x:
                                                  redirects_r.get(x, x))
                df = df.groupby(['source', 'target',
                                 'type']).sum().reset_index()

            if mode == 'OR':
                df = df[(df['source'].isin(arts)) | (df['target'].isin(arts))]
            elif mode == 'AND':
                df = df[(df['source'].isin(arts)) & (df['target'].isin(arts))]
            else:
                raise

            # get all redirects
            if agg_rd:
                articles = set(df['source']) | set(df['target'])
                l_redirects[lang] = fix_redirects(articles, l_redirects[lang],
                                                  lang, norm_keys=False)

                df['source'] = df['source'].apply(lambda x:
                                                  l_redirects[lang].get(x, x))
                df['target'] = df['target'].apply(lambda x:
                                                  l_redirects[lang].get(x, x))
                # sum redirects

                df = df.groupby(['source', 'target',
                                 'type']).sum().reset_index()

            if lang not in cs_df_dict:
                cs_df_dict[lang] = pd.DataFrame(
                    index=df.set_index(list(df.columns[:3])).index)

            cs_df_dict[lang] = pd.concat([cs_df_dict[lang],
                                          df.set_index(list(df.columns[:3])
                                                       )['n_%s' % month]],
                                         axis=1)

            if csdf_path:
                cs_df_dict[lang].reset_index().to_hdf(csdf_path, key=lang)

    return {k: v.reset_index() for k, v in cs_df_dict.items()}


#%% Page views

def pv_space_estimate(hours, data_path):
    """Estimate space required for pageview data.

    Args:
        hours (list): List of hours to download.
        data_path (str): Folder to save data to.
    """    
    hours = {data_path+'/pageviews-%s.gz' % x.strftime('%Y%m%d-%H0000')
             for x in hours}
    ex_hours = set(glob.glob(data_path+'/*'))
    new_hours = hours-ex_hours
    size = round_sig(len(new_hours)*50)//1000
    print(len(hours), len(ex_hours), len(new_hours))
    if size > 10:
        input("""Warning, page view data will take around an additional %dGB.
              Enter any key to proceed""" % size)
        

def download_pageview_hour(hour, data_path, headers):
    """Download pageview data for a given hour.

    Args:
        hour (_type_): Hour to download.
        data_path (str): Folder to save data to.
        headers (dict): HTTP headers to use.
    """
    if hour > pd.Timestamp.now(tz='UTC'):
        print('Hour in future')
        return
    url = 'https://dumps.wikimedia.org/other/pageviews/'
    fn = 'pageviews-%s.gz' % hour.strftime('%Y%m%d-%H0000')
    f_url = url + '%d/%d-%02d/%s' % (hour.year, hour.year, hour.month, fn)
    filepath = '%s/%s' % (data_path, fn)
    if not os.path.exists(filepath):
        download_table(f_url, filepath, headers, ret=False)


def download_pageview_hours(hours, data_path, headers):
    """Download pageview data for a list of hours.

    Args:
        hours (list): Hours to download.
        data_path (str): Folder to save data to.
        headers (dict): HTTP headers to use.
    """

    hours_got = glob.glob('%s/pageviews-*.gz' % data_path)
    hours_got = {pd.to_datetime(x, format=data_path +
                                '/pageviews-%Y%m%d-%H0000.gz', utc=True)
                 for x in hours_got}

    hours = sorted([x for x in set(hours) - set(hours_got)
                    if x < pd.Timestamp.now(tz='UTC')])
    
    pv_space_estimate(hours, data_path)

    url = 'https://dumps.wikimedia.org/other/pageviews/'
    t0 = time.time()
    for n, hour in enumerate(hours):
        tr = time.time()
        download_pageview_hour(hour, data_path, headers)
        av = (time.time() - t0)/(n+1)
        print('Progress=%.2f%%, Elapsed=%.2fh, Last=%.1fs, Average=%.1fs, Remaining~%.2fh'
              % (100*n/len(hours), (time.time()-t0)/3600, time.time()-tr, av,
                 av*(len(hours) - (n+1))/3600))

def download_pageview_range(start, stop, data_path, headers={}):
    """Downloads pageview data for a given range of hours.

    Args:
        start (_type_): Start hour.
        stop (_type_): End hour.
        data_path (str): Folder to save data to.
        headers (dict, optional): HTTP headers to use.. Defaults to {}.
    """
    dr = pd.date_range(pd.to_datetime(start).ceil('H'),
                       pd.to_datetime(stop).ceil('H'), freq='H')
    download_pageview_hours(dr, data_path, headers)


def get_pageview_hour(hour, data_path, headers={}):
    """Downloads (if necessary) and reads pageview data for a given hour.

    Args:
        hour (_type_): Hour to get.
        data_path (str): Folder to save data to.
        headers (dict, optional): HTTP headers to use. Defaults to {}.

    Returns:
        _type_: Dataframe of pageviews for requested hour.
    """
    if hour > pd.Timestamp.now(tz='UTC'):
        print('Hour in future')
        return pd.DataFrame(columns=['domain', 'article', 'views', 'response'])
    url = 'https://dumps.wikimedia.org/other/pageviews/'
    fn = 'pageviews-%s.gz' % hour.strftime('%Y%m%d-%H0000')
    f_url = url + '%d/%d-%02d/%s' % (hour.year, hour.year, hour.month, fn)
    if data_path:
        filepath = '%s/%s' % (data_path, fn)
    else:
        filepath = None
    cols = ['domain', 'article', 'views', 'response']
    df = download_read_table(f_url, filepath, cols, None, headers)
    return df


def get_datelangartdict(langartdict, redirects, days=False):
    """Converts a dictionary of language-article-date ranges to a dictionary of
    date-language-article ranges.

    Args:
        langartdict (dict): Dictionary of language-article-date ranges.
        redirects (dict): Dictionary of redirects.
        days (bool, optional): Whether to get full days at the end of range.
        Defaults to False.

    Returns:
        dict: Dictionary of date-language-article ranges.
    """    
    alldatetimes = {}
    for lang, artdict in langartdict.items():
        for art, daterange in artdict.items():
            dr = langartdict[lang][art]
            if days:
                dr = (dr[0].floor('D'),
                      dr[1].ceil('D') - datetime.timedelta(hours=1))
            for d in pd.date_range(*dr, freq='H'):
                if d not in alldatetimes:
                    alldatetimes[d] = {}
                if lang not in alldatetimes[d]:
                    alldatetimes[d][lang] = {}
                alldatetimes[d][lang][art] = redirects[lang][art]

    return {k: alldatetimes[k] for k in sorted(alldatetimes.keys())}


def check_rows(hour, existing_df, langdict={}, langs=[]):
    """Checks whether an entry for a given language/article/hour is in an existing dataframe.

    Args:
        hour (_type_): Hour to check.
        existing_df (_type_): Existinn dataframe to check in.
        langdict (dict, optional): Language/article combos to check. Defaults to {}.
        langs (list, optional): Languages to check. Defaults to [].

    Returns:
        dict/list: A filtererd version of langdict/langs, containing only those
        languages/articles that are not in the existing dataframe.
    """    

    assert bool(langdict) != bool(langs)

    try:
        hdf = existing_df.loc[[hour]]
    except KeyError:
        if langdict:
            return langdict
        else:
            return langs

    if langdict:
        out = {}
        for lang, arts in langdict.items():
            hdfl = hdf[(hdf['domain'] == lang) | (hdf['domain'] == lang+'.m')]
            f_arts = {art: rds for art, rds in arts.items()
                      if art not in hdfl['article'].values}
            if f_arts:
                out[lang] = f_arts
    else:
        out = sorted(set(langs) - {x.replace('.m', '')
                                   for x in hdf.T.dropna().index})

    return out

def filter_pv_df(lang, arts, df, hour, pop_zero=True, agg_rd=True,
                 agg_dm=True, percentile=False):
    """Filters pageview dataframe to requested languages and articles.

    Args:
        lang (str): Language to filter to.
        arts (str): Articles to filter to.
        df (_type_): DataFrame with page view data.
        hour (_type_): Hour of the DataFrame.
        pop_zero (bool, optional): Whether to fill any empty page view hours
        with 0. Defaults to True.
        agg_rd (bool, optional): Whether to aggregate redirects. Defaults to
        True.
        agg_dm (bool, optional): Whether to aggregate desktop and mobile views.
        Defaults to True.
        percentile (bool, optional): Whether to additionally calculate pageviews
         as percentile of total. Defaults to False.

    Returns:
        _type_: DataFrame of filtered page views for language and articles.
    """    

    langdf = df[(df['domain'] == lang) | (df['domain'] == lang+'.m')]

    if arts == 'all':
        qviews = langdf[['domain', 'article', 'views']]
        arts = {}
    else:
        rdarticles = set([y for x in arts.values() for y in x])
        qviews = langdf[langdf['article'].isin(rdarticles)][['domain',
                                                             'article',
                                                             'views']]
    if pop_zero:
        zero_arts = list(set(arts.keys())-set(qviews['article']))
        lza = len(zero_arts)
        zero_df = pd.DataFrame({'domain': [lang]*lza+[lang+'.m']*lza,
                                'article': zero_arts*2,
                                'views': [0]*lza*2})
        qviews = pd.concat([qviews, zero_df])

    if agg_rd:
        rd_rev = {x: k for k, v in arts.items() for x in v}
        qviews['article'] = qviews['article'].map(rd_rev)

    if arts == {}:
        qviews['article'] = 'all'

    if agg_dm:
        qviews = qviews.groupby('article').sum().reset_index()
        qviews['domain'] = lang
        qviews = qviews[['domain', 'article', 'views']]
    else:
        qviews = qviews.groupby(['domain',
                                 'article']).sum().reset_index()

    if arts == {}:
        qviews = qviews[['domain', 'views']]

    qviews.index = [hour]*len(qviews)

    if percentile == 'both':
        qviews_p = qviews.copy()
        qviews_p['views'] = qviews_p['views'].apply(lambda x:
                                                    stats.percentileofscore(
                                                        langdf['views'], x, kind='weak'))
        return qviews, qviews_p

    elif percentile:
        qviews['views'] = qviews['views'].apply(lambda x:
                                                stats.percentileofscore(
                                                    langdf['views'], x, kind='weak'))
        return qviews

    else:
        return qviews


def get_hourly_pageviews(langartdict, redirects, data_path, pvdf_path, 
                         agg_rd=True, agg_dm=True, dl_first=True,
                         pop_zero=True,
                         **kwargs):
    """Get the hourly pageview for a selection of languages/articles/hours.

    Args:
        langartdict (dict): Dictionary with languages, articles, and hours to get pageviews for.
        redirects (dict): Redirects to use.
        data_path (str): Folder with all hourly pageviews to read from / save to.
        pvdf_path (str): Filepath to save DataFrame to. If None, DataFrame is not saved.
        agg_rd (bool, optional): Whether to aggregate redirects. Defaults to True.
        agg_dm (bool, optional): Whether to aggregate desktop and mobile views. Defaults to True.
        dl_first (bool, optional): Whether to download all hours first. Defaults to True.
        pop_zero (bool, optional): Whether to fill any empty page view hours with 0. Defaults to True.

    Returns:
        _type_: DataFrame of pageviews for specified languages/articles/hours.
    """

    if os.path.exists(pvdf_path):
        existing_df = pd.read_hdf(pvdf_path, key='df')
    else:
        existing_df = pd.DataFrame()

    # reshape input
    datelangartdict = get_datelangartdict(langartdict, redirects)

    if pvdf_path:
        pd.Series({'save_df': bool(data_path), 'save_hpv': bool(pvdf_path),
                   'agg_rd': agg_rd, 'agg_dm': agg_dm}
                   ).to_hdf(pvdf_path, key='df_info')
    # t0 = time.time()
    # print(t0)
    if dl_first:
        download_pageview_hours(datelangartdict.keys(), data_path, **kwargs)
        print('All files downloaded')

    for n, (hour, langdict) in enumerate(datelangartdict.items()):
        # print(time.time()-t0)
        print('Page views progress', round(100*n/len(datelangartdict), 4), '%')
        # filter down langdict
        flangdict = check_rows(hour, existing_df, langdict=langdict)

        if not flangdict:
            continue
        df = get_pageview_hour(hour, data_path, **kwargs)

        # get views for article(s) (desktop+mobile)
        print('Filtering df')
        for lang, arts in flangdict.items():
            qviews = filter_pv_df(lang, arts, df, hour, pop_zero, agg_rd,
                                  agg_dm)
            existing_df = pd.concat([existing_df, qviews])
            if existing_df['domain'].isnull().sum():
                raise

        existing_df = existing_df.sort_index()
        # save data
        if pvdf_path:
            os.rename(pvdf_path, pvdf_path.replace('.h5', '_old.h5'))
            existing_df.to_hdf(pvdf_path, key='df')

    return existing_df


def get_hourly_pageview_totals(daterange, langs, data_path, pvtdf_path,
                               agg_dm=True, dl_first=True, **kwargs):
    """Get the hourly pageview totals across all articles for a selection of languages/hours.

    Args:
        daterange (list): Range of dates to get pageviews for.
        langs (list): Languages to get pageviews for.
        data_path (str): Folder with all hourly pageviews to read from / save to.
        pvtdf_path (str): Filepath to save DataFrame to. If None, DataFrame is not saved.
        agg_dm (bool, optional): Whether to aggregate desktop and mobile views. Defaults to True.
        dl_first (bool, optional): Whether to download all hours first. Defaults to True.

    Returns:
        _type_: _description_
    """

    if os.path.exists(pvtdf_path):
        existing_df = pd.read_hdf(pvtdf_path, key='df')
    else:
        existing_df = pd.DataFrame()

    # reshape input

    daterange = sorted(daterange)

    if data_path:
        pd.Series({'save_df': bool(data_path), 'save_hpv': bool(pvtdf_path),
                   'agg_dm': agg_dm}).to_hdf(pvtdf_path, key='df_info')
    # t0 = time.time()
    # print(t0)
    if dl_first:
        download_pageview_hours(daterange, data_path, **kwargs)
        print('All files downloaded')

    for n, hour in enumerate(daterange):
        # print(time.time()-t0)
        print('Page views progress', round(100*n/len(daterange), 4), '%')
        # download and/or unzip data
        # filter down langdict

        flangs = check_rows(hour, existing_df, langs=langs)
        if not flangs:
            continue

        df = get_pageview_hour(hour, data_path, **kwargs)

        # get views for article(s) and total lang views (desktop+mobile)
        print('Filtering df')
        edf = pd.DataFrame()
        for lang in flangs:
            qviews = filter_pv_df(lang, 'all', df, hour, pop_zero=False,
                                  agg_rd=False, agg_dm=agg_dm)
            qviews = pd.pivot(qviews, columns='domain', values='views')
            edf = pd.concat([edf, qviews], axis=1)

        existing_df.loc[hour, edf.columns] = edf.loc[hour]
        existing_df = existing_df.sort_index()
        # save data
        if pvtdf_path:
            if os.path.exists(pvtdf_path):
                os.rename(pvtdf_path, pvtdf_path.replace('.h5', '_old.h5'))
            existing_df.to_hdf(pvtdf_path, key='df')

    if pvtdf_path:
        os.remove(pvtdf_path.replace('.h5', '_old.h5'))

    return existing_df


def get_hourly_pageview_percs_old(langartdict, redirects, data_path, pvpdf_path,
                              agg_rd=True, agg_dm=True, dl_first=True,
                              pop_zero=True, days=False, **kwargs):
    """Get the hourly pageview percentiles for a selection of languages/articles/hours.

    Args:
        langartdict (dict): Dictionary with languages, articles, and hours to get pageviews for.
        redirects (dict): Redirects to use.
        data_path (str): Folder with all hourly pageviews to read from / save to.
        pvpdf_path (str): Filepath to save DataFrame to. If None, DataFrame is not saved.
        agg_rd (bool, optional): Whether to aggregate redirects. Defaults to True.
        agg_dm (bool, optional): Whether to aggregate desktop and mobile views. Defaults to True.
        dl_first (bool, optional): Whether to download all hours first. Defaults to True.
        pop_zero (bool, optional): Whether to fill any empty page view hours with 0. Defaults to True.
        days (bool, optional): _description_. Defaults to False.

    Returns:
        _type_: _description_
    """

    if os.path.exists(pvpdf_path):
        existing_df = pd.read_hdf(pvpdf_path, key='df')
    else:
        existing_df = pd.DataFrame()

    # reshape input
    datelangartdict = get_datelangartdict(langartdict, redirects, days)

    if days:
        day_df = pd.Series(name='views',
                           index=pd.MultiIndex.from_tuples([],
                                                           names=["domain",
                                                                  "article"]))
        qviewsd = day_df.copy()
        last_day = min(datelangartdict.keys()).floor('D')

    if pvpdf_path:
        pd.Series({'save_df': bool(data_path), 'save_hpv': bool(pvpdf_path),
                   'agg_rd': agg_rd, 'agg_dm': agg_dm}
                   ).to_hdf(pvpdf_path, key='df_info')
    # t0 = time.time()
    # print(t0)
    if dl_first:
        download_pageview_hours(datelangartdict.keys(), data_path, **kwargs)
        print('All files downloaded')

    t0 = time.time()
    for n, (hour, langdict) in enumerate(datelangartdict.items()):
        tr = time.time()
        # print(time.time()-t0)
        print('Page views progress', round(100*n/len(datelangartdict), 4), '%')
        # download and/or unzip data
        # filter down langdict
        if days:
            flangdict = check_rows(hour.floor(
                'D'), existing_df, langdict=langdict)
        else:
            flangdict = check_rows(hour, existing_df, langdict=langdict)

        if not flangdict:
            continue
        df = get_pageview_hour(hour, data_path, **kwargs)

        if days:
            print('Summing all day data')
            day = hour.floor('D')
            if day != last_day:
                day_df = pd.Series(name='views',
                                   index=pd.MultiIndex.from_tuples([],
                                                                   names=["domain",
                                                                          "article"]))
                qviewsd = day_df.copy()
                last_day = day
            lang_keys = list(langdict.keys()) + [x+'.m' for x in
                                                 langdict.keys()]
            df = df[df['domain'].isin(lang_keys)]
            day_df = day_df.add(df.set_index(['domain', 'article'])[
                'views'], fill_value=0)
        # get views for article(s) and total lang views (desktop+mobile)
        print('Filtering df')
        for lang, arts in flangdict.items():
            qviews = filter_pv_df(lang, arts, df, hour, pop_zero, agg_rd,
                                  agg_dm, percentile=False)

            if days:
                qviewsd = qviewsd.add(qviews.set_index(['domain',
                                                        'article'])['views'],
                                      fill_value=0)

            else:
                existing_df = pd.concat([existing_df, qviews])
                if existing_df['domain'].isnull().sum():
                    raise

        if days:
            if hour.hour == 23:
                ddf = day_df.reset_index()
                langdf = ddf[(ddf['domain'] == lang) |
                             (ddf['domain'] == lang+'.m')]
                qd = qviewsd.apply(lambda x: stats.percentileofscore(  # group dm?
                    langdf['views'], x,
                    kind='weak')).reset_index()
                qd.index = [day]*len(qd)
                existing_df = pd.concat([existing_df, qd])

        existing_df = existing_df.sort_index()
        # save data
        if pvpdf_path:
            os.rename(pvpdf_path, pvpdf_path.replace('.h5', '_old.h5'))
            existing_df.to_hdf(pvpdf_path, key='df')

        print(n, time.time()-t0, time.time()-tr, (time.time()-t0)/(n+1))

    return existing_df


def get_hourly_pageview_percs(langartdict, redirects, data_path, pvpdf_path,
                               agg_rd=True, dl_first=True, pop_zero=True,
                               days=False, **kwargs):
    """Get the hourly pageview percentiles for a selection of languages/articles/hours.

    Args:
        langartdict (dict): Dictionary with languages, articles, and hours to get pageviews for.
        redirects (dict): Redirects to use.
        data_path (str): Folder with all hourly pageviews to read from / save to.
        pvpdf_path (str): Filepath to save DataFrame to. If None, DataFrame is not saved.
        agg_rd (bool, optional): Whether to aggregate redirects. Defaults to True.
        dl_first (bool, optional): Whether to download all hours first. Defaults to True.
        pop_zero (bool, optional): Whether to fill any empty page view hours with 0. Defaults to True.
        days (bool, optional): _description_. Defaults to False.

    Returns:
        _type_: _description_
    """
    if os.path.exists(pvpdf_path):
        existing_df = pd.read_hdf(pvpdf_path, key='df')
    else:
        existing_df = pd.DataFrame()

    # reshape input
    datelangartdict = get_datelangartdict(langartdict, redirects, days)

    if days:
        langs = list(langartdict.keys())
        day_df_d = {lang: pd.Series(name='views', index=pd.Index([], name="article"))
                    for lang in langs}

        qviewsd = pd.Series(name='views',
                            index=pd.MultiIndex.from_tuples([],
                                                            names=["domain",
                                                                   "article"]))
        last_day = min(datelangartdict.keys()).floor('D')

    if pvpdf_path:
        pd.Series({'save_df': bool(data_path), 'save_hpv': bool(pvpdf_path),
                   'agg_rd': agg_rd}).to_hdf(pvpdf_path, key='df_info')
    # t0 = time.time()
    # print(t0)
    if dl_first:
        download_pageview_hours(datelangartdict.keys(), data_path, **kwargs)
        print('All files downloaded')

    t0 = time.time()
    for n, (hour, langdict) in enumerate(datelangartdict.items()):
        tr = time.time()
        # print(time.time()-t0)
        print('Page views progress', round(100*n/len(datelangartdict), 4), '%')
        # download and/or unzip data
        # filter down langdict
        if days:
            flangdict = check_rows(hour.floor(
                'D'), existing_df, langdict=langdict)
        else:
            flangdict = check_rows(hour, existing_df, langdict=langdict)

        if not flangdict:
            continue
        df = get_pageview_hour(hour, data_path, **kwargs)

        if days:
            print('Summing all day data')
            day = hour.floor('D')
            if day != last_day:
                day_df_d = {lang: pd.Series(name='views', index=pd.Index([],name="article"))
                            for lang in langs}
                qviewsd = pd.Series(name='views',
                                    index=pd.MultiIndex.from_tuples([],
                                                                    names=["domain",
                                                                           "article"]))
                last_day = day
            lang_keys = list(langdict.keys()) + [x+'.m' for x in
                                                 langdict.keys()]
            dfd = {}
            for lang in langdict.keys():
                dfd[lang] = df[(df['domain'] == lang)].set_index('article')['views'].add(
                    df[(df['domain'] == lang + '.m')].set_index('article')['views'], fill_value=0)
                day_df_d[lang] = day_df_d[lang].add(dfd[lang], fill_value=0)
                dfd[lang] = pd.DataFrame(
                    {'domain': lang, 'views': dfd[lang]}).reset_index()

            df = pd.concat(dfd.values())
        # get views for article(s) and total lang views (desktop+mobile)
        print('Filtering df')
        for lang, arts in flangdict.items():
            qviews = filter_pv_df(lang, arts, df, hour, pop_zero, agg_rd,
                                  True, percentile=False)

            if days:
                qviewsd = qviewsd.add(qviews.set_index(['domain',
                                                        'article'])['views'],
                                      fill_value=0)

            else:
                existing_df = pd.concat([existing_df, qviews])
                if existing_df['domain'].isnull().sum():
                    raise

        if days:
            if hour.hour == 23:
                for lang in langs:
                    if lang not in qviewsd.index:
                        continue

                    qd = qviewsd.loc[lang].apply(lambda x: stats.percentileofscore(
                        day_df_d[lang], x,
                        kind='weak')).reset_index()
                    qd.index = [day]*len(qd)
                    qd['domain'] = lang
                    existing_df = pd.concat([existing_df, qd])

        existing_df = existing_df.sort_index()
        # save data
        if pvpdf_path:
            os.rename(pvpdf_path, pvpdf_path.replace('.h5', '_old.h5'))
            existing_df.to_hdf(pvpdf_path, key='df')

        print(n, time.time()-t0, time.time()-tr, (time.time()-t0)/(n+1))

    return existing_df


def get_all_pageview_data(langartdict, redirects, data_path, h_path, ht_path,
                          hp_path, dp_path, agg_rd=True, dl_first=True,
                          pop_zero=True, **kwargs):
    """Gets all pageview data for given language, article, and hours (hourly,
    hourly total, hourly percentiles, daily percentiles).

    Args:
        langartdict (dict): Dictionary with languages, articles, and hours to get pageviews for.
        redirects (dict): Redirects to use.
        data_path (str): Folder with all hourly pageviews to read from / save to.
        h_path (str): Filepath to hourly count df.
        ht_path (str): Filepath to hourly total count df.
        hp_path (str): Filepath to hourly percentiles df.
        dp_path (str): Filepath to daily percentiles df.
        agg_rd (bool, optional): Whether to aggregate redirects. Defaults to True.
        dl_first (bool, optional): Whether to download all hours first. Defaults to True.
        pop_zero (bool, optional): Whether to fill any empty page view hours with 0. Defaults to True.

    Returns:
        dict: Dictionary of DataFrames with hourly, hourly total, hourly
        percentiles, and daily percentiles for the specified languages, articles.
    """    

    df_paths = {'hourly': h_path, 'hourly_total': ht_path,
                'hourly_percentile': hp_path, 'daily_percentile': dp_path}
    existing_df_dict = {}
    for k, p in df_paths.items():
        if os.path.exists(p):
            existing_df_dict[k] = pd.read_hdf(p, key='df')
        else:
            existing_df_dict[k] = pd.DataFrame()

    # reshape input
    datelangartdict = get_datelangartdict(langartdict, redirects, days=True)

    for p in df_paths.values():
        pd.Series({'agg_rd': agg_rd, 'dl_first': dl_first, 'pop_zero': pop_zero}
                   ).to_hdf(p, key='df_info')

    if dl_first:
        download_pageview_hours(datelangartdict.keys(), data_path, **kwargs)
        print('All files downloaded')

    langs = list(langartdict.keys())
    day_df_d = {lang: pd.Series(name='views', index=pd.Index([],
                                                             name="article"))
                for lang in langs}

    qviewsd = pd.Series(name='views',
                        index=pd.MultiIndex.from_tuples([],
                                                        names=["domain",
                                                               "article"]))
    last_day = min(datelangartdict.keys()).floor('D')

    t0 = time.time()
    nn = 0
    for n, (hour, langdict) in enumerate(datelangartdict.items()):
        tr = time.time()
        # print(time.time()-t0)
        print('Page views progress', round(100*n/len(datelangartdict), 3), '%')
        flangdict = check_rows(
            hour, existing_df_dict['hourly'], langdict=langdict)

        if not flangdict:
            continue

        df = get_pageview_hour(hour, data_path, **kwargs)

        print('Summing all day data')
        day = hour.floor('D')
        if day != last_day:
            day_df_d = {lang: pd.Series(name='views', index=pd.Index([], name="article"))
                        for lang in langs}
            qviewsd = pd.Series(name='views',
                                index=pd.MultiIndex.from_tuples([],
                                                                names=["domain",
                                                                       "article"]))
            last_day = day

        dfd = {}
        for lang in langdict.keys():
            dfd[lang] = df[(df['domain'] == lang)].set_index('article')['views'].add(
                df[(df['domain'] == lang + '.m')].set_index('article')['views'], fill_value=0)
            day_df_d[lang] = day_df_d[lang].add(dfd[lang], fill_value=0)
            dfd[lang] = pd.DataFrame(
                {'domain': lang, 'views': dfd[lang]}).reset_index()

        df = pd.concat(dfd.values())

        print('Summing all articles hourly data')
        edf = pd.DataFrame()
        for lang in flangdict.keys():
            ht_views = filter_pv_df(lang, 'all', df, hour, pop_zero=False,
                                    agg_rd=False, agg_dm=True)
            ht_views = pd.pivot(ht_views, columns='domain', values='views')
            edf = pd.concat([edf, ht_views], axis=1)

        existing_df_dict['hourly_total'].loc[hour, edf.columns] = edf.loc[hour]

        # get views for article(s) and total lang views (desktop+mobile)
        print('Filtering df')
        for lang, arts in flangdict.items():
            qviews, qviews_p = filter_pv_df(lang, arts, df, hour, pop_zero, agg_rd,
                                            True, percentile='both')

            qviewsd = qviewsd.add(qviews.set_index(['domain',
                                                    'article'])['views'],
                                  fill_value=0)

            existing_df_dict['hourly'] = pd.concat([existing_df_dict['hourly'],
                                                    qviews])
            existing_df_dict['hourly_percentile'] = pd.concat([existing_df_dict['hourly_percentile'],
                                                               qviews])
            if existing_df_dict['hourly']['domain'].isnull().sum():
                raise

        if hour.hour == 23:
            for lang in langs:
                if lang not in qviewsd.index:
                    continue

                qd = qviewsd.loc[lang].apply(lambda x: stats.percentileofscore(
                    day_df_d[lang], x,
                    kind='weak')).reset_index()
                qd.index = [day]*len(qd)
                qd['domain'] = lang
                existing_df_dict['daily_percentile'] = pd.concat(
                    [existing_df_dict['daily_percentile'], qd])

            existing_df_dict = {k: v.sort_index()
                                for k, v in existing_df_dict.items()}
            # save data
            print('Saving dataframes')
            for k, v in existing_df_dict.items():
                os.rename(df_paths[k], df_paths[k].replace(
                    '.h5', '_old.h5'))
                v.to_hdf(df_paths[k], key='df')
        nn += 1
        av = (time.time()-t0)/(nn)
        print('%d: Total=%.2fh, Last=%.1fs, Average=%.1fs, Remaining~%.2fh'
              % (n, (time.time()-t0)/3600, time.time()-tr, av,
                 av*(len(datelangartdict)-n)/3600))

    return existing_df_dict
