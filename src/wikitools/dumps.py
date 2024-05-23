import requests
import time
import io
import gzip
import bz2
import os
import datetime
import glob
import pandas as pd
import polars as pl
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
    try:
        txt = gzip.GzipFile(fileobj=compressed_file)
    except:
        txt = bz2.BZ2File(compressed_file)

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
    months_got = [m.split('wiki-')[-1].split('.')[0] for m in months_got]
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
    df = download_read_table(f_url, filepath, cols, headers)
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
    t0 = time.time()
    for lang, artdict in langcsartdict.items():
        if lang not in ['de', 'en', 'es', 'fa', 'fr', 'it', 'ja', 'pl', 'pt',
                        'ru', 'zh']:
            print('No clickstream data for %s' % lang)
            continue
        redirects_r = {x: k for k, v in redirects[lang].items() for x in v}
        l_redirects[lang] = redirects_r.copy()
        tl0 = time.time()
        tr = tl0
        for month, arts in artdict.items():
            print(lang, month)
            if 'n_%s' % month in cs_df_dict.get(lang, []):
                print('Already got %s %s' % (lang, month))
                continue
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
            # if agg_rd:
            #     articles = set(df['source']) | set(df['target'])
            #     l_redirects[lang] = fix_redirects(articles, l_redirects[lang],
            #                                       lang, norm_keys=False)

            #     df['source'] = df['source'].apply(lambda x:
            #                                       l_redirects[lang].get(x, x))
            #     df['target'] = df['target'].apply(lambda x:
            #                                       l_redirects[lang].get(x, x))

            #     df = df.groupby(['source', 'target',
            #                      'type']).sum().reset_index()
            td = time.time()
            if lang not in cs_df_dict:
                cs_df_dict[lang] = pd.DataFrame(
                    index=df.set_index(list(df.columns[:3])).index)

            cs_df_dict[lang] = pd.concat([cs_df_dict[lang],
                                          df.set_index(list(df.columns[:3])
                                                       )['n_%s' % month]],
                                         axis=1)

            ts = time.time()
            if csdf_path:
                cs_df_dict[lang].reset_index().to_hdf(csdf_path, key=lang)
            
            tb = time.time()
            average = (tb-tl0)/(list(artdict.keys()).index(month)+1)
            tl_remaining = average*(len(artdict)-list(artdict.keys()).index(month)+1)/3600

            print('Month %s took %.2fs total, %.2fs to save, %.2fs to join data, %.2fs to process'
                    % (month, tb-tr, tb-ts, ts-td, td-tr))
            print('Total lang time elapsed: %.2fh' % ((tb-tl0)/3600))
            print('Total time elapsed: %.2fh' % ((tb-t0)/3600))
            print('Lang time remaining: %.2fh' % tl_remaining)
            tr = time.time()


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
    df = download_read_table(f_url, filepath, cols, headers)
    return df


def get_datelangartdict(langartdict, redirects, days=False, offset = {}):
    """Converts a dictionary of language-article-date ranges to a dictionary of
    date-language-article ranges.

    Args:
        langartdict (dict): Dictionary of language-article-date ranges.
        redirects (dict): Dictionary of redirects.
        days (bool, optional): Whether to get full days at the end of range.
        Defaults to False.
        offset (dict, optional): Dictionary of time offset. Defaults to {}.

    Returns:
        dict: Dictionary of date-language-article ranges.
    """    
    alldatetimes = {}
    for lang, artdict in langartdict.items():
        for art, dateranges in artdict.items():
            if offset:
                dateranges2 += [(dr[0] + relativedelta(offset[lang]),
                                 dr[1] + relativedelta(offset[lang]))
                              for dr in dateranges]
            for dr in dateranges2:
                if days:
                    dr = (dr[0].floor('D'),
                        dr[1].ceil('D') - datetime.timedelta(hours=1))
                for d in pd.date_range(*dr, freq='H'):
                    if d not in alldatetimes:
                        alldatetimes[d] = {}
                    if lang not in alldatetimes[d]:
                        alldatetimes[d][lang] = {}
                    if art not in alldatetimes[d][lang]:
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
            f_arts = arts - set(hdfl['article'].values)
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


def get_hourly_pageviews(datelangartdict, redirects, data_path, pvdf_path, 
                         agg_rd=True, agg_dm=True, dl_first=True,
                         pop_zero=True, savemode='hdf', savekwargs={}, ret=False,
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

    if (savemode=='hdf') & (os.path.exists(pvdf_path)):
        existing_df = pd.read_hdf(pvdf_path, key='df')
    else:
        existing_df = pd.DataFrame()

    # reshape input
    # datelangartdict = get_datelangartdict(langartdict, redirects)

    if pvdf_path:
        pd.Series({'save_df': bool(data_path), 'save_hpv': bool(pvdf_path),
                   'agg_rd': agg_rd, 'agg_dm': agg_dm}
                   ).to_hdf(pvdf_path, key='df_info')
    # t0 = time.time()
    # print(t0)
    if dl_first:
        download_pageview_hours(datelangartdict.keys(), data_path, **kwargs)
        print('All files downloaded')

    t0 = time.time()
    tl = t0
    for n, (hour, langdict) in enumerate(datelangartdict.items()):
        print(hour)
        # print(time.time()-t0)
        print('Page views progress', round(100*n/len(datelangartdict), 4), '%')
        # filter down langdict
        if savemode=='sql':
            ts = time.time()
            try:
                existing_df = pd.read_sql_query('SELECT * FROM pageviews WHERE hour="%s"' %hour.isoformat(sep=' '),
                                                con=savekwargs['con'], parse_dates=['hour'], index_col='hour')
            except:
                existing_df = pd.DataFrame()
            print('SQL read time (length %d):' %len(existing_df), time.time()-ts)

        tc = time.time()
        if (not agg_rd): #&(bool(redirects)):
            rdlangdict = {k: [y for z in [redirects[k][x] for x in v]
                                for y in z] for k, v in langdict.items()}
            flangdict = check_rows(hour, existing_df, langdict=rdlangdict)
        else:
            flangdict = check_rows(hour, existing_df, langdict=langdict) 
        if not flangdict:
            print('No new rows')
            continue
        print('Check rows time:', time.time()-tc)

        tg = time.time()
        df = get_pageview_hour(hour, data_path, **kwargs)
        print('Get pageview hour time:', time.time()-tg)

        # get views for article(s) (desktop+mobile)
        tf = time.time()
        edf = pd.DataFrame()
        for lang, arts in flangdict.items():
            rdarts = {k: redirects[lang][k] for k in arts}
            qviews = filter_pv_df(lang, rdarts, df, hour, pop_zero, agg_rd,
                                    agg_dm)
            edf = pd.concat([edf, qviews])
            if edf['domain'].isnull().sum():
                raise
        print('Filter df time:', time.time()-tf)

        tcc = time.time()
        if ret | (bool(pvdf_path)&(savemode == 'hdf')):
            existing_df = pd.concat([existing_df, edf])
            existing_df = existing_df.sort_index()
        print('Concat time:', time.time()-tcc)

        # save data
        tsv = time.time()
        if pvdf_path:
            if savemode == 'hdf':
                if os.path.exists(pvdf_path):
                    os.rename(pvdf_path, pvdf_path.replace('.h5', '_old.h5'))
                existing_df.to_hdf(pvdf_path, key='df')
            elif savemode == 'sql':
                #edf???
                edf.reset_index().rename({'index': 'hour'}, axis=1).to_sql('pageviews', con=savekwargs['con'],
                            if_exists=savekwargs['if_exists'], index=False)
            else:
                raise ValueError('savemode must be "hdf" or "sql"')
        print('Save time:', time.time()-tsv)

        th = time.time() - tl
        tl = time.time()
        total = time.time()-t0
        average = total/(n+1)
        progress = round(100*n/len(datelangartdict), 2)
        remaining1 = (len(datelangartdict)-n)*average/3600
        remaining2 = (len(datelangartdict)-n)*th/3600
        print('Progress %f%%, time %fh, average %.1fs, remaining 1 %fh, remaining 2 %fh'
              %(progress, total/3600, average, remaining1, remaining2))

    if bool(pvdf_path)&(savemode == 'hdf'):
        os.remove(pvtdf_path.replace('.h5', '_old.h5'))

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

#%% Page views (daily dumps)

def find_file_lines(filepath):
    """Find the first lines of the file that contains the first wikipedia pageview,
    and the first line that contains the first wikipedia pageview with 6 columns.

    This is used to adapt to polars erratic csv reading behavior.

    Args:
        filepath (str): Path to the file.

    Returns:
        tuple (int, int, list): Index of first wikipedia line, first wikipedia
        line with 6 columns, list of dictionaries with the first wikipedia data
        before 6 columns are found.
    """

    firstwikip = 0
    needwk = True
    data = []
    for n, line in enumerate(open(filepath, 'r')):
        if (n==0)&(line.split()[0]=='domain'):
            continue
        try:
            wk = line.split('.')[1][:9] == 'wikipedia'
        except IndexError:
            continue        
        l = len(line.split()) == 6

        if wk & l:
            firstwikip6 = n
            if not firstwikip:
                firstwikip = n
            break
        elif wk:      
            data.append(dict(zip(['domain', 'page', 'source', 'daily', 'hourly'], line.split())))
            if needwk:
                needwk = False
                firstwikip = n
        
    return firstwikip, firstwikip6, data

def read_pageviews(filepath, returnroute=False):
    """Reads a pageview file and returns a polars dataframe.

    Args:
        filepath (str): Path to the file.
        returnroute (bool, optional): Whether to return if the polars/pandas
        reader was used. Defaults to False.

    Returns:
        _type_: Polars dataframe with pageviews data.
        bool (optional): Whether the polars reader was used.
    """
    polars = True
    try:
        # polars reader is erratic, so we need to find the first wikipedia line
        # and the first wikipedia line with 6 columns
        firstwikip, firstwikip6, data = find_file_lines(filepath)

        # read the initial file lines with 5 columns with polars
        if data:
            df0 = pl.DataFrame(data).with_columns(
                                            pl.lit('0').alias('id')
                                            ).filter(
                                            pl.col('domain').str.ends_with('.wikipedia')
                                            ).with_columns(
                                                pl.col('id').cast(pl.Int64),
                                                pl.col('daily').cast(pl.Int64),
                                            )
        else:
            df0 = pl.DataFrame(schema={'domain':str, 'page':str, 'id':int,
                                       'source':str, 'daily':int, 'hourly':str})
        
        # read subsequent (mostly) 6 column lines with polars
        df1 = pl.read_csv(filepath, separator=' ', has_header=False, skip_rows=firstwikip6,
                            quote_char=None, null_values='', infer_schema_length=0,
                            columns=list(range(6)),
                            new_columns=['domain', 'page', 'id',  'source', 'daily', 'hourly']
                            )
    except pl.ComputeError as e: # if polars reader fails with 6 column read, try pandas
        print('Polars reading error, trying pandas')
        print(e)
        polars = False
        df0 = pl.DataFrame(schema={'domain':str, 'page':str, 'id':int,
                                   'source':str, 'daily':int, 'hourly':str})
        df1 = pl.from_pandas(pd.read_csv(filepath, sep=' ', header=None,
                    keep_default_na=False, na_values='', quoting=3,
                    names=['domain', 'page', 'id',  'source', 'daily', 'hourly'],
                    dtype=str))
    
    # Some data wrangling needs to be done:
    # 1. Some entries have data split across two rows, need to be merged.
    # 2. We only need data for wikipedia domains.
    # 3. Rows with missing ID need to have column data shifted across.
    # 4. Cast ID and daily columns to int.

    df1 = df1.with_columns( # 1. merge split rows 
                            pl.when(pl.col('domain').is_null()
                                    ).then(
                                        pl.col('daily').alias('hourly')
                                    ).otherwise(pl.col('hourly')),
                            pl.when(pl.col('domain').is_null()
                                    ).then(
                                        pl.col('source').alias('daily')
                                    ).otherwise(pl.col('daily')),
                            pl.when(pl.col('domain').is_null()
                                    ).then(
                                        pl.col('id').alias('source')
                                    ).otherwise(pl.col('source')),
                            pl.when(pl.col('domain').is_null()
                                    ).then(
                                        pl.col('page').alias('id')
                                    ).otherwise(pl.col('id')),
                            pl.when(pl.col('domain').is_null()
                                    ).then(
                                        pl.col('page').shift(-1)
                                    ).otherwise(pl.col('page')),
                            pl.when(pl.col('domain').is_null()
                                    ).then(
                                        pl.col('domain').shift(-1)
                                    ).otherwise(pl.col('domain'))
                        ).drop_nulls(
                        subset=['source']
                        ).filter( # 2. only wikipedia domains
                            pl.col('domain').str.ends_with('.wikipedia')
                        ).with_columns( # 3. shift data for missing ID
                            pl.when(
                                pl.col('hourly').is_null()
                            ).then(
                                pl.lit(0).alias('id'),
                            ).otherwise(
                                pl.col('id')
                            ),
                            pl.when(
                                pl.col('hourly').is_null()
                            ).then(
                                pl.col('id').alias('source'),
                            ).otherwise(
                                pl.col('source')
                            ),
                            pl.when(
                                pl.col('hourly').is_null()
                            ).then(
                                pl.col('source').alias('daily'),
                            ).otherwise(
                                pl.col('daily'),
                            ),
                            pl.when(
                                pl.col('hourly').is_null()
                            ).then(
                                pl.col('daily').alias('hourly'),
                            ).otherwise(
                                pl.col('hourly'),
                            )
                        ).with_columns( # 4. cast ID and daily columns to int
                            pl.col('id').map_dict({'null':pl.Null()},
                                        default=pl.first()).cast(pl.Int64),
                            pl.col('daily').cast(pl.Int64)
                        )

    # combine dfs and add a column with the date
    tdf = pl.concat(
            [df0.select('domain', 'page', 'id',  'source', 'daily', 'hourly'),
            df1]
            ).with_columns(
                pl.col('source').cast(pl.Categorical),
            )
    
    if returnroute:
        return tdf, polars
    else:
        return tdf

def filter_pageviews(tdf, date, langarts, redirects, rdr_flat, fill_empty=False):
    """Filter pageviews dataframe to only include pages in the list of pages.

    Args:
        tdf (_type_): Polars dataframe with pageviews data for that day.
        date (_type_): Date of the pageviews data.
        langarts (dict): Dictionary of languages and pages to include.
        redirects (dict): Dictionary of redirects for each language.
        rdr_flat (dict): Redirects of langpages to true page title.
        fill_empty (bool, optional): Whether to fill values for days with no
        data to 0. Defaults to False.

    Returns:
        _type_: Dataframe with pageviews data for that day, filtered to only
        include pages in the list of pages.
    """
    # map hour letter to number
    md = {x: ord(x)-65 for x in 'ABCDEFGHIJKLMNOPQRSTUVWX'}

    # flatten articles and redirects required for filtering
    rdartsl = {l: [y for x in arts for y in redirects[l][x]] for l, arts in langarts.items()}
    rdartsl_flat = {'%s__%s' %(k, x) for k, v in rdartsl.items() for x in v}
    del rdartsl
    
    gdf = tdf.with_columns( # add language column
                        pl.col('domain').str.split('.').list.get(0).alias('lang')
                    ).with_columns( # create unique langpage column for filtering
                        pl.concat_str([pl.col('lang'), pl.lit('__'), pl.col('page')]
                                    ).alias('langpage'),
                    ).filter( # filter out pages not in the list
                        pl.col('langpage').is_in(rdartsl_flat)
                    ).select( # convert hourly column to list of tuples
                        pl.col('hourly').str.extract_all('(\w)(\d+)'),
                        'daily',
                        pl.col('lang').cast(pl.Categorical),
                        pl.col('langpage').map_dict(rdr_flat).alias('page') # map page to where it redirects to
                    ).explode( # explode hourly column for unique entry per hour
                        'hourly'
                    ).select( # map hour letter to number and cast views per hour to int
                        'lang',
                        'page',
                        'daily',
                        pl.col('hourly').str.slice(0,1).map_dict(md).alias('hour'),
                        pl.col('hourly').str.slice(1).alias('views').cast(pl.Int64)
                    ).pivot( # pivot to get views per hour horizontally
                        index=('lang', 'page', 'daily'), columns='hour',
                        values='views', aggregate_function='sum'
                    ).with_columns( # add missing hours if required
                        *[pl.col(str(x)).cast(pl.Int64) for x in range(24)]
                    ).groupby( # group by language, page
                        'lang',
                        'page'
                    ).agg( # sum views per hour per unique page
                        pl.col('daily').sum(),
                        *[pl.col(str(x)).sum() for x in range(24)]
                    ).with_columns( # add date column
                        pl.lit(date).cast(pl.Date).alias('date')
                    )
    if fill_empty:
        raise('not yet implemented')
        # missing = 
        # if missing:
        #     fills = 
        #     gdf = pl.concat([gdf, missing])
        # pass

    return gdf



