import requests
import pandas as pd
from wikitools.tools import chunks

def query(request, lang='en'):
    """
    Query Wikipedia API with specified parameters.

    Parameters
    ----------
    request : dict
        API call parameters.

    Raises
    ------
    ValueError
        Raises error if returned by API.

    Yields
    ------
    dict
        Subsequent dicts of json API response.

    """
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


def parse(request, lang='en'):
    """
    Query Wikipedia API with specified parameters.

    Parameters
    ----------
    request : dict
        API call parameters.

    Raises
    ------
    ValueError
        Raises error if returned by API.

    Yields
    ------
    dict
        Subsequent dicts of json API response.

    """
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


#%% Redirects

def fix_redirects(articles, existingmap={}, norms={}, lang='en',
                  norm_keys=True, nonepages=True):
    """
    Map redirect name to true Wikipedia article name.

    Parameters
    ----------
    articles : iterable
        Wikipedia article names.
    existingmap : dict, optional
        If a (partial) existing map exists, combine. The default is {}.

    Returns
    -------
    dict
        Map with redirect name keys and true article name values.

    """
    tar_chunks = list(chunks(list(set(articles)-set(existingmap.keys())), 50))
    mapping = {}
    for n, i in enumerate(tar_chunks):
        if (n % 100 == 0) & (n > 0):
            print('Fixing redirects', round(100*n/len(tar_chunks), 2), '%')
        istr = '|'.join(i)
        params = {'titles': istr, 'redirects': ''}
        dat = list(query(params, lang))
        for j in dat:
            if norm_keys:
                try:
                    for k in j['normalized']:
                        norms[k['from'].replace(' ', '_')
                              ] = k['to'].replace(' ', '_')

                except KeyError:
                    # print('No redirects in this chunk')
                    pass

            try:
                for k in j['redirects']:
                    mapping[k['from'].replace(' ', '_')
                            ] = k['to'].replace(' ', '_')

            except KeyError:
                # print('No redirects in this chunk')
                pass

            if nonepages:
                try:
                    for k, v in j['pages'].items():
                        if k[0] == '-':
                            mapping[v['title'].replace(' ', '_')] = None

                except KeyError:
                    # print('No pages in this chunk')
                    pass
    # Apply mapping to self to fix second order redirects
    # print('Consolidating mapping')
    titlemap = mapping.copy()
    for k, v in mapping.items():
        for i in mapping.keys():
            if i == v:
                titlemap[k] = mapping[i]
                break

    if norm_keys:
        return {**existingmap, **titlemap}, norms
    else:
        return {**existingmap, **titlemap}


def get_redirects(articles, existingrds={}, lang='en', verbose=False):
    """
    Get all redirects to a true Wikipedia article name.

    Parameters
    ----------
    articles : iterable
        Wikipedia article names.
    existingrds : dict, optional
        If a (partial) existing map exists, combine. The default is {}.

    Returns
    -------
    dict
        Map with true article name keys and all names that redirect to it
        in a list as values.
    """
    tar_chunks = list(chunks(list(set(articles)-set(existingrds.keys())), 50))
    rd_map = {x.replace(' ', '_'): [x.replace(' ', '_')] for x in articles}
    for n, i in enumerate(tar_chunks):
        if (verbose) & (n % 100 == 0):
            print('Getting redirects %.2f%%' % (100*n/len(tar_chunks)))
        istr = '|'.join(i)
        params = {'titles': istr, 'prop': 'redirects',
                  'rdlimit': 'max', 'rdnamespace': '0'}
        try:
            for j in query(params, lang=lang):
                for k in j['pages'].values():
                    try:
                        rd_map[k['title'].replace(' ', '_')].extend(
                            [x['title'].replace(' ', '_') for x in k['redirects']])
                    except KeyError:
                        pass
        except ValueError:
            for is2 in ['|'.join(istr.split('|')[:25]),
                        '|'.join(istr.split('|')[25:])]:
                params['titles'] = is2
                for j in query(params, lang=lang):
                    for k in j['pages'].values():
                        try:
                            rd_map[k['title'].replace(' ', '_')].extend(
                                [x['title'].replace(' ', '_') for x in k['redirects']])
                        except KeyError:
                            pass
    return {**existingrds, **rd_map}

#%% links

def get_links(articles, lang='en'):
    """Get links on each article in list.

    Args:
        articles (list): List of articles.
        lang (str, optional): Language. Defaults to 'en'.

    Returns:
        dict: Dictionary of articles and their links.
    """
    ldict = {x.replace(' ', '_'): [] for x in articles}

    tar_chunks = list(chunks(list(set(articles)), 50))
    for n, i in enumerate(tar_chunks):
        istr = '|'.join(i)
        params = {'titles': istr, 'prop': 'links', 'pllimit': 'max'}
        for j in query(params, lang=lang):
            for k in j['pages'].values():
                try:
                    ldict[k['title'].replace(' ', '_')] += [x['title']
                                                            for x in k['links']]
                except KeyError:
                    pass

    return ldict

def get_langlinks(articles, lang='en'):
    """    Get all lang links from a true Wikipedia article name.

    Args:
        articles (list): List of articles.
        lang (str, optional): Language. Defaults to 'en'.

    Returns:
        dict: Dictionary of articles and their lang links.
    """
    lldict = {x.replace(' ', '_'): {lang: x.replace(' ', '_')}
              for x in articles}

    tar_chunks = list(chunks(list(set(articles)), 50))
    for n, i in enumerate(tar_chunks):
        istr = '|'.join(i)
        params = {'titles': istr, 'prop': 'langlinks', 'lllimit': 'max'}
        for j in query(params, lang=lang):
            for k in j['pages'].values():
                try:
                    nd = {x['lang']: x['*'].replace(' ', '_')
                          for x in k['langlinks']}
                    lldict[k['title'].replace(' ', '_')].update(nd)
                except KeyError:
                    pass
    return lldict


#%% Revisions

def get_revision(title, pageid, date, props=['timestamp', 'ids', 'content']):
    """Get data for a particular revision of a page.

    Args:
        title (str): Article title.
        pageid (int): Page ID of article.
        date (_type_): Date to retrieve revision for.
        props (list, optional): Revision properties to collect. Defaults to ['timestamp', 'ids', 'content'].

    Returns:
        _type_: DataFrame with revision data.
    """

    r = {'prop': 'revisions', 'titles': title, 'rvdir': 'older',
         'rvstart': date, 'rvlimit': 1, 'rvslots': 'main',
         'rvprop': '|'.join(props)}
    rd = query(r)
    dat = next(rd)
    return pd.json_normalize(dat['pages'][str(pageid)]['revisions'][0])


def get_revisions(title, pageid, start, stop, props=['timestamp', 'ids']):
    """Get revisions for a page between two dates.

    Args:
        title (str): Article title.
        pageid (int): Page ID of article.
        start (_type_): Start date.
        stop (_type_): Stop date.
        props (list, optional): Revision properties to collect. Defaults to ['timestamp', 'ids'].

    Returns:
        _type_: DataFrame with revision data.
    """

    r = {'prop': 'revisions', 'titles': title, 'rvdir': 'newer',
         'rvstart': start, 'rvend': stop, 'rvlimit': 'max', 'rvslots': 'main',
         'rvprop': '|'.join(props)}
    rd = query(r)
    dat = [y for x in rd for y in x['pages'].get(pageid, {}).get('revisions', [])]
    return pd.json_normalize(dat)

