from wikitools.api import *
from wikitools.redirects import *
import mwapi


async def parse_links(data, prop):

    if prop in ['links', 'linkshere']:
        # rds = {}
        links = []
        redirects = {}
        norms = {}
        ids = {}
        for page in await data:
            if 'query' not in page:
                if 'title' in page:
                    links.append(page)
                else:
                    links=None
            
            else:
                links.extend(page['query'].get('pages', []))
                redirects.update({x['from']: x['to']
                                for x in page['query'].get('redirects', {})})
                redirects.update({l['title']: None
                                for l in page['query'].get('pages', [])
                                if 'missing' in l})
                norms.update({x['from']: x['to']
                                for x in page['query'].get('normalized', {})})
                ids.update({x['title']: x.get('pageid', -1)
                                for x in page['query'].get('pages', [])})

        return links, redirects, norms, ids

    links = {}
    for page in await data:
        new_links = {(page['pageid'], page['title']): page.get(prop, [])}
        for k, v in new_links.items():
            if k in links:
                links[k].extend(v)
            else:
                links[k] = v

    if prop=='langlinks':
        langlinks = {art:{} for art in links.keys()}
        for k, v in links.items():
            for l in v:
                if l['lang'] in langlinks[k]:
                    langlinks[k][l['lang']].append(l['title'])
                else:
                    langlinks[k][l['lang']] = [l['title']]
        return langlinks
    else:
        return links

async def get_links(session, mode='out', titles=None, pageids=None, norm_map={}, redirect_map={}, id_map={}, namespaces=[0], resolve_redirects=False):
    """Get links on each article in list.

    Args:
        articles (list): List of articles.
        lang (str, optional): Language. Defaults to 'en'.

    Returns:
        dict: Dictionary of articles and their links.
    """
    if not (bool(titles) ^ bool(pageids)):
        raise ValueError('Must specify exactly one of titles or pageids')
    
    if namespaces == 'all':
        ns = '*'
    else:
        ns = '|'.join([str(x) for x in namespaces])

    modedict = {'out': {'pg':'generator', 'pval': 'links', 'ns': 'gplnamespace', 'limit': 'gpllimit'},
                'in': {'pg':'generator', 'pval': 'linkshere', 'ns': 'glhnamespace', 'limit': 'glhlimit'},
                'lang': {'pg':'prop', 'pval': 'langlinks', 'limit': 'lllimit'},
                'interwiki': {'pg':'prop', 'pval': 'iwlinks', 'limit': 'iwlimit'},
                'ext': {'pg':'prop', 'pval': 'extlinks', 'limit': 'ellimit'}}
    
    if type(mode) == str:
        if mode == 'all':
            mode = ['out', 'in', 'lang', 'interwiki', 'ext']
        else:
            mode = [mode]
    
    return_dict = {}
    for m in mode:  
        params = {modedict[m]['pg']: modedict[m]['pval'],
                modedict[m]['limit']: 'max',
                'redirects':resolve_redirects}
        if m in ['out', 'in']:
            params[modedict[m]['ns']] = ns
        query_args_list, key, ix = querylister(titles, pageids, generator=(m in ['out', 'in']), norm_map=norm_map, titles_redirect_map=redirect_map, params=params)
        data = await iterate_async_query(session, query_args_list,
                                         function=parse_links, args=[modedict[m]['pval']],
                                         debug=resolve_redirects&(m in ['out', 'in']))
        if m in ['out', 'in']:
            data_keys = [x[key] for x in query_args_list]
            if resolve_redirects:
                links = dict(zip(data_keys, [x[0] for x in data]))
                for x in data:
                    redirect_map.update(x[1])
                    norm_map.update(x[2])
                    id_map.update(x[3])
                missing = [k for k, v in links.items() if v is None]
                redirect_map.update({x: None for x in missing})
                id_map.update({x: -1 for x in missing})
            else:
                links = dict(zip(data_keys, [x[0] for x in data]))
        else:
            links = {k[ix]: val for d in data for k, val in d.items()}

        return_dict[m] = links
    
    if len(return_dict) == 1:
        return return_dict[mode[0]]
    else:
        return return_dict


async def pipeline_get_links(project, user_agent, titles=None, pageids=None, id_map=None, norm_map=None, redirect_map=None,
                             gl_args={}, asynchronous=True, session_args={'formatversion':2}):

    return_maps = {'id_map': id_map, 'redirect_map': redirect_map,
                   'norm_map': norm_map}
    return_bools = {k: v is None for k, v in return_maps.items()}

    for k, v in return_maps.items():
        if v is None:
            return_maps[k] = {}

    url = f'https://{project}.org'

    if asynchronous:
        async_session = mwapi.AsyncSession(url, user_agent=user_agent, **session_args)
    else:
        session = mwapi.Session(url, user_agent=user_agent, **session_args)

    if asynchronous:
        if titles:
            await fix_redirects(async_session, titles, return_maps['redirect_map'],
                                return_maps['norm_map'], return_maps['id_map'])
        links = await get_links(async_session, titles=titles, pageids=pageids, norm_map=return_maps['norm_map'],
                        redirect_map=return_maps['redirect_map'], **gl_args)
    else:
        raise ValueError('Only async supported at present.')
    
    return_dict = {'links': links}
    if any(return_bools.values()):
        for k, v in return_bools.items():
            if v:
                return_dict[k] = return_maps[k]
        return return_dict
    else:
        return links