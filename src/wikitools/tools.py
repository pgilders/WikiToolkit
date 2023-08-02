from math import log10, floor

def round_sig(x, sig=2):
    """Rounds a number to a given number of significant figures.

    Args:
        x (float): Number to round.
        sig (int, optional): Number of significant figures. Defaults to 2.

    Returns:
        float: Rounded number.
    """    
    if x:
        return round(x, sig-int(floor(log10(abs(x))))-1)
    else:
        return 0


def chunks(l, n):
    """
    Split list l into list of lists of length n.

    Parameters
    ----------
    l : list
        Initial list.
    n : int
        Desired sublist size.

    Yields
    ------
    list
        Subsequent sublists of length n.

    """
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]




# def wiki_details(row):
#     out = []
#     # if row['entities.urls'] != row['entities.urls']:
#     # return []
#     for link in row['entities.urls']:
#         match = re.match("https:\/\/([^\/]*).wikipedia.org\/([^\/]*)/([^?]*)",
#                          link.get('unwound_url', ''))
#         if match:
#             locale = match.group(1)
#             endpoint = match.group(2)
#             page = urllib.parse.unquote(match.group(3))
#             lang = locale.replace('.m', '')
#             if '.m' in locale:
#                 mobile = True
#             else:
#                 mobile = False

#             out.append({'tweet_id': row.name, 'tweet_date': row['created_at'],
#                         'url': link['unwound_url'], 'url_start': link['start'],
#                         'url_end': link['end'], 'lang': lang,
#                         'endpoint': endpoint, 'page': page, 'mobile': mobile})
#     return out

# def wiki_details2(row):
#     out = []
#     # if row['entities.urls'] != row['entities.urls']:
#     # return []
#     for link in row['entities.urls']:
#         match = re.match("https:\/\/([^\/]*).wikipedia.org\/([^\/]*)/([^?]*)",
#                          link.get('unwound_url', ''))
#         if match:
#             locale = match.group(1)
#             endpoint = match.group(2)
#             page = urllib.parse.unquote(match.group(3))
#             lang = locale.replace('.m', '')
#             if '.m' in locale:
#                 mobile = True
#             else:
#                 mobile = False

#             out.append({'tweet_id': row.name, 'tweet_date': row['created_at'],
#                         'url': link['unwound_url'], 'lang': lang,
#                         'endpoint': endpoint, 'page': page, 'mobile': mobile})
#     return out





