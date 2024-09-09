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
    """Split list l into a list of lists of length n.

    Args:
        l (list): Initial list.
        n (int): Desired sublist size.

    Yields:
        list: Subsequent sublists of length n.

    """
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

def process_articles(titles=None, pageids=None, pagemaps=None):
    """Process article titles or pageids. Runs normalisation and redirects.

    Args:
        titles (list, optional): The article titles to process. Must specify exactly one of titles or pageids. Defaults to None.
        pageids (list, optional): The article IDs to process. Must specify exactly one of titles or pageids. Defaults to None.
        pagemaps (PageMaps, optional): The PageMaps object to map redirects with. Defaults to None.

    Raises:
        ValueError: Must specify exactly one of titles or pageids

    Returns:
        list: Processed article titles or pageids.
    """
    if not ((titles is not None) ^ (pageids is not None)):
        raise ValueError('Must specify exactly one of titles or pageids')
    elif (not titles)&(not pageids):
        return []

    if titles:
        items = titles
        redirect_map = pagemaps.titles_redirect_map
    else:
        items = pageids
        redirect_map = pagemaps.pageids_redirect_map

    if (type(items) == str)|(type(items) == int):
        items = [items]

    if titles:
        items = [pagemaps.norm_map.get(a, a) for a in items]
    items = [redirect_map.get(a, a) for a in items]
    items = list(dict.fromkeys([a for a in items if a]))

    return items
