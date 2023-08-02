
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