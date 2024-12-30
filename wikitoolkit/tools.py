from math import log10, floor
import requests

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


def download_large_file(url, output_path, chunk_size=1024 * 1024):
    """
    Download a large file in chunks and save it to a specified path.

    Args:
        url (str): URL of the file to download.
        output_path (str): Path where the file will be saved.
        chunk_size (int): Size of each chunk in bytes. Default is 1024 bytes (1 KB).
    """
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()  # Raise an error for bad status codes
            total_size = int(response.headers.get('content-length', 0))
            with open(output_path, 'wb') as file:
                print(f"Starting download: {url}")
                downloaded = 0
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:  # Filter out keep-alive chunks
                        file.write(chunk)
                        downloaded += len(chunk)
                        # Optional: Print download progress
                        print(f"\rDownloaded {downloaded}/{total_size} bytes ({(downloaded/total_size)*100:.2f}%)", end="")
            print("\nDownload completed.")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")