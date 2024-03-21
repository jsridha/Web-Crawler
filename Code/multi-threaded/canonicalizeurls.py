import re
from urllib.parse import urlparse, urlunparse, urljoin

def canonicalize_url(url, base=None):
    # Parse the URL
    parsed_url = urlparse(url)

    # Convert scheme and host to lower case
    scheme = parsed_url.scheme.lower()
    domain = parsed_url.netloc.lower()

    # Remove default ports
    if (scheme == 'http' and parsed_url.port == 80) or (scheme == 'https' and parsed_url.port == 443):
        domain = domain.split(':')[0]

    # Make relative URLs absolute
    if not parsed_url.netloc and base:
        base_url = urlparse(base)
        url = urljoin(base_url.geturl(), url)
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme.lower()
        domain = parsed_url.netloc.lower()

    # Convert to http
    if scheme == 'https':
        scheme = 'http'

    # Remove fragment
    url = urlunparse((scheme, domain, parsed_url.path, parsed_url.params, '', ''))

    # Remove duplicate slashes except after http:
    url = re.sub(r'([^:])//+', r'\1/', url)

    return url
