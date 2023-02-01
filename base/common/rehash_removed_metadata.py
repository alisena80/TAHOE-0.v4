import hashlib
from pip._internal.utils.misc import read_chunks
from base64 import urlsafe_b64encode
import os
import sys
def rehash(path, blocksize=1 << 20):
    # type: (str, int) -> Tuple[str, str]
    """Return (hash, length) for path using hashlib.sha256()"""
    h = hashlib.sha256()
    length = 0
    with open(path, 'rb') as f:
        for block in read_chunks(f, size=blocksize):
            length += len(block)
            h.update(block)
    digest = 'sha256=' + urlsafe_b64encode(
        h.digest()
    ).decode('latin1').rstrip('=')
    # unicode/str python2 issues
    return (digest, str(length))
print(rehash(sys.argv[0]))