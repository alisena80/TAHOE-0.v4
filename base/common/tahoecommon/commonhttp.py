import os
import json
import urllib3


def httpdownload_preload(url: str, destpath: str):
    """
    Get http download.

    :param url: Url to get data from
    :param destpath: Local destination to write file to
    """
    print("httpdownload_preload - " + url)
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    print("httpdownload_preload - response.status %d" % response.status)
    with open(destpath, 'wb') as handle:
        handle.write(response.data)
        handle.close()


def httpdownload_stream(url, destpath):
    """
    Stream http download.

    :param url: Url to get data from
    :param destpath: Local destination to write file to
    """
    print("httpdownload_stream - " + url)
    http = urllib3.PoolManager()
    response = http.request('GET', url, preload_content=False)
    print("httpdownload_stream - response.status %d" % response.status)
    with open(destpath, 'wb') as handle:
        for block in response.stream(1024):
            handle.write(block)
        handle.close()
    response.release_conn()

