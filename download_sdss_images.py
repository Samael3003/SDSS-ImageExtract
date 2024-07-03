#!/bin/env python
"""
Script for downloading SDSS galaxy image cutouts in parallel using multi-threading
(inspired from https://github.com/thespacedoctor/panstamps)
"""

import sys
import argparse
import os
import logging
import concurrent.futures
import random
import time
import requests
import urllib
from astropy import units as u
import pandas as pd
import csv

def get_SDSS_url(ra, dec, impix=256, imsize=1*u.arcmin):
    cutoutbaseurl = 'http://skyservice.pha.jhu.edu/DR12/ImgCutout/getjpeg.aspx'
    query_string = urllib.parse.urlencode(dict(ra=ra, dec=dec, width=impix, height=impix, scale=imsize.to(u.arcsec).value/impix))
    url = cutoutbaseurl + '?' + query_string
    return url

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_url(entry):
    downloaded = False
    tries = 5
    count = 0
    uri, path = entry
    timeout = gtimeout

    randSleep = random.randint(1, 101) / 20.
    time.sleep(randSleep)

    while not downloaded and count < tries:
        try:
            r = requests.get(uri, stream=True, timeout=timeout)
        except Exception as e:
            print(e)
            count += 1
            timeout *= 2
            llog.warning(f"timeout on attempt number {count}/{tries}. Increasing to {timeout}s")
            continue

        if r.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in r:
                    f.write(chunk)
            return path
        else:
            count += 1
            llog.warning(f"Getting status code {r.status_code} on download attempt {count}/{tries}.")
            downloaded = False

    return None

def multiobject_download(urlList, downloadDirectory, log, filenames, timeout=180, concurrentDownloads=10):
    global gtimeout, llog
    llog = log
    gtimeout = float(timeout)

    thisArray = []
    totalCount = len(urlList)
    if not isinstance(downloadDirectory, list):
        for i, url in enumerate(urlList):
            filename = filenames[i]
            localFilepath = os.path.join(downloadDirectory, filename)
            thisArray.extend([[url, localFilepath]])

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(fetch_url, thisArray)
    
    returnPaths = []
    for path in results:
        returnPaths.append(path)
        percent = (float(len(returnPaths)) / float(totalCount)) * 100.
        print(f"  {len(returnPaths)} / {totalCount} ({percent:.1f}%) URLs downloaded")

    localPaths = [o[1] for o in thisArray if o[1] in returnPaths]
    return localPaths

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('list', help="Path of CSV file containing RA, DEC values")
    parser.add_argument('-o', default="images", help="Output directory for storing images")
    args = parser.parse_args()

    download_list = os.path.join(args.o, "downloaded.csv")
    url_notfound = os.path.join(args.o, "urlnotfound.csv")
    downdir = os.path.join(args.o, "panstamps")

    if not os.path.exists(args.o):
        print(f"Directory {args.o} doesn't exist. Creating...")
        os.makedirs(args.o)
    if not os.path.exists(downdir):
        os.makedirs(downdir)

    print(f"Download list is being appended to {download_list}")

    logger = logging.getLogger("my logger")

    data = pd.read_csv(args.list)
    print("Select RA and DEC columns:", [column for column in data.columns])
    racol = input("RA column: ")
    deccol = input("DEC column: ")
    objidcol = input("ObjID column: ")

    retrieved_ids = []
    try:
        downloaded = pd.read_csv(download_list, names=['paths'], dtype={"paths": str})
        print("[INFO]: Resuming previous download job")
        paths = list(downloaded["paths"])
        if not any(isinstance(item, str) for item in paths):
            print("Found non-strings in the list of paths")
        retrieved_ids = [os.path.basename(x).rstrip(".jpeg") for x in paths if isinstance(x, str)]
        print(f"Length of retrieved ids: {len(retrieved_ids)}")
    except:
        print("[INFO]: This is a fresh download...")

    batches = list(chunks(list(data[[racol, deccol, objidcol]].itertuples(index=False)), 10))
    
    # Initialize a counter for unique filenames
    counter = 1

    for batch in batches:
        concurrentDownloads = len(batch)
        objids = []
        allUrls = []

        for row in batch:
            RA, DEC, oid = row
            if str(oid) in retrieved_ids:
                print("skipped")
                continue
            objids.append(f"{counter}.jpeg")
            counter += 1

            try:
                url = get_SDSS_url(ra=RA, dec=DEC)
            except:
                with open(url_notfound, "a") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([f"{RA},{DEC} not found"])
            else:
                allUrls.append(url)

        st = time.time()
        localUrls = multiobject_download(
            urlList=allUrls,
            downloadDirectory=downdir,
            log=logger,
            timeout=180,
            concurrentDownloads=concurrentDownloads,
            filenames=objids
        )
        t = time.time() - st
        print(f"Time taken: {t}s")

        with open(download_list, "a") as csvfile:
            writer = csv.writer(csvfile)
            for item in localUrls:
                writer.writerow([item])
