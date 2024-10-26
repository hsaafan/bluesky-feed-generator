import pandas as pd
import requests
import os
import time

MAX_TRIES = 10
WAIT_TIME = 5


def generate_whitelist_ids():
    handles = read_whitelist()

    dids = []
    for handle in handles:
        req = requests.get(
            f"https://bsky.social/xrpc/com.atproto.identity.resolveHandle?handle={handle}"
        )
        for _ in range(MAX_TRIES):
            if req.status_code == 200:
                dids.append(req.json()["did"])
                break

            if req.status_code == 429:
                print(
                    f"Too many requests. Waiting {WAIT_TIME} seconds and trying again..."
                )
                time.sleep(WAIT_TIME)

    return set(dids)


def read_whitelist():
    whitelist_url = os.getenv("WHITELIST_URL")
    url = "https://drive.google.com/uc?id=" + whitelist_url.split("/")[-2]
    df = pd.read_csv(url, header=None)[0].to_list()
    return df
