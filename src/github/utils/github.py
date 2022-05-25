import time
import requests
import pandas as pd
from utils.store_data import *


def get_github_data(context, process_data):
    results = []
    pageNumber = 1

    headers = "accept_header" in context and {
        "Accept": context["accept_header"]} or None
    query = "query" in context and f'&{context["query"]}' or ''

    while True:
        try:
            r = requests.get(
                f'https://api.github.com/{context["url_part"]}?page={pageNumber}&per_page=100{query}', auth=(context["username"], context["password"]), headers=headers)

            page = r.json()

            if len(page) == 0:
                break

            is_list = isinstance(page, list)
            results.extend(is_list and page or [page])

            pageNumber = pageNumber + 1

            time.sleep(0.1)

            print(
                f'Getting {context["data_name"]} for {context["for_name"]}. Page: {pageNumber}', end="\r")

            if ('single_page' in context and context['single_page']) or not is_list:
                break
        except:
            break

    print("", end="\r")

    df = pd.json_normalize(results)

    if df.empty:
        return df

    df = process_data(df)

    return df


def get_github_repo_data(context, process_data):
    return get_github_data(
        {
            **context,
            "for_name": f'{context["org"]}/{context["repo_name"]}',
            "url_part": f'repos/{context["org"]}/{context["repo_name"]}/{context["data_name"]}'
        },
        process_data
    )


def get_folder_name(context):
    folder_name = "."
    if("org_folder" in context):
        folder_name += f"/{context['org_folder']}"
    if("repo_name" in context):
        folder_name += f"/{context['repo_name']}"

    return folder_name
