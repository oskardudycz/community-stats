import pandas as pd
from utils.store_data import *
from utils.github import *


def get_repositories(context, onlyPublic):
    def store(df):
        storeDataFrame(df, get_folder_name(context), "repositories_all.csv")

        df.query('fork == False', inplace=True)
        if onlyPublic:
            df.query('private == False', inplace=True)

        storeDataFrame(df, get_folder_name(context), "repositories.csv")

        return df

    return get_github_data(
        {
            **context,
            "data_name": 'repositories',
            "for_name": context["org"],
            "url_part": f'orgs/{context["org"]}/repos'
        },
        store
    )


def get_stargazers(context):
    def store(df):
        # parse dates
        df['starred_at'] = pd.to_datetime(df['starred_at'],
                                          format="%Y-%m-%dT%H:%M:%SZ")

        storeDataFrame(df, get_folder_name(context), "stargazers.csv")

        return df

    return get_github_repo_data(
        {
            **context,
            "data_name": 'stargazers',
            "accept_header": 'application/vnd.github.v3.star+json'
        },
        store
    )


def get_issues(context):
    def store(df):
        # parse dates
        df['created_at'] = pd.to_datetime(df['created_at'],
                                          format="%Y-%m-%dT%H:%M:%SZ")

        df['closed_at'] = pd.to_datetime(df['closed_at'],
                                         format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')

        # make sure that all non-uniform data have headers
        df = df.reindex(
            columns=["created_at", "closed_at", "pull_request.url"])

        # filter out issues
        issues = df[(df["pull_request.url"].isnull())].filter(
            ['created_at', "closed_at"])
        storeDataFrame(issues, get_folder_name(context), "issues.csv")

        # filter out pull requests
        pull_requests = df[(df["pull_request.url"].notnull())].filter(
            ['created_at', "closed_at"])
        storeDataFrame(
            pull_requests, get_folder_name(context), "pull_requests.csv")

        return df

    return get_github_repo_data(
        {
            **context,
            "data_name": 'issues',
            "query": "state=all",
            "accept_header": 'application/vnd.github.v3+json'
        },
        store,

    )


def get_releases(context):
    def store(df):
        df['created_at'] = pd.to_datetime(df['created_at'],
                                          format="%Y-%m-%dT%H:%M:%SZ")

        storeDataFrame(df, get_folder_name(context), "releases.csv")

        return df

    return get_github_repo_data(
        {
            **context,
            "data_name": 'releases',
            "accept_header": 'application/vnd.github.v3.star+json'
        },
        store
    )


def get_commits(context):
    def store(df):
        storeDataFrame(df, get_folder_name(context), "commits.csv")

        return df

    return get_github_repo_data(
        {
            **context,
            "data_name": 'commits',
            "accept_header": 'application/vnd.github.v3+json'
        },
        store
    )


def get_summary(context):
    def store(df):
        df['created_at'] = pd.to_datetime(df['created_at'],
                                          format="%Y-%m-%dT%H:%M:%SZ")

        storeDataFrame(df, get_folder_name(context), "summary.csv")

        return df

    return get_github_data(
        {
            **context,
            "data_name": 'summary',
            "for_name": f'{context["org"]}/{context["repo_name"]}',
            "url_part": f'repos/{context["org"]}/{context["repo_name"]}',
            "single_page": True
        },
        store
    )


def get_contributions_stats(context):
    def store(df):
        storeDataFrame(df, get_folder_name(context), "contributors.csv")

        return df

    return get_github_repo_data(
        {
            **context,
            "data_name": 'stats/contributors',
            "single_page": True
        },
        store
    )


def get_issue_comments(context):
    def store(df):
        df['created_at'] = pd.to_datetime(df['created_at'],
                                          format="%Y-%m-%dT%H:%M:%SZ")

        storeDataFrame(df, get_folder_name(context), "issue_comments.csv")

        return df

    return get_github_data(
        {
            **context,
            "data_name": 'issue comments',
            "for_name": f'{context["org"]}/{context["repo_name"]}',
            "url_part": f'repos/{context["org"]}/{context["repo_name"]}/issues/comments'
        },
        store
    )
