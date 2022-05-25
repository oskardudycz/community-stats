
import os
import pandas as pd
import glob
from utils.store_data import *
from utils.github import *


def read_from_csv(context, for_name, cols=None):
    csv = f'{get_folder_name(context)}/{for_name}.csv'

    if not os.path.exists(csv):
        return pd.DataFrame()

    return pd.read_csv(csv, usecols=cols)


def post_process_stars(context):
    df = read_from_csv(context, "stargazers", ['starred_at', 'user.login'])

    if(df.empty):
        return

    # rename columns to make it more readable
    df.columns = ['day', 'stars']

    if df.empty:
        return

    # convert string to datetime
    df['day'] = pd.to_datetime(df['day'])

    # group per day
    by_day = df.groupby(pd.Grouper(
        key='day', freq='1D', axis='day')).count()

    # calculate cumulative sum
    by_day["cum"] = by_day.cumsum(axis=0)

    storeDataFrame(
        by_day, f"./{get_folder_name(context)}", "stargazers_by_day.csv")


def post_process_issues(context, type):
    df = read_from_csv(context, "issues", ['created_at', 'closed_at'])

    if df.empty:
        return

    # convert string to datetime
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['closed_at'] = pd.to_datetime(df['closed_at'], errors='coerce')
    df['was_opened'] = 1

    # created per day
    created_by_day = df.groupby(pd.Grouper(
        key='created_at', freq='1D')).count()

    created_by_day.columns = ['closed', 'opened']

    created_by_day["cum_closed"] = created_by_day['closed'].cumsum()
    created_by_day["cum_opened"] = created_by_day['opened'].cumsum()

    storeDataFrame(
        created_by_day, f"./{get_folder_name(context)}", f"{type}_created_by_day.csv")


def post_process_issue_comments(context):
    df = read_from_csv(context, "issue_comments", ['created_at'])

    if df.empty:
        return

    # convert string to datetime
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['comments'] = 1

    # created per day
    created_by_day = df.groupby(pd.Grouper(
        key='created_at', freq='1D')).count()

    created_by_day["cum"] = created_by_day['comments'].cumsum()

    storeDataFrame(
        created_by_day, f"./{get_folder_name(context)}", f"issue_comments_created_by_day.csv")


def post_process_stars_totals(context):
    folder_name = get_folder_name(context)
    if not os.path.exists(f'./{folder_name}'):
        return

    df = pd.concat(map(pd.read_csv, glob.glob(
        f"./{folder_name}/**/stargazers_by_day.csv", recursive=True)))

    if df.empty:
        return

    df = df.filter(items=['day', 'stars'])

    # convert string to datetime
    df['day'] = pd.to_datetime(df['day'])

    # group per day
    by_day = df.groupby(pd.Grouper(
        key='day', freq='1D')).sum()

    # calculate cumulative sum
    by_day["cum"] = by_day.cumsum(axis=0)

    storeDataFrame(
        by_day, f"./{folder_name}", "stargazers_by_day_totals.csv")


def post_process_issues_totals(context, type):
    folder_name = get_folder_name(context)
    if not os.path.exists(f'./{folder_name}'):
        return

    df = pd.concat(map(pd.read_csv, glob.glob(
        f"./{folder_name}/**/{type}_created_by_day.csv", recursive=True)))

    if df.empty:
        return

    df = df.filter(items=['created_at', 'closed', 'opened'])

    # convert string to datetime
    df['created_at'] = pd.to_datetime(df['created_at'])

    # group per day
    by_day = df.groupby(pd.Grouper(
        key='created_at', freq='1D')).sum()

    by_day["cum"] = by_day['closed'].cumsum()
    by_day["cum_opened"] = by_day['opened'].cumsum()

    storeDataFrame(
        by_day, f"./{folder_name}", f"{type}_by_day_totals.csv")


def post_process_issues_comments_totals(context):
    folder_name = get_folder_name(context)
    if not os.path.exists(f'./{folder_name}'):
        return

    df = pd.concat(map(pd.read_csv, glob.glob(
        f"./{folder_name}/**/issue_comments_created_by_day.csv", recursive=True)))

    if df.empty:
        return

    df = df.filter(items=['created_at', 'comments'])

    # convert string to datetime
    df['created_at'] = pd.to_datetime(df['created_at'])

    # group per day
    by_day = df.groupby(pd.Grouper(
        key='created_at', freq='1D')).sum()

    by_day["cum"] = by_day['comments'].cumsum()

    storeDataFrame(
        by_day, f"./{folder_name}", f"issue_comments_by_day_totals.csv")
