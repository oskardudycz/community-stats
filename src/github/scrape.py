import os
import sys
import shutil
from utils.store_data import *
from utils.github import *
from utils.github_scrape import *
from scrape import *

RESULTS_FOLDER = "results"

###############################
# Parse args
###############################
argsCount = len(sys.argv)

if argsCount < 3:
    print("python ./github_stats.py <github org name> <username> <password> <only public repos = true>")
    sys.exit(1)

org = sys.argv[1]
username = sys.argv[2]
password = sys.argv[3]
onlyPublic = argsCount > 4 and sys.argv[4] == 'yes' or True

org_context = {
    "org": org,
    "username": username,
    "password": password,
    "org_folder": f'{RESULTS_FOLDER}/{org}'
}

folder_name = get_folder_name(org_context)

if os.path.exists(folder_name):
    shutil.rmtree(folder_name)

#################################
####   Release the Kraken!   ####
#################################

repos = get_repositories(
    org_context,
    onlyPublic
)

for index, repo in repos.iterrows():
    name = repo["name"]

    repo_context = {
        **org_context,
        "repo_name": name
    }

    get_summary(repo_context)
    get_stargazers(repo_context)
    get_issues(repo_context)
    get_releases(repo_context)
    get_commits(repo_context)
    get_contributions_stats(repo_context)
    get_issue_comments(repo_context)
