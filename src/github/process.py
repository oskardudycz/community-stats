import sys
from utils.github_process import *
from utils.store_data import *
from utils.github import *

RESULTS_FOLDER = "results"

###############################
# Parse args
###############################
argsCount = len(sys.argv)

if argsCount < 1:
    print("python ./github_stats.py <github org name>")
    sys.exit(1)

org = sys.argv[1]

org_context = {
    "org": org,
    "org_folder": f'{RESULTS_FOLDER}/{org}'
}
dev = {}
repo_totals = {}
repo_stats = {}

#################################
####     Post processing     ####
#################################

repos = read_from_csv(org_context, "repositories")

for index, repo in repos.iterrows():
    name = repo["name"]

    repo_context = {
        **org_context,
        "repo_name": name
    }

    repo_stats[name] = {
        "stars": repo["stargazers_count"],
        "forks": repo["forks_count"],
        "commits": 0,
        "issues": 0,
        "pull_requests": 0,
    }

    summary = read_from_csv(repo_context, "summary")
    repo_stats[name]["created_at"] = (
        not summary.empty) and summary.iloc[0]["created_at"] or "unknown"

    repo_stats[name]["releases"] = len(
        read_from_csv(repo_context, "releases").index)
    repo_stats[name]["issues"] = len(
        read_from_csv(repo_context, "issues").index)
    repo_stats[name]['pull_requests'] = len(
        read_from_csv(repo_context, "pull_requests").index)
    repo_stats[name]['commits'] = len(
        read_from_csv(repo_context, "commits").index)
    repo_stats[name]['issue_comments'] = len(
        read_from_csv(repo_context, "issue_comments").index)

    post_process_stars(repo_context)
    post_process_issues(repo_context, "issues")
    post_process_issues(repo_context, "pull_requests")
    post_process_issue_comments(repo_context)

    print(
        f"{name} ⭐: {repo_stats[name]['stars']}, forks: {repo_stats[name]['forks']}, commits: {repo_stats[name]['commits']}, "
        + f"issues: {repo_stats[name]['issues']}, pull requests: {repo_stats[name]['pull_requests']}, "
        + f"releases: {repo_stats[name]['releases']}, created: {repo_stats[name]['created_at']}"
        + f"issue comments: {repo_stats[name]['issue_comments']}")

post_process_stars_totals(org_context)
post_process_issues_totals(org_context, "issues")
post_process_issues_totals(org_context, "pull_requests")
post_process_issues_comments_totals(org_context)
