# API Rate Limit - 5000 requests per authorized user per hour, but not enough)
# not authorized - 600

import aiohttp
import asyncio
from dotenv import load_dotenv
import os
import json
from pathlib import Path
import time
from datetime import datetime


load_dotenv()


GITHUB_REPOS_FILENAME = 'github_repos_names.txt'
GITHUB_ISSUES_FILENAME = 'github_issues.json'
VISITED_GITHUB_REPOS_FILENAME = 'visited_github_repos.txt'

API_LIMIT = 100
GITHUB_API_TOKEN = os.environ["GITHUB_API_TOKEN"]
GITHUB_API_URL = f"https://api.github.com"
headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GITHUB_API_TOKEN}",
    "X-GitHub-Api-Version": "2022-11-28"
}


# create following file, because json loads strange with "a" mode, so it's better to create file explicitly
file = Path(GITHUB_ISSUES_FILENAME)
file.touch(exist_ok=True)


async def fetch(url, session=None, data=None):
    async def _get(session):
        await asyncio.sleep(0.01)
        async with session.get(url) as response:
            print(f"Fetching: {url}")
            resp = await response.json()
        return url, resp, data

    if not session:
        async with aiohttp.ClientSession(headers=headers) as session:
            url, resp, data = await _get(session)
    else:
        url, resp, data = await _get(session)

    return url, resp, data


with open(GITHUB_REPOS_FILENAME, 'r') as file:
    GITHUB_REPOS = file.read().splitlines()

try:
    _, resp, _ = asyncio.run(fetch(f"{GITHUB_API_URL}/rate_limit"))
    quota = resp['resources']['core']['remaining']

    reset_timestamp = resp['resources']['core']['reset']
    reset_time = datetime.fromtimestamp(reset_timestamp)

    print(f"REMAINING QUOTA: {quota}")
    print(f"RESET TIME: {reset_time}")
    time.sleep(3)

    API_LIMIT = quota // 100 if quota > 100 else exit()

    with open(VISITED_GITHUB_REPOS_FILENAME, 'r') as file:
        VISITED_REPOS = file.read().splitlines()

        GITHUB_REPOS = list(set(GITHUB_REPOS) - set(VISITED_REPOS))[:API_LIMIT]
        if len(GITHUB_REPOS) == 0:
            print("ALL DATA SUCCESSFULLY INGESTED!")
            exit()
except FileNotFoundError:
    VISITED_REPOS = []


def generate_chunks_unvisited(visited_repos, repos, chunk_len=100):
    def append_chunk(repo, visited_repos): return chunk.append(
        repo) if repo not in visited_repos else None
    chunk = []
    for i, repo in enumerate(repos, 1):
        if i == len(repos):
            append_chunk(repo, visited_repos)
            yield chunk
            return
        if i % chunk_len == 0:
            yield chunk
            chunk = []
        append_chunk(repo, visited_repos)


def append_to_json(filename, data):
    with open(filename, 'r+', encoding='utf-8') as file:
        if os.stat(filename).st_size == 0:
            json.dump(data, file, indent=4)
        else:
            data_json = json.load(file, strict=False)
            data_json.extend(data)

            file.seek(0)
            json.dump(data_json, file, indent=4)


def append_to_file(filename, data):
    with open(filename, 'a+', encoding='utf-8') as file:
        file.write('\n') if data else None
        file.write('\n'.join(data))


async def main(visited_repos, repos):
    tasks = []

    async with aiohttp.ClientSession(headers=headers) as session:
        for repos in generate_chunks_unvisited(visited_repos, repos):
            tasks = [
                asyncio.ensure_future(
                    fetch(
                        f"{GITHUB_API_URL}/repos/{repo_url}/issues?state=closed", session)
                )
                for repo_url in repos
            ]

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        respones_cleared = []
        visited_repos_batch = set()

        for repo_url, response, data in responses:
            repo_url = '/'.join(repo_url.split('/')[-3:-1])

            if isinstance(response, dict) or not response:
                output = f"\033[96m{repo_url}" + f"\033[96m {response.get('message', response)}\033[0m" if response\
                    else f"\033[96m{repo_url}" + " No closed issues\033[0m"
                print(output)
                visited_repos_batch.add(repo_url)
                continue

            comments_tasks = []

            for resp in response:
                if "comments_url" in resp and "title" in resp and "url" in resp:

                    issue_url = resp["url"]
                    issue_title = resp["title"]

                    comments_url = resp["comments_url"]
                    comments_tasks.append(
                        asyncio.ensure_future(fetch(comments_url, session, data={
                            "issue_url": issue_url,
                            "issue_title": issue_title
                        }))
                    )

            comments_responses = await asyncio.gather(*comments_tasks, return_exceptions=True)
            comments_responses = [
                resp for resp in comments_responses if not isinstance(resp, Exception)]
            for url, response, data in comments_responses:
                if response:
                    data.update({
                        "comments": response
                    })
                    respones_cleared.append(data)
                visited_repos_batch.add(repo_url)

        append_to_json(GITHUB_ISSUES_FILENAME, respones_cleared)
        append_to_file(VISITED_GITHUB_REPOS_FILENAME, visited_repos_batch)


if __name__ == "__main__":
    asyncio.run(main(VISITED_REPOS, GITHUB_REPOS))
