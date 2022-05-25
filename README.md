# Community Stats

## Scrape GitHub data

1. Get and install Python: https://www.python.org/downloads/

2. Go to `src/github` folder

```shell
cd ./src/github
```

3. Install dependencies

```shell
python -m pip install -r requirements.txt
```

3. To scrape data run:

```shell
python ./scrape.py <github org name> <username> <password> <only public repos = true>
```

**WARNING!:** Running scraper will erase previous results

4. To process scraped data run:

```shell
python ./process.py <github org name>
```