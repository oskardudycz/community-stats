import os
import json


def storeDataFrame(df, path, fileName):
    if not os.path.exists(path):
        os.makedirs(path)

    df.to_csv(f"./{path}/{fileName}")


def storeResults(obj, path, fileName):
    if not os.path.exists(path):
        os.makedirs(path)

    with open(f"./{path}/{fileName}", "w") as outfile:
        json.dump(obj, outfile)
