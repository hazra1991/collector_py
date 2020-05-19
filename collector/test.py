import pandas as pd
import fcntl,subprocess,re,json,time
import os

source_file = "/root/PycharmProjects/collector/testfile.csv"
engine_id =  "/opt/sevone-uc/NagiosSecurityCollector/outboundengineid/engine_id.txt"

with open(source_file, "r+") as file:
    fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)  # locking the file
    csv_df = pd.read_csv(file, header=None, names=["time", "ip address", "engine ID"],
                         converters={"engine ID": lambda x: str(x)})  # creating DataFrame to work with

    csv_df = csv_df.sort_values(["time"])
    print(csv_df)
    csv_df.drop_duplicates(subset="ip address", keep="last", inplace=True)
    print(csv_df)
    # searching and removing the old duplicates
    del csv_df["time"]
    csv_df.to_csv(engine_id, index=False)
    file.truncate(0)
    fcntl.flock(file, fcntl.LOCK_UN)  # unlocking the file

# with open(source_file, "r+") as file:
#     fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
#     csv_df = pd.read_csv(file, header=None, names=["time", "ip address", "engine ID"],
#                          converters={"engine ID": lambda x: str(x)})
#     csv_df = csv_df.sort_values(["time"])
#     csv_df.drop_duplicates(subset="ip address", keep="last", inplace=True)
#     del csv_df["time"]
#     csv_df.to_csv(engine_id, index=False)
#     print(csv_df)
#     file.truncate(0)
#     fcntl.flock(file, fcntl.LOCK_UN)
