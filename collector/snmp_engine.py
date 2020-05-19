import fcntl
from time import sleep

'''Module to move the provided dictionary to csv'''


def save_to_snmp(args,filename="/root/PycharmProjects/collector/testfile.csv",lock_wait_time=0.05,retry_no=10000):
    # function to save to file to CSV in a given location
    for _ in range(retry_no):
        try:
            with open(filename,"a") as file:
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)  # locking the file
                file.write("{},{},{}\n".format(args["timestamp"],args["ip address"], args["engine_ID"]))  # writing to the csv
                sleep((2))
                # fcntl.flock(file, fcntl.LOCK_UN)  # unlocking the file
                break
        except:
            sleep(lock_wait_time)









arg = {"device name":"cisco",
       "timestamp":"51",
       "device type":"router",
       "ip address":"10.3.121.1",
       "engine_ID":"000008345678"}
save_to_snmp(arg)