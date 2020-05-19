import pandas as pd
import fcntl,subprocess,re,json,time
import os


class DataProcessor(object):

    """
    class having write_data() and SCP() to write and copy the specified fle
    Static method validate_file() validates the requirements before copying.
    Variables can be set during initialization : source_file=""
                                                 engine_file=""
    """

    def __init__(self, source_file, engine_file,pre_file,gzip):
        # Initialization block
        self.source_file = source_file
        self.engine_file = engine_file
        print(self.engine_file)
        self.pre_file = pre_file
        print(self.pre_file)
        self.gzip = gzip
        print(self.source_file)

    def write_data(self, flush=True):

        # Writing data to the engine_id.txt file

        if flush:
            with open(self.source_file, "r+") as file:
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)  # locking the file
                time.sleep(3)
                csv_df = pd.read_csv(file, header=None, names=["time", "ip address", "engine ID"],
                                     converters={"engine ID": lambda x: str(x)})  # creating DataFrame to work with

                csv_df = csv_df.sort_values(["time"])
                csv_df.drop_duplicates(subset="ip address", keep="last", inplace=True)
                # searching and removing the old duplicates
                del csv_df["time"]
                csv_df.to_csv(self.engine_file, index=False)
                file.truncate(0)
                fcntl.flock(file, fcntl.LOCK_UN)  # unlocking the file
        elif not flush:
            with open(self.source_file,"r") as file:
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                csv_df = pd.read_csv(file, header=None, names=["time", "ip address", "engine ID"],
                                     converters={"engine ID": lambda x: str(x)})
                csv_df = csv_df.sort_values(["time"])
                csv_df.drop_duplicates(subset="ip address", keep="last", inplace=True)
                del csv_df["time"]
                csv_df.to_csv(self.engine_file, index=False)
                fcntl.flock(file, fcntl.LOCK_UN)

    def scp(self, server_list_files, dest_path="/root/home/engine_id.txt"):
        """
        secure copy to a predefined server list
        accepts variable server_details ,port_number,dest_path
        server_details should be provided as a list of dictionaries having exact  keys "username" , "IP address" and "port_number"

        """
        print(self.pre_file)
        if self.validate_file():

            try:
                subprocess.call(["gzip",self.pre_file])
                subprocess.call(["mv",self.pre_file +".gz","{}/{}".format(os.path.dirname(self.pre_file),self.gzip)])
                subprocess.call(["cp",self.engine_file,"{}/{}-engine_id.txt.previous".format(os.path.dirname(self.engine_file),int(time.time()))])
            except:
                #need to check
                print("Something went wrong ")
            with open(server_list_files, "r") as fd:
                server_details = json.load(fd)

            for _ in server_details:
                send = subprocess.call(["scp", "-P",_["port_number"], self.engine_file,
                                        "{}@{}:{}".format(_["username"], _["IP address"], dest_path)])
                if send == 0:
                    pass
                else:
                    print("IP {} not reachable ".format(_["IP address"]))


    # need to verify the  logic
    def validate_file(self, percent=10):

        with open(self.pre_file,"r") as prev_fd, open(self.engine_file,"r") as current_fd:
            rowcount_prev = float(sum(1 for _ in prev_fd))
            rowcount_current = float(sum(1 for _ in current_fd))
            return True
            # requirment unclear
            # if rowcount_current > rowcount_prev and (rowcount_current-rowcount_prev)/rowcount_current*100 > 10.0:
            #     return False
            # else:
            #     return True


if __name__ == "__main__":

    try:
        with open("scp_engine_env_var.txt", "r") as f:
            for i in f:
                if not re.match(r"^\s*#",i):
                    ls = i.split("=")
                    os.environ[ls[0].strip()] = ls[1].strip()

        file_list = os.listdir(os.path.dirname(os.getenv("ENGINE_FILE_PATH",
                                                         "/opt/sevone-uc/NagiosSecurityCollector/outboundengineid/engine_id.txt")))
        print(file_list)
        prev_file = ""
        gip_filename =""
        for i in file_list:
            if re.match("(\d{10}-engine_id.txt.previous\s*)",i):
                gip_filename = "{}-engine_id.txt.gz".format(i[:10])
                prev_file = os.path.dirname(os.getenv("ENGINE_FILE_PATH",
                                                         "/opt/sevone-uc/NagiosSecurityCollector/outboundengineid/engine_id.txt")) +'/'+ i
                print(gip_filename,prev_file)
                break

        scr = os.getenv("SOURCE_FILE_PATH", "/root/PycharmProjects/collector/tempfile.txt")
        des = os.getenv("ENGINE_FILE_PATH", "/opt/sevone-uc/NagiosSecurityCollector/outboundengineid/engine_id.txt")

        handler = DataProcessor(scr, des, prev_file, gip_filename)
        # for _ in range(10):
        #     try:
        #         handler.write_data()
        #         break
        #     except IOError:
        #         if _ == 9:
        #             print("write failed .The file is locked :retried: ",_)
        #             exit(0)
        #         time.sleep(0.5)
        handler.scp(os.getenv("SERVER_LIST_FILE_PATH", "/root/PycharmProjects/collector/snmp_server.json"))
    # except FileNotFoundError:
    #     print("verify if the file path and directory are pre-created")
    except IndexError:
        print("[+] exception :Please check the scp_engine_env_var.txt file")