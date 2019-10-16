# Main Program
# Date 2019 10 16 - Commit
import argparse
import datetime as dt
from log import logErr
from global_functions import conform_exit, validate_file_xlsx, create_work_book
from os import environ
import traceback as err
from xlsx_processer import read_quires
from readConfigFile import read_config_file
from DBProcesser import DBProcesser_main, getConnection

g_config_d = dict()

# Main program Starts Here
if __name__ == "__main__":
    print(f"Program Start")
    x_now = dt.datetime.now().strftime("%y%d%m%H%M%S%f%Z")
    # -----------------------
    # Reading Command Parser
    # -----------------------
    try: # Parsing the Input Options (args[])
        cmd_args = argparse.ArgumentParser()
        cmd_args.add_argument('-c', "--configfile", default="compare_table_key.conf",
                              help="Pass the Configuration fileName available in Current Dir " \
                                   " below are Key Value must be available in Configuration file " \
                                   "OUTPUT_FILE_NAME = <filename.xlsx> "
                                   "SRC_DB_URL =  <schemaname/password@hostname:port/sid>"
                                   "DEST_DB_URL = <schemaname/password@hostname:port/sid>"
                                   "ORACLE_HOME = <path of the orable_home>"
                                   "PATH = <path of the orable client>")
        cmd_args.add_argument('-m', "--module", help="Enter the Module Code " \
                                                     "Ex: "
                                                     "GRC, ISM, CMP, etc...")

        args = cmd_args.parse_args()
        # Recording the Input options passed
        print("main: Configuration File: %s" % (args.configfile))
        print("main: Module Code: %s" % (args.module))
    except:
        print(f"Error in Argument")
        conform_exit("")

    # ------------------------------------------------------
    # Reading Config File and Setting Environment Variables
    # ------------------------------------------------------
    # Setting the OS Environments
    try:
        g_config_d = read_config_file(args.configfile)
        environ['ORACLE_HOME'] = g_config_d['ORACLE_HOME']
        environ['LD_LIBRARY_PATH'] = g_config_d['PATH']
        environ['PATH'] = g_config_d['PATH']
    except:
        pass

    try:
        src_dburl = g_config_d['SRC_DB_URL']
        dest_dburl = g_config_d['DEST_DB_URL']
        xlsx_file_name = g_config_d['OUTPUT_FILE_NAME'] + "_" + x_now
        s_con, d_con = getConnection(src_dburl, dest_dburl)
        for lSrcQuery, lDestQuery, lSrcKeys, lDestKeys in read_quires("CompareQuiresList.xlsx", "TableList", 1):
            # print(f"****Main: {lSrcQuery} - {lDestQuery} - {lSrcKeys} - {lDestKeys}")
            DBProcesser_main(s_con, d_con, lSrcQuery, lDestQuery, lSrcKeys, lDestKeys)

    except Exception as e:
        print(f"Error In main: {e}")

    '''
        try:
            s_con = ora.connect(src_db)
            d_con = ora.connect(dest_db)
            print("Connected to DB")

            prepare_queries(s_con, d_con, xlsx_file_name, g_module)

        except Exception as e:
            print(f"Error In main: {e}")
        finally:
            if s_con:
                s_con.close()
                print("Disconnected from DB")
            if d_con:
                d_con.close()
                print("Disconnected from DB")
    '''



