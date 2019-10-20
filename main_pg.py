# Main Program
# Date 2019 10 16 - Commit
import argparse
import datetime as dt
from log import logErr
from global_functions import conform_exit, validate_file_xlsx, create_work_book
from os import environ
import traceback as err
from xlsx_processer import read_quires, read_proc_filter
from readConfigFile import read_config_file
from DBProcesser import DBProcesser_main, getConnection
import signal, os
from pathlib import Path

g_config_d = dict()


def signal_handler(signum, frame):
    print(f"Signal handler called with signal {signum}")
    raise IOError("Program interrupted!")


# Main program Starts Here
if __name__ == "__main__":
    print(f"Program Start")
    x_now_dt = dt.datetime.now().strftime("%y%d%m%H%M%S%f%Z")
    x_now_d = dt.datetime.now().strftime("%y%d%m")
    x_now_t = dt.datetime.now().strftime("%H%M%S%f%Z")
    l_module_arg, l_form_arg, l_table_arg = None, None, None
    # -----------------------
    # Handelling signals
    # -----------------------
    signal.signal(signal.SIGINT, signal_handler)
    # -----------------------
    # Reading Command Parser
    # -----------------------
    try:  # Parsing the Input Options (args[])
        cmd_args = argparse.ArgumentParser()
        cmd_args.add_argument('-c', "--configfile", default="compare_table_key.conf",
                              help="Pass the Configuration fileName available in Current Dir " \
                                   " below are Key Value must be available in Configuration file " \
                                   "OUTPUT_FILE_NAME_PREFIX = <filenamePrefix> "
                                   "SRC_DB_URL =  <schemaname/password@hostname:port/sid>"
                                   "DEST_DB_URL = <schemaname/password@hostname:port/sid>"
                                   "ORACLE_HOME = <path of the orable_home>"
                                   "PATH = <path of the orable client>")
        cmd_args.add_argument('-m', "--module", help="Enter the Module Code" \
                                                     "Ex: "
                                                     "GRC, ISM, CMP, etc...")

        cmd_args.add_argument('-f', "--form", help="Enter the form code/name" \
                                                     "Ex: "
                                                     "MS_GRC_CONTROL, MS_GRC_RISK, etc...")

        cmd_args.add_argument('-t', "--tabel", help="Enter the table name" \
                                                     "Ex: "
                                                     "SI_METRICS_T, SI_METRIC_COLUMNS, etc...")

        args = cmd_args.parse_args()
        # Recording the Input options passed
        l_module_arg = args.module if args.module is not None else "ALL"
        l_form_arg = args.form if args.form is not None else "ALL"
        l_table_arg = args.tabel if args.tabel is not None else "ALL"

        print("main: Configuration File: %s" % (args.configfile))
        print("main: Module Code    : %s" % (args.module))
        print("main: Form Code/Name : %s" % (args.form))
        print("main: Table Name     : %s" % (args.tabel))
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
        xlsx_file_name = g_config_d['OUTPUT_FILE_NAME_PREFIX'] + '_' + x_now_t + ".xlsx"

        l_input_master_file = g_config_d['INPUT_MASTER_FILE']
        l_master_sh = g_config_d['MASTER_SHEET_NAME']
        l_table_col_sh = g_config_d['TABLE_COL_SHEET_NAME']
    except:
        print(f"Error in getting Configuration prameter SRC_DB_URL, DEST_DB_URL, OUTPUT_FILE_NAME_PREFIX")
        conform_exit(0)

    try:
        s_con, d_con = None, None
        if not validate_file_xlsx(xlsx_file_name):
            if not create_work_book(xlsx_file_name):
                print(f"Unable to Create a Output File [{xlsx_file_name}]")
                exit(0)
            else:
                print(f"Output File Created [{xlsx_file_name}]")

        input_xlsx_file_name = Path(xlsx_file_name).resolve()
        l_tables = read_proc_filter(l_input_master_file, l_master_sh, 1, l_module_arg, l_form_arg, l_table_arg)
        print(f"{l_tables}")

        s_con, d_con = getConnection(src_dburl, dest_dburl)
        print(f"Main: Connection obtained")

        for l_s_tbl, l_d_tbl, lSrcQuery, lDestQuery, lSrcKeys, lDestKeys in read_quires(l_input_master_file, l_table_col_sh, 1):
            # print(f"****Main: {lSrcQuery} - {lDestQuery} - {lSrcKeys} - {lDestKeys}")
            if l_s_tbl in l_tables:
                DBProcesser_main(s_con, d_con, lSrcQuery, lDestQuery, l_s_tbl, l_d_tbl, lSrcKeys, lDestKeys, input_xlsx_file_name)
            else:
                print(f"Main: Table Name {l_s_tbl}, ignored")
    except KeyboardInterrupt:
        print(f"Exception: {err.format_exc()}")
    except:
        print(f"Error In main: {err.format_exc()}")
    finally:
        if s_con is not None:
            s_con.close()
            print(f"Main: Src Connection Closed")

        if d_con is not None:
            d_con.close()
            print(f"Main: Destination Connection Closed")

