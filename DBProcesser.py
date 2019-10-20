import cx_Oracle as ora
from xlsx_processer import write2Xlsx
import traceback as err
from global_functions import conform_exit


def getConnection(p_src_dburl, p_dest_dburl):
    try:
        print(f"DB Connection Start")
        ls_con = ora.connect(p_src_dburl)
        ld_con = ora.connect(p_dest_dburl)
        # con = ora.connect('METRICSTREAM', 'password', '10.101.1.16:32941/orcl:pooled', cclass = "HOL", purity = ora.ATTR_PURITY_SELF)
    except Exception:
        print(f"Error: Getting DB Connection error for {p_src_dburl} {p_dest_dburl}")
    else:
        print(f"DB Connection Completed")
        return ls_con, ld_con


def executeQuery(p_dbcon, p_Query):
    print(f"executeQuery: Start")
    print(f"executeQuery: Stmt {p_Query}")
    try:
        l_DataExists = True
        l_cur = p_dbcon.cursor()
        l_cur.arraysize = 500
        l_cur.execute(p_Query)

        # only print head
        l_title = [i[0] for i in l_cur.description]
        l_columns = [tuple(l_title),]
        #print (f"Columns {l_columns}")
        yield l_columns

        while l_DataExists:
            l_rs = l_cur.fetchmany(numRows=10)
            if l_rs.__len__() <= 0:
                l_DataExists = False
            else:
                #print(f"Type: {type(l_rs)}")
                yield l_rs
    except:
        print(f"Error: Conn [{p_dbcon}]\nQuery [{p_Query}]\n{err.format_exc()}")
        conform_exit(0)



def DBProcesser_main(p_s_dbcon, p_d_dbcon, p_SrcQuery, p_DestQuery, p_s_tbl, p_d_tbl, p_SrcKeys, p_DestKeys, p_xlsx_file_name):
    # Getting the Query List and Processing
    l_get_data_flag = True
    l_header = ""
    s_db_result, d_db_result = list(), list()
    print(f"**************\nFile:{p_xlsx_file_name}\nSrc :[{p_SrcKeys}:{p_SrcQuery}]\nDest: [{p_DestKeys}:{p_DestQuery}]")
    print(f"Source DB Fetch")
    for lIdx, s_db_result in enumerate(executeQuery(p_s_dbcon, p_SrcQuery)):
        #print(f"\nS:{lIdx}:{p_s_tbl}={s_db_result}")
        l_ret = write2Xlsx("SRC", p_s_tbl, l_header, p_SrcKeys,  s_db_result, p_xlsx_file_name)
        if (l_ret == False):
            print(f"DBProcesser_main: Source Return %s", "Failed")
        #print(f"Return DBProcessError In mainer_main:{l_rel}")

    print(f"Destination DB Fetch")
    for lIdx, d_db_result in enumerate(executeQuery(p_d_dbcon, p_DestQuery)):
        #print(f"\nD:{lIdx}:{p_d_tbl}={d_db_result}")
        l_ret = write2Xlsx("DEST",p_d_tbl, l_header, p_DestKeys,  d_db_result, p_xlsx_file_name)
        if (l_ret == False):
            print(f"DBProcesser_main: Destination Return %s", "Failed")
        # print(f"Return DBProcessError In mainer_main:{l_rel}")



if __name__ == "__main__":
    print(f"DB Processer Starting")
    src_dburl, dest_dburl = ""
    l_SrcQuery, l_DestQuery, l_SrcKeys, l_DestKeys = ""
    s_dbcon, d_dbcon = getConnection(src_dburl, dest_dburl)
    DBProcesser_main(s_dbcon, d_dbcon, l_SrcQuery, l_DestQuery, l_SrcKeys, l_DestKeys)
