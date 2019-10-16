import cx_Oracle as ora


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


def executeQuery_SrcDest(p_s_dbcon, p_d_dbcon, p_SrcQuery, p_DestQuery):
    print(f"executeQuery_SrcDest: Start")
    print(f"Executing Current Stmt {p_SrcQuery} {p_DestQuery}")
    pass


def DBProcesser_main(p_s_dbcon, p_d_dbcon, p_SrcQuery, p_DestQuery, p_SrcKeys, p_DestKeys):
    # Getting the Query List and Processing
    print(f"****Main: {p_SrcQuery} - {p_DestQuery} - {p_SrcKeys} - {p_DestKeys}")
    executeQuery_SrcDest(p_s_dbcon, p_d_dbcon, p_SrcQuery, p_DestQuery)

    pass


if __name__ == "__main__":
    print(f"DB Processer Starting")
    src_dburl, dest_dburl = ""
    l_SrcQuery, l_DestQuery, l_SrcKeys, l_DestKeys = ""
    s_dbcon, d_dbcon = getConnection(src_dburl, dest_dburl)
    DBProcesser_main(s_dbcon, d_dbcon, l_SrcQuery, l_DestQuery, l_SrcKeys, l_DestKeys)
