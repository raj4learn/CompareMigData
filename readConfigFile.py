# read config file and return as Dictionary
import global_functions as gf

f_nm = ""

def read_config_file(filename):
    m_nm = f_nm + "-"
    lConfigD = dict()
    try:
        fn = open(filename, "r")

        if fn.mode == 'r':
            for line in fn:
                ln = line.strip(' \t\n\r')
                if (len(ln) > 0):
                    if ln[0] == '#' or len(ln) == 0:
                        pass
                    else:
                        # print('Current Line:~'+str(ln)+"~")
                        # print("Current Line:~" + str(ln)+"~")
                        (key, val) = ln.split("=", 1)
                        nkey = str(key).strip()
                        nval = str(val).strip()
                        lConfigD[nkey] = nval

        fn.close()
    except:
        print("Unable to Read the Config File")
        gf.conform_exit()

    return lConfigD


def init(conf_file_name):
    m_nm = f_nm + "-"
    l_config_d = dict(read_config_file(conf_file_name))
    print("Config File Content" + str(l_config_d))
    print("**** End of Config Read *****\n")
    return l_config_d


if __name__ == "__main__":
    conf_file_name = ".\\xlsx_key_col_mapping.conf"
    init(conf_file_name)
