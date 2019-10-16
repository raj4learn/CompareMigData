# xlsx processer
# This program will be used to process the xlsx file.
from openpyxl import load_workbook
import pyexcelerate as pyxl

# fnReadQuires
# Purpose: This function will have the details of the Mapping sheet content.read_Quires
# With this we will be creating the Source and Destination instance Query
# l_key will help to mark the Key columns, for the comparision
def fnReadQuires(p_fileName = "CompareQuiresList.xlsx", p_sheetName = "TableList", p_Header = 1):
    one_table_set_d = dict()
    print(f"read_Quires: Read excel file: {p_fileName}:{p_sheetName}")
    try:
        wb = load_workbook(p_fileName, read_only=True, data_only=False)
        ws = wb[p_sheetName]
        l_max_row = ws.max_row
        l_max_col = ws.max_column
        #print(f"Row:{l_max_row} and Col:{l_max_col}")

        if l_max_row <= 0 or l_max_col <=0:
            return dict()

        l_previous_rec = ""
        for r in range(p_Header + 1, l_max_row + 1):
            col_header_l = list()
            for c in range(1, l_max_col + 1):
                cell_obj = ws.cell(row=r, column=c)
                col_header_l.append(cell_obj.value)

            if l_previous_rec != col_header_l[0]:
                if one_table_set_d.__len__() > 0:
                    #print(f"Return Final Table Records In : {one_table_set_d}" )
                    yield one_table_set_d
                    one_table_set_d.clear()
                    #print(f"Alter yield current Data : {one_table_set_d}-{l_previous_rec}~{col_header_l[0]}")
                l_previous_rec = col_header_l[0]

            one_table_set_d[r] = col_header_l
            #print(f"Stored {r}:{col_header_l}")

    except Exception as e:
        print(f"Error in Reading Input Compare Quires List Sheet")
        print(f"{e}")
    else:
        #print(f"Return Final Table Records Out : {one_table_set_d}")
        yield one_table_set_d
    finally:
        print(f"Finally Closing WorkBook...")
        wb.close()


# fnBuildQuery
# This function help to Create a query from the Excel sheet shared data.
# Function returns Source and Destination
def fnBuildQuery(p_tblDataFromXlsxD):
    l_s_Key_col = l_s_col_nm = l_s_tbl_nm = ""
    l_d_tbl_nm = l_d_col_nm = l_d_Key_col = ""

    for lk, lv in p_tblDataFromXlsxD.items():
        # Source Table Name
        l_s_tbl_nm = lv[0] if (lv[0] is not None and l_s_tbl_nm == "") else l_s_tbl_nm
        # Destination Table Name
        l_d_tbl_nm = lv[3] if (lv[3] is not None and l_d_tbl_nm == "") else l_d_tbl_nm

        # Source Column Names
        if lv[1] is not None:
            l_s_col_nm = l_s_col_nm + lv[1] if (l_s_col_nm == "") else (l_s_col_nm + "," + lv[1])
        # Destination Column Names
        if lv[4] is not None:
            l_d_col_nm = l_d_col_nm + lv[4] if (l_d_col_nm == "") else (l_d_col_nm + "," + lv[4])

        # Source Key
        if str(lv[2]).upper() == 'YES':
            l_s_Key_col = l_s_Key_col + lv[1] if (l_s_Key_col == "") else (l_s_Key_col + "," + lv[1])

        # Destination Key
        if str(lv[5]).upper() == 'YES':
            l_d_Key_col = l_d_Key_col + lv[4] if (l_d_Key_col == "") else (l_d_Key_col + "," + lv[4])

    #print(f"fnBuildQuery Source Query: {l_s_tbl_nm}:{l_s_col_nm}:{l_s_Key_col}")
    #print(f"fnBuildQuery Destination Query: {l_d_tbl_nm}:{l_d_col_nm}:{l_d_Key_col}")
    l_s_sql = "SELECT " + l_s_col_nm + " FROM " + l_s_tbl_nm
    l_d_sql = "SELECT " + l_d_col_nm + " FROM " + l_d_tbl_nm

    return l_s_sql, l_d_sql, l_s_Key_col, l_d_Key_col


# This main function which call two method in this file.
def read_quires(p_excel_input_file, p_sheet_name, p_header_row):
    for l_key in fnReadQuires(p_excel_input_file, p_sheet_name, p_header_row):
        lSrcQuery, lDestQuery, lSrcKeys, lDestKeys = fnBuildQuery(l_key)
        #print(f"Main: Source Query: {lSrcQuery}:{lSrcKeys}")
        #print(f"Main: Destination Query: {lDestQuery}:{lDestKeys}")
        #print("******************************************")
        yield lSrcQuery, lDestQuery, lSrcKeys, lDestKeys

################################################
# Tester
if __name__ == "__main__":
    read_quires("CompareQuiresList.xlsx", "TableList", 1)

################## End of File #################

