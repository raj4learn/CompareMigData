import openpyxl as xl
import os

def validate_file_xlsx(p_file_name):
    try:
        if p_file_name[-5:] == '.xlsx':
            if os.path.isfile(p_file_name) == True:
                return True

        print(f"Xlsx File {p_file_name} is not available")
        return False
    except:
        return False


def create_work_book(p_xlsx_file_name, p_dest_out_file_sheet = "Mapping", p_columns_l = ""):
    try:
        wb = xl.Workbook()
        sh = wb.active
        cidx = 1
        for li in p_columns_l:
            sh.cell(row= 1, column = cidx).value = li
            cidx += 1

        sh.title = p_dest_out_file_sheet
        wb.save(p_xlsx_file_name)
    except Exception as e:
        print("Error in creating excel file "+ str(e))
        return False
    else:
        return True
    finally:
        wb.close()


def conform_exit(p_log = 0):
    if p_log == 1:
        print(f"Please provide feedback by writing to rajkumar.oppilamani@metricstream.com")
    print("Program Exiting")
    exit(0)

