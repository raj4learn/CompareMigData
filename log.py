import datetime as dt

def logErr(eLevel, eMesg, *p_str):
    x_now = dt.datetime.now().strftime("%y%d%m%H%M%S%f%Z")
    print(f"{x_now}:{eLevel}: {eMesg} {p_str}")

