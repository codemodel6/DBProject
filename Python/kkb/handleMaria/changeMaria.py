# 오라클 -> 마리아
def handleChangeMaria(l):
    if(l == "VARCHAR2"):
        l = "VARCHAR"
    elif(l == "NUMBER"):
        l = "INT"
    elif(l == "RAW"):
        l = "BIT"
    elif (l == "DATE"):
        l = "DATETIME"
    elif (l == "FLOAT"):
        l = "DOUBLE"
    elif (l == "CLOB"):
        l = "VARCHAR"
    return l

# [] -> ()
def handleChangeStrToNum(l,toggle):
    if(len(l) == 0 or toggle):
        return ""
    l = f"({int(l[0])})"
    return l
