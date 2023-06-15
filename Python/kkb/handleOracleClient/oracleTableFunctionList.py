# 데이터를 하나하나 짜름
from handleOracleClient.columns import handleColumns


def splitData (data) :
    myData = []
    for i in data :
        myData.append(i.split("\n"))
    return myData


# 테이블 데이터를 가져옴
def TableData(data):
    index = 0
    tableOwnerDic = {}
    tableNameDic = {}

    for i in data:
        User_Info = {}
        userData = i[1].split(" ")[4].replace('"', '')
        dotData = userData.split(".")

        # ETL_Admin
        tableOwner = dotData[0]
        
        # 테이블 명
        tableName = dotData[1]

        columns = handleColumns(i)

        # Columns : {User_ID, CREATED_DATE}"
        User_Info["Columns"] = "안녕"

        # 테이블 명 : Columns
        tableNameDic[tableName] = User_Info

        # ETL_Admin : 테이블 명
        tableOwnerDic[tableOwner] = tableNameDic

        index+=1
    return tableOwnerDic

def TableInfo(data,mariaDic):
    index = 0
    tableInfoList = []
    tableInfoName = []
    for i in data:
        userData = i[2].split(" ")[3].split('"')[1]






