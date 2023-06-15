# 오라클DB로부터 테이블을 가져오는 함수
def getOracleData(cursor, query):
    cursor.execute(query)

    # 결과를 가져옵니다.
    result = cursor.fetchall()

    # result 리스트 안에서 테이블명 추출
    tableNameList = []
    for i in result:
        tableNameList.append(i[0])

    # 각 테이블의 생성 정보를 딕셔너리로 정리

    tableInfoDic = []
    for i in tableNameList:
        cursor.execute(f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{i}') FROM DUAL")
        tableInfo = cursor.fetchall()
        readInfo = ''
        for j in tableInfo:
            readInfo += j[0].read()
        tableInfoDic.append(readInfo)

    ######### 딕셔너리
    # tableInfoDic = []
    # cursor.execute(f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{i}') FROM DUAL")
    # tableInfo = cursor.fetchall()
    # readInfo = ''
    # for i in tableInfo:
    #     readInfo.append(i)
    ##############

    # print("123 : ",tableInfoDic)
    return tableInfoDic

# 오라클DB로부터 인덱스를 가져오는 함수
def getOracleIndex(cursor, query):
    cursor.execute(query)

    # 결과를 가져옵니다.
    result = cursor.fetchall()

    tableInfoList = []
    for i in result:
        cursor.execute(f"SELECT dbms_metadata.get_ddl('INDEX', '{i[0]}') FROM dual")
        getTableList = cursor.fetchall()
        ddl = getTableList[0][0].read()
        tableInfoList.append(ddl)

    return tableInfoList


# 오라클DB로부터 테이블 내용(content)을 가져오는 코드
def getOracleContent(cursor, query):
    cursor.execute(query)
    result = cursor.fetchall()
    tableList = []

    classfication = 0
    tableNameList = []
    for i in result:
        tableNameList.append(i[0])

    for i in tableNameList:
        classficationList = []  # 만들어진 내용을 묶을 리스트
        classficationDic = {}  # 테이블의 컬럼과 내용을 구분시킬 딕셔너리
        sql=f'SELECT * FROM "{i}"'
        cursor.execute(sql)
        tableValue = cursor.fetchall()
        cursor.execute(f"SELECT COLUMN_NAME FROM COLS WHERE TABLE_NAME = '{i}'")
        tableKey = cursor.fetchall()

        count = 0

        for k in tableValue:
            tableContentDic = {}  # 각 행의 데이터를 담을 딕셔너리

            newArr = []
            for j in range(len(tableKey)):
                tableContentDic[tableKey[j][0]] = tableValue[count][j]
            newArr.append(tableContentDic)
            classficationList.append(tableContentDic)
            count += 1

        classficationDic[tableNameList[classfication]] = classficationList
        tableList.append(classficationDic)
        classfication += 1
        tableList.append

    return tableList

def getOracleDic (table,index,content):
    oracleDic = {}
    oracleDic["table"] = table
    oracleDic["index"] = index
    oracleDic["content"] = content
    return oracleDic



