from handleOracleClient.oracleTableFunctionList import splitData,TableData,TableInfo

def oracleTableFunctions(serverTable):
    # 데이터 자르는 함수 실행
    tableValue = {}
    SplitData = splitData(serverTable)

    # 테이블 데이터 가져오는 함수 실행
    # owner와 name 가져오는 함수
    TableInfo = TableData(SplitData)
    
    # 세부 내용 가져오는 함수
    # RealData = TableInfo(SplitData)
    
    # 각각의 값을 딕셔너리에 넣어줌
    tableValue['TableInfo'] = TableInfo
    # tableValue['RealData'] = RealData

    return tableValue
