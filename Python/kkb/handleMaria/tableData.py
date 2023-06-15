from handleMaria.alterTable import handleAlterData
from handleMaria.createTable import handleCreateTable
from handleMaria.realData import handleRealData


def handleTableData(data,realDataKeys):
    OWNER = "ETL_ADMIN"
    # User_Info 가져오기(테이블 이름)
    User_Info = list(data['TableData']["TableInfo"][OWNER].keys())

    # CREATE문 + Comment 만들기
    createTableData = handleCreateTable(data, User_Info, OWNER)

    # ALTER문 만들기
    alterTableData = handleAlterData(data, User_Info, OWNER)

    realData = handleRealData(data, realDataKeys)

    # 최종 결과물
    scriptTable = f"{createTableData}\n{realData}\n{alterTableData}"


    return scriptTable
