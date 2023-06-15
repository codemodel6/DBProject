from handleMaria.changeMaria import handleChangeMaria, handleChangeStrToNum
from handleMaria.comment import handleComment


def handleCreateTable(data,User_Info,i):
    createData = ""
    for j in User_Info:
        # 테이블 이름 넣어주기
        # CREATE문 이름 넣어주기 #
        createData += f"CREATE TABLE {j} (\n"

        # 컬럼들 가져오기
        Columns = list(data['TableData']["TableInfo"][i][j]['Columns'])

        for k in Columns:
            # 컬럼의 속성들을 리스트로 가져오기
            columnsInfo = data['TableData']["TableInfo"][i][j]['Columns'][k]

            # 컬럼의 속성중 이름 더해주기
            createData += f"\t{k} "

            # 컬럼의 속성중 타입과 NOT NULL 더해주기
            index = 0
            tableDataInfo = ""
            toggle = False
            for l in columnsInfo:


                # 오라클 -> 마리아 문법으로 변경
                l = handleChangeMaria(l)
                
                # DOUBLE일 경우 사이즈 제거
                if (l == "DOUBLE"):
                    toggle = True

                # list[1]의 [string] 값을 (number)값으로 바꿔줌
                if (index == 1):
                    l = handleChangeStrToNum(l,toggle)

                tableDataInfo += f"{l} "
                index += 1

            # 한개 정보 끝난 후 줄바꿈
            createData += f"{tableDataInfo},\n"

        # 마지막 쉼표 제거 후 줄바꿈
        createData = createData[:-2]
        createData += '\n'
        
        # 기본키 넣어주기
        PKQuery = "\tPRIMARY KEY "
        PK = list(data['TableData']["TableInfo"][i][j]["PKInfo"])

        # 쿼리가 없을 경우
        if((len(PK) == 0)):
            PKQuery = ""
            createData = createData[:-1]

        elif (len(PK) > 1):
            # 쉼표를 다시 만드는 작업
            createData = createData[:-1]
            createData += ",\n"

            PKQuery += "("
            # 기본키 여러개 추가
            for h in range(len(PK)):
                PKQuery+= f"{PK[h]},"

            PKQuery = PKQuery[:-1]
            PKQuery += ")"

        elif (len(PK) == 1):
            # 쉼표를 다시 만드는 작업
            createData = createData[:-1]
            createData += ",\n"
            # 기본키 추가
            PKQuery += f"({PK[0]})"
        
        # 전체 쿼리에 기본키 추가
        createData += f"{PKQuery}\n"

        # 하나 끝나고 ) 쳐주기
        createData += ");\n\n"
        
        # COMMENT 가져오는 함수 실행
        comment = handleComment(data,i,j)

        createData += f"{comment}"

    return createData
