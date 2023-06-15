def handleAlterData(data,User_Info,i):
    alterDataList = ""
    for j in User_Info:
        alterData = ""
        # 외래키 키값
        FKKey = list(data['TableData']["TableInfo"][i][j]["FKInfo"])
        
        # 외래키 정보
        for k in FKKey:
            #--- 가져올 외래키 값들---
            # 제약조건 이름
            FKName = ""
            # 외래키 설정할 컬럼
            Columns = ""
            # 참조될 스키마 (ETL_ADMIN)
            OriginalSchema = ""
            # 참조될 테이블
            OriginalTable = ""
            # 참조되는 컬럼
            OriginalColumn = ""
            # 마지막 제약조건
            ETCInfo = ""


            FK = list(data['TableData']["TableInfo"][i][j]["FKInfo"][k])
            
            # 외래키의 각 값들
            for l in FK:
                if(l == 'FKName'):
                    FKName = data['TableData']["TableInfo"][i][j]["FKInfo"][k][l]
                    print("-> ",FKName)
                elif(l == 'Columns'):
                    ColumnList = data['TableData']["TableInfo"][i][j]["FKInfo"][k][l]
                    
                    # 리스트 값 꺼내고 괄호 쳐주기
                    Columns = f"("
                    for c in ColumnList:
                        Columns += f"{c},"
                    Columns = Columns[:-1]
                    Columns += ")"

                elif(l == 'Original Schema'):
                    OriginalSchema = data['TableData']["TableInfo"][i][j]["FKInfo"][k][l]
                elif(l == 'Original Table'):
                    OriginalTable = data['TableData']["TableInfo"][i][j]["FKInfo"][k][l]
                elif(l == 'Original Column'):
                    OriginalColumnList = data['TableData']["TableInfo"][i][j]["FKInfo"][k][l]

                    # 리스트 값 꺼내고 괄호 쳐주기
                    OriginalColumn = f"("
                    for c in OriginalColumnList:
                        OriginalColumn += f"{c},"
                    OriginalColumn = OriginalColumn[:-1]
                    OriginalColumn += ")"


                elif(l == 'ETC Info'):
                    ETCInfo = data['TableData']["TableInfo"][i][j]["FKInfo"][k][l]


            # 데이터 조립
            alterData = f"ALTER TABLE {j}\n" \
                        f"ADD CONSTRAINT {FKName}\n" \
                        f"FOREIGN KEY {Columns} REFERENCES {OriginalSchema}.{OriginalTable} {OriginalColumn}\n" \
                        f"{ETCInfo};\n\n"

            # 스키마가 다를 경우 주석처리
            if (OriginalSchema != i):
                alterData = alterData[:-1]
                alterData = f"/*\n{alterData}*/\n\n"

            print(alterData)
            alterDataList += alterData

    return alterDataList
