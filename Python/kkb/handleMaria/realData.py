def handleRealData(data,realDataKeys):
    # 최종 쿼리문
    realDataQuery = ""

    for i in realDataKeys:
        # 테이블 이름
        tableData = list(data['RealData'][i].keys())

        for j in tableData:
            # 하나의 INSERT문
            realData = ""
            
            # INSERT문 테이블 넣어주기
            realData += f"INSERT INTO {j} "

            # 테이블의 키 값
            tableDataKeys = list(data['RealData'][i][j].keys())

            # 문장 추가
            realData += f"("

            for k in tableDataKeys:
                realData += f"{k},"
                
            # 콤마 하나 삭제 후 문장 추가
            realData = realData[:-1]
            realData += f")\nVALUES\n"

            # 실제 데이터의 개수 하나를 보면 모두를 알 수 있다
            tableRealData = data['RealData'][i][j][k]
            
            # 데이터가 없으면 INSERT문 삭제
            if(len(tableRealData) == 0):
                realData = ""
            
            # 데이터가 있으면 INSERT문 작성
            else:
                # 리스트에서 값을 꺼내오기 위한 index
                for idx in range(len(tableRealData)):

                    # 문장 추가
                    realData += "\t("
                    for k in tableDataKeys:
                        # 실제 데이터
                        tableRealData = data['RealData'][i][j][k][idx]
                        realData += f"'{tableRealData}',"

                    # 콤마 없애고 괄호 생성
                    realData = realData[:-1]
                    realData += "),\n"

                # 콤마 없애고 줄바꿈
                realData = realData[:-2]
                realData += f";\n\n"

            # 쿼리문 합침
            realDataQuery += realData






    return realDataQuery