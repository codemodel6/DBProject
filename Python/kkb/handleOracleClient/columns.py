def handleColumns(i):
    # CREATE문 제거
    TableData = []
    data = i[2:]
    target = "CONSTRAINT"

    # 첫번째 문자의 ( 와 ENABLE 제거하는 코드
    firstData = data[0][4:].replace("ENABLE", "")
    TableData.append(firstData)
    
    # 2번째 문자부터 CONSTRAINT 문자를 발견하기 전까지 실행되는 코드
    for idx in range(len(data)):
        # CONSTRAINT를 찾으면 for문 중단
        if data[idx+1].find(target) >= 0:
            break

        # ENABEL 제거하는 코드
        restData = data[idx+1].replace("ENABLE", "")
        TableData.append(restData)

    
    # 컬럼 리스트로 딕셔너리 생성
    for j in  TableData:
        j = j[:-2]
        print(j)
        # a = j[:-1].replace("\t", "").split(" ")
        # print(a)
    print()