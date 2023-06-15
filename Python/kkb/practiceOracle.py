import cx_Oracle
import json

# 오라클 데이터베이스에 연결합니다.
conn = cx_Oracle.connect('rudqq/1234@localhost:1521/XE')

# 커서를 생성합니다.
cursor = conn.cursor()

# SQL 쿼리를 실행합니다.
cursor.execute("SELECT table_name FROM user_tables")

# 결과를 가져옵니다.
result = cursor.fetchall()

# 결과를 출력합니다.
print(result)

# result 리스트 안에서 테이블명 추출
tableNameList = []
for i in result :
    tableNameList.append(i[0])

print("tableNameList : ", tableNameList)

# 각 테이블의 생성 정보를 딕셔너리로 정리

# tableInfoDic = {}
# for i in tableNameList :
#     print(i)
#     cursor.execute(f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{i}') FROM DUAL")
#     tableInfo = cursor.fetchall()
#     readInfo = ''
#     for j in tableInfo:
#         readInfo += j[0].read()
#     tableInfoDic[i] = readInfo
#
# jsonTableInfoDic = json.dumps(tableInfoDic)
# print("tableInfoDic : ",jsonTableInfoDic)


tableInfoDic = []
for i in tableNameList :
    print(i)
    cursor.execute(f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{i}') FROM DUAL")
    tableInfo = cursor.fetchall()
    readInfo = ''
    for j in tableInfo:
        tableInfoDic.append(j)

print("tableInfo : ",tableInfoDic)


# 파일 열기
# with open('result.txt', 'w') as f:
#     # 결과값을 파일에 작성
#     for key, value in tableInfoDic.items():
#         f.write(key + ': ' + str(value) + '\n')


# 테이블과 그 값의 정보들을 딕셔너리로 정리

classfication = 0
tableList = []
for i in tableNameList :
    classficationList = []  # 만들어진 내용을 묶을 리스트
    classficationDic = {}  # 테이블의 컬럼과 내용을 구분시킬 딕셔너리
    cursor.execute(f"SELECT * FROM {i}")
    tableValue = cursor.fetchall()
    print("tableValue : ", tableValue)
    cursor.execute(f"SELECT COLUMN_NAME FROM COLS WHERE TABLE_NAME = '{i}'")
    tableKey = cursor.fetchall()
    print("tableKey : ", tableKey)

    count = 0


    for k in tableValue:
        tableContentDic = {} # 각 행의 데이터를 담을 딕셔너리


        print("k : ", k)
        newArr = []
        for j in range(len(tableKey)):
            print("j : ", j)
            print("tableKey : ", tableKey[j][0])
            print("tableValue : ", tableValue[count][j])
            tableContentDic[tableKey[j][0]] = tableValue[count][j]
            print("tableContentDic : ", tableContentDic)
        newArr.append(tableContentDic)
        classficationList.append(tableContentDic)
        print("classficationList : ", classficationList)
        count+=1


    classficationDic[tableNameList[classfication]] = classficationList
    print("classficationDic : ", classficationDic)
    tableList.append(classficationDic)
    classfication+=1
    print("classfication : ",classfication)
    # tableList.append


print("tableList : ",tableList)


# 커서와 연결을 닫습니다.
cursor.close()
conn.close()

# 처음에는 리스트로 테이블 모두 넣고
# 두번째는 딕셔너리로 key value 값으로 넣는다.