# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

import json

import pandas as pd

from sqlalchemy import create_engine

# -----

with open("Result_File_1.json", "r", encoding='utf-8') as Json_Result:
    JSON_Obj = json.load(Json_Result)

USERNAME = 'ETL_ADMIN'

TypeDict = {'RAW':'BIT',
           'BLOB':'BLOB',
           'CHAR':'CHAR',
           'DATE':'DATETIME',
           'FLOAT':'DOUBLE',
           'VARCHAR':'VARCHAR',
            'VARCHAR2':'VARCHAR',
           'CLOB':'VARCHAR',
           'NUMBER':'BIGINT'}

ENGINETYPE = 'INNODB'
CHARSET = 'UTF8'

# -----

# +
# SQL 리스트를 작성할 txt 파일
SQLFile = open("SQL_List.txt", 'w',encoding='utf-8')

# 테이블 리스트 가져오기
MyTables = JSON_Obj['TableData']['TableInfo']['ETL_ADMIN']
TableList = list(MyTables)

# CREATE 문 작성
for item in TableList:
    CreationSQL = f'CREATE TABLE {item} ('
    count = 0
    ColNames = list(MyTables[item]['Columns'])
    
    PKCols = list(MyTables[item]['PKInfo'])
    #print(PKCols)
    
    for ColName in ColNames:
        ColDetail = MyTables[item]['Columns'][ColName]
        
        # 데이터타입 NUMBER (10,5) 같이 길이가 2 이상이면 FLOAT로 처리
        if len(ColDetail[1])>1:
            CreationSQL += f'{ColName} FLOAT({int(ColDetail[1][0][0])})'
        else:
            CreationSQL += f'{ColName} {TypeDict[ColDetail[0]]}'
            if ColDetail[1] != '':
                CreationSQL += f'({int(ColDetail[1][0])})'
        count+= 1
        
        # PK 제약조건 추가
        if count == len(ColNames):
            # PK 컬럼 길이가 1 이상이라면
            if len(PKCols)>0:
                # SQL에 PK 제약조건을 명시한다.
                CreationSQL += f' CONSTRAINT {item}_PK PRIMARY KEY('
                count_pkc = 0
                for PKColName in PKCols:
                    CreationSQL += f'{PKColName}'
                    count_pkc += 1
                    if count_pkc == len(PKCols):
                        CreationSQL += ')'
                    else:
                        CreationSQL += ', '
            else:
                CreationSQL += ')'
        # 아니라면 쉼표 추가
        else:
            CreationSQL += ', '
            
    # 엔진 타입 추가
    CreationSQL += f' ENGINE={ENGINETYPE}'
    
    # CHARSET 추가
    CreationSQL += f' CHARSET={CHARSET}'
    
    # 끝맺음표 추가
    CreationSQL += ';'
    
    # SQL 문 출력
    print(CreationSQL)
    
    # 파일에 쓰기
    SQLFile.write(CreationSQL)
    SQLFile.write('\n')
# -
MyTables = JSON_Obj['TableData']['TableInfo']['ETL_ADMIN']
TableList = list(MyTables)
TableList

# +
SQLFile.write('-'*30)
# ALTER 문
for item in TableList:
    pass
    
# 파일 닫기
SQLFile.close()
# -
RealInfoDataTables = list(JSON_Obj['RealData']['ETL_ADMIN'])
Datas = []
for TableName in RealInfoDataTables:
    Datas.append(JSON_Obj['RealData']['ETL_ADMIN'][TableName])

password = 'hnw123'
DBName = 'TESTTABLEUNIVERSE'

DATABASE = f'mariadb+pymysql://ETL_db:{password}@192.168.50.195:3306/testtableuniverse'
engine = create_engine(DATABASE)

conn = engine.connect()

for i in range(len(Datas)):
    DictDF = pd.DataFrame.from_dict(Datas[i])
    try:
        DictDF.to_sql(f'{RealInfoDataTables[i]}',con=engine, if_exists='replace', index=False)
    except Exception as e:
        TableStatus = pd.read_sql(f'SELECT * FROM {RealInfoDataTables[i]}', con=engine)
        if DictDF.equals(TableStatus)==False:
            print('>>> INSERTING...')
            try:
                DictDF.to_sql(f'{RealInfoDataTables[i]}',con=engine, if_exists='replace', index=False)
            except Exception as e:
                print(f'>>> ERR : 데이터프레임 {RealInfoDataTables[i]}를 업로드하지 못했습니다:')
                print(f'사유: {e}')
                continue
            print('-'*30)
        else:
            print('>>> ERR : 해당 자료는 최신입니다.')
            print('-'*30)




