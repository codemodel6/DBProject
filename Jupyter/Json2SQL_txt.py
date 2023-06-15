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

# # IMPORT

import json

import pandas as pd

# -----

# # OPEN FILE

with open("Result_File.json", "r", encoding='utf-8') as Json_Result:
    JSON_Obj = json.load(Json_Result)

USERNAME = 'ETL_ADMIN'

global TypeDict
TypeDict={
    'RAW':'BIT',
    'BLOB':'BLOB',
    'CHAR':'CHAR',
    'DATE':'DATETIME',
    'FLOAT':'DOUBLE',
    'VARCHAR':'VARCHAR',
    'VARCHAR2':'VARCHAR',
    'CLOB':'VARCHAR',
    'NUMBER':'BIGINT'
}

ENGINETYPE = 'INNODB'
CHARSET = 'UTF8'

# -----

# # CREATE SQL TXT

# SQL 리스트를 작성할 txt 파일
global SQLFile
SQLFile = open("SQL_List.txt", 'w',encoding='utf-8')
# Create 문

# -----

# # CREATE문 생성기

def CREATE_TABLE_SQL_GENERATOR(TableLists, TableName):
    MyTables = TableLists
    item = TableName
    
    # Table 생성 SQL
    CreationSQL = f'CREATE TABLE {item.lower()} (\n'
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
        if ColDetail[2] != '':
            CreationSQL += f' {ColDetail[2]}'
        count+= 1
        
        # PK 제약조건 추가
        if count == len(ColNames):
            # PK 컬럼 길이가 1 이상이라면
            if len(PKCols)>0:
                # SQL에 PK 제약조건을 명시한다.
                CreationSQL += f'\nCONSTRAINT {item.lower()}_PK PRIMARY KEY('
                count_pkc = 0
                for PKColName in PKCols:
                    CreationSQL += f'{PKColName}'
                    count_pkc += 1
                    if count_pkc == len(PKCols):
                        CreationSQL += ')'
                    else:
                        CreationSQL += ','
                CreationSQL += ')'
            else:
                CreationSQL += ')'
        # 아니라면 쉼표 추가
        else:
            CreationSQL += ','
        CreationSQL += '\n'
            
    # 엔진 타입 추가
    CreationSQL += f'ENGINE={ENGINETYPE},'
    
    # CHARSET 추가
    CreationSQL += f' CHARSET={CHARSET}'
    
    # 끝맺음표 추가
    CreationSQL += ';\n'
#######################################################################
    # SQL 문 출력
    print(CreationSQL)
#######################################################################
    # 파일에 쓰기
    SQLFile.write(CreationSQL)
    SQLFile.write('\n')


# -----

# # ALTER 문 생성기

# +
# ALTER 문
def ALTER_TABLE_SQL_GENERATOR(TableLists, TableName):
    MyTables = TableLists
    item = TableName

    count = 0
    FKR_Names = list(MyTables[item]['FKInfo'])
    for FKR_Name in FKR_Names:
        FKData = MyTables[item]['FKInfo'][FKR_Name]
        FKR_Cols = list(FKData['Columns'])
        O_Schema = FKData['Original Schema']
        O_Table = FKData['Original Table']
        O_Cols = list(FKData['Original Column'])
        
        AlterSQL = f'ALTER TABLE {item.lower()}\n'
        AlterSQL += f'ADD CONSTRAINT {FKR_Name}\n'
        AlterSQL += f'FOREIGN KEY ('
        
        for i in range(len(FKR_Cols)):
            AlterSQL += f'{FKR_Cols[i]}'
            if i == len(FKR_Cols)-1:
                AlterSQL += ')'
            else:
                AlterSQL += ', '
                
        AlterSQL += f' REFERENCES {O_Schema}.{O_Table} ('
        for i in range(len(O_Cols)):
            AlterSQL += f'{O_Cols[i]}'
            if i == len(O_Cols)-1:
                AlterSQL += ')'
            else:
                AlterSQL += ', '
        AlterSQL += ';\n'
        
#######################################################################
    # SQL 문 출력
    print(AlterSQL)
#######################################################################
    # 파일에 쓰기
    SQLFile.write(AlterSQL)
    SQLFile.write('\n')


# -
# -----

# # 작성 메인

# +
# 테이블 리스트 가져오기
MyTables = JSON_Obj['TableData']['TableInfo']['ETL_ADMIN']
TableList = list(MyTables)
#######################################################################
# 각 테이블 당
for item in TableList:
#######################################################################
    CREATE_TABLE_SQL_GENERATOR(MyTables, item)
    if MyTables[item]['FKInfo'] != {}:
        ALTER_TABLE_SQL_GENERATOR(MyTables, item)

    
# 파일 닫기
SQLFile.close()
