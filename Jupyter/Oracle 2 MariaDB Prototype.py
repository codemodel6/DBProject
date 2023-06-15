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

# ### pandas : 데이터프레임을 사용하기 위한 라이브러리

import pandas as pd

# ### getpass : 비밀번호를 ●으로 보이면서 입력할 수 있게 하는 라이브러리
# 장식용이다.

import getpass

# ### oracledb : Oracle DB와 연계작업을 위한 라이브러리
# 일반적인 OracleDB 작업에 사용했다.

import oracledb

# ### pymysql : MySQL을 쓰기 위한 라이브러리
# 일반적인 MariaDB 작업에 사용했다.

import pymysql

# ### sqlalchemy : 필요한 문구만 입력하면 SQL을 자동으로 완성해주는 라이브러리
# 데이터를 집어넣을때 사용했다.

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

# ### TypeDict : 타입이 저장된 Dictionary
# Oracle DB와 Maria DB 간 Type이 다르기 때문에 Oracle DB의 데이터타입에 대응하는 Maria DB의 데이터타입이 저장되어있다.

TypeDict = {'RAW':'BIT',
           'BLOB':'BLOB',
           'CHAR':'CHAR',
           'DATE':'DATETIME',
           'FLOAT':'DOUBLE',
           'VARCHAR':'VARCHAR',
            'VARCHAR2':'VARCHAR',
           'CLOB':'VARCHAR',
           'NUMBER':'BIGINT'}

# ### Pandas 옵션 (주석 처리됨)
# Pandas의 DataFrame이 표시되는 최대 컬럼 수의 제한을 없앴다.

pd.set_option('display.max_columns', None)

# -------------

# # Oracle DB와 연결

# ## 인스턴트 클라이언트 주소 설정
# 인스턴트 클라이언트가 설치된 곳의 주소를 넣으면 된다.

oracledb.init_oracle_client(lib_dir="C:\Oracle\InstantClient")

# ## Oracle DB 정보 입력
# 위의 전부 주석처리된 부분은 한줄씩 입력하는 방식이다.<br>
# 아래쪽은 모든 정보가 하드코딩되어있다.<br>
# 해당 Tablespace에 접근 가능한 계정이여야 한다. (DBA는 물론이고 해당 Tablespace의 모든 Table에 대해 SELECT 연산이 GRANT 되어있어야 함)

connection = oracledb.connect(user="ETL_ADMIN",
                              password="1234",
                              dsn=f"192.168.50.239:1521/XE")
cursor = connection.cursor()

# ## Tablespace Name이 'TestTableUniverse' 인 테이블 전부 가져오기
# 주의: BIN~으로 시작하는 (휴지통에 들어간) 테이블은 제외한다.

Result = cursor.execute("""SELECT *
                        FROM DBA_SEGMENTS
                        WHERE TABLESPACE_NAME ='TESTTABLEUNIVERSE'
                        AND SEGMENT_TYPE ='TABLE'
                        AND SEGMENT_NAME NOT LIKE 'BIN%'""").fetchall()

# ## DataFrame으로 형태 확인하기 (주석처리됨)
# (확인시 Jupyter Notebook / VSCode 등 ipynb 열 수 있는 IDE 사용 권장)

TableList = pd.DataFrame(Result)
TableList

# ## 가져온 Table의 DDL 읽어오기

DDLTXT = []
for i in range(len(Result)):
    SQL = f'''SELECT DBMS_METADATA.GET_DDL('TABLE', '{Result[i][1]}','{Result[i][0]}')
            FROM DUAL'''
    #print(SQL)
    DDLTXT.append(cursor.execute(SQL).fetchone()[0].read().replace("\n",'').replace("\t",''))

# ---------

# # Table 정보 추출

# +
# 테이블들의 정보가 들어가는 리스트이다.
TableInformation = []
# 기본키 정보가 들어가는 리스트이다.
PRIMARY_KEY_INFO = []
# 외래키 정보가 들어가는 리스트이다.
FOREIGN_KEY_INFO = []

for item in DDLTXT:
    rowdata = item.split("  ")
    # 테이블스페이스 이름, 스키마 이름과 테이블 이름을 추출한다.
    TableInfo = rowdata[1].split(" ")[2].split(".")
    Schema_Name = TableInfo[0].strip('""')
    Table_Name = TableInfo[1].strip('""')
    Tablespace_Name = rowdata[-1].strip(' ').strip('""').split(' ')[1].strip('""')
    
    # 테이블의 각 컬럼 정보를 저장한다.
    ColumnInfo = []
    CreateDetail = rowdata[3].split('"')[1:]
    for i in range(0,len(CreateDetail),2):
        # 컬럼 이름
        ColName = CreateDetail[i]
        
        RestraintDetail = CreateDetail[i+1].strip().split(' ')[0].rstrip(')').split('(')
        # 널 조건
        if len(CreateDetail[1].strip().split(' ')) > 1:
            Null_Info = "NOT NULL"
        else:
            Null_Info = ""
        # 컬럼의 데이터타입, 데이터길이
        ColType = TypeDict[RestraintDetail[0]]
        if RestraintDetail[0] in ["NUMBER","FLOAT"]:
            if RestraintDetail[1].split(',')[0] == '*':
                CharLength = 100
            else:
                CharLength = RestraintDetail[1].split(',')[0]
        elif RestraintDetail[0] in ["DATE"]:
            CharLength = ''
        else:
            CharLength = RestraintDetail[1].strip(',').strip('()')

        ColumnInfo.append([ColName, ColType, Null_Info, CharLength])
        
    TableInformation.append([Tablespace_Name,
                             Schema_Name,
                             Table_Name,
                             ColumnInfo])
        
    # 제약정보를 추출한다.
    for i in range(len(rowdata)):
        rowword = rowdata[i].split(' ')
        if rowword[0] == "CONSTRAINT":
            # 키
            if rowword[2] == "PRIMARY":
                # 테이블 이름, 컬럼 이름
                PKS = rowword[4:]
                PKCols = []
                for i in range(len(PKS)):
                    PKCols.append(PKS[i].strip(',').strip('()').strip('""'))
                PRIMARY_KEY_INFO.append([Tablespace_Name, Schema_Name, Table_Name, rowword[1].strip('""'), PKCols])
            if rowword[2] == "FOREIGN":
                FKREF = rowdata[i+1].split(' ')
                RefSchema = FKREF[1].split('.')[0].strip('""')
                RefTable = FKREF[1].split('.')[1].strip('""')
                RefCol = FKREF[2].strip('("")')
                Others = ' '.join(FKREF[3:-1])
                FOREIGN_KEY_INFO.append([Tablespace_Name, Schema_Name, Table_Name, rowword[1].strip('""'), rowword[4].strip('("")'), RefSchema, RefTable, RefCol,Others])
# -

# ## DataFrame으로 형태 확인하기 (주석처리됨)
# (확인시 Jupyter Notebook / VSCode 등 ipynb 열 수 있는 IDE 사용 권장)

# ### TableInformation 형태

pd.set_option('display.max_columns', None)
Tables = pd.DataFrame(TableInformation)
Tables.columns = ['테이블스페이스명','스키마명','테이블명','컬럼 정보']
Tables

# ### 기본키 형태

PKINFO = pd.DataFrame(PRIMARY_KEY_INFO)
PKINFO.columns = ['테이블스페이스명','스키마명','테이블명','제약조건명','PK컬럼']
PKINFO

# ### 외래키 형태

FKINFO = pd.DataFrame(FOREIGN_KEY_INFO)
FKINFO.columns = ['테이블스페이스명','참조스키마명','참조테이블명','제약조건명','참조컬럼명','원본스키마','원본테이블','원본컬럼','기타조건']
FKINFO

# -----

# ## Tablespace Name이 'TestTableUniverse' 인 인덱스 전부 가져오기

IndexFinder = cursor.execute('''SELECT
                                    A.INDEX_OWNER,
                                    A.INDEX_NAME,
                                    A.TABLE_OWNER,
                                    A.TABLE_NAME,
                                    A.COLUMN_POSITION,
                                    A.COLUMN_NAME,
                                    B.TABLESPACE_NAME,
                                    B.UNIQUENESS
                                FROM
                                    ALL_IND_COLUMNS A,
                                    ALL_INDEXES B
                                WHERE
                                    B.TABLESPACE_NAME='TESTTABLEUNIVERSE'
                                AND
                                    A.INDEX_NAME=B.INDEX_NAME
                            ''').fetchall()

# ## DataFrame으로 형태 확인하기 (주석처리됨)
# (확인시 Jupyter Notebook / VSCode 등 ipynb 열 수 있는 IDE 사용 권장)

# ### 인덱스 정보 형태

IndexListsDF = pd.DataFrame(IndexFinder)
IndexListsDF

# ## 가져온 INDEX 정보 보기 편하게 재조립하기
# 컬럼이 여러개가 모여 인덱스로 잡힌 경우 하나의 리스트로 컬럼들이 모이도록 재조립한다.

# +
INDEX_INFO = []
RecentIndex = ''
counter=0

for item in IndexFinder:
    # 이 인덱스를 이미 다뤘다면 (다중 컬럼으로 만들어진 인덱스) 컬럼만 추가한다.
    if RecentIndex == item[1]:
        INDEX_INFO[counter-1][6].append(item[5])
        RecentIndex = item[1]
        continue
        
    # 인덱스 종류 (기본)
    IndexType = 'INDEX'
    
    # 인덱스 테이블 스페이스
    Index_Tablespace = item[6]
    
    # 인덱스 스키마
    Index_Owner = item[0]
    
    # 인덱스 이름
    Index_Name = item[1]
        
    # 가장 최근 사용한 인덱스 이름을 갱신한다.
    RecentIndex = item[1]
    
    # 인덱스 테이블 스키마
    Index_Table_Owner = item[2]
    
    # 인덱스 테이블 이름
    Index_Table_Name = item[3]
    
    # 인덱스 컬럼
    Index_Columns = []
    Index_Columns.append(item[5])
    
    # 고유 인덱스라면 앞에 유니크를 붙인다.
    if item[7] == "UNIQUE":
        IndexType = 'UNIQUE '+ IndexType
        
    # 로우 추가
    INDEX_INFO.append([Index_Tablespace, IndexType, Index_Owner, Index_Name, Index_Table_Owner, Index_Table_Name, Index_Columns])
    
    # 카운터 추가
    counter+=1
# -

# ## DataFrame으로 형태 확인하기 (주석처리됨)
# (확인시 Jupyter Notebook / VSCode 등 ipynb 열 수 있는 IDE 사용 권장)

# ### 인덱스 정보 형태

IndexDF = pd.DataFrame(INDEX_INFO)
IndexDF.columns = ['인덱스 테이블스페이스','인덱스 종류','인덱스 스키마','인덱스 이름','테이블 스키마','테이블 이름','컬럼']
IndexDF

# -----

# # MariaDB에 옮기기

# ## 커넥터, 커서, SQL 초기화

conn = None
cur = None
sql = ""

# ## 정보

MARIA_DB_HOSTNAME = '192.168.50.195'
MARIA_DB_USERNAME = 'ETL_db'
MARIA_DB_PASSWORD = 'hnw123'
DATABASE_NAME = 'testtableuniverse'

# ## DB 연결

conn = pymysql.connect(host=MARIA_DB_HOSTNAME,user=MARIA_DB_USERNAME,password=MARIA_DB_PASSWORD)

# ## 커서 생성

cur = conn.cursor()

# ----

# # MariaDB에 빈 Table 생성
# checkpoint 프린트는 에러가 날 경우를 대비해 작성해두었다.

for item in TableInformation:
    #print(f"checkpoint {item[2]}-1")
    try:
        #print(f"checkpoint {item[2]}-2")
        TableCreateSQL = 'CREATE TABLE'
        # 테이블스페이스를 선택한다.
        try:
            #print(f"checkpoint {item[2]}-3")
            cur.execute(f'use {item[0]};')
        # 해당 테이블스페이스명의 스키마가 없으면 생성해준다.
        except:
            cur.execute(f'create database {item[0]};')
        finally:
            cur.execute(f'use {item[0]};')

        TableCreateSQL += f' {item[1]}_{item[2]} ('
        for i in range(len(item[3])):
            #print(f"checkpoint {item[2]}-4")
            if i > 0:
                #print(f"checkpoint {item[2]}-5")
                TableCreateSQL += ', '
            # 컬럼 명
            TableCreateSQL += f' {item[3][i][0]}'
            # 데이터 타입, 데이터 길이
            if item[3][i][1] in ["DATETIME"]:
                TableCreateSQL += f' {item[3][i][1]}'
            else:
                TableCreateSQL += f' {item[3][i][1]}({item[3][i][3]})'
            # NULL 관련 조건
            TableCreateSQL += f' {item[3][i][2]}'
        TableCreateSQL += ');'
        print(TableCreateSQL)
        cur.execute(TableCreateSQL)
       # print(f"checkpoint {item[2]}-6")
    except Exception as e:
        #print(f"checkpoint {item[2]}-7")
        print(e)
        continue

# -----

# # OracleDB 데이터 추출

# ## OracleDB 연결

oracledb.init_oracle_client(lib_dir="C:\Oracle\InstantClient")

connection = oracledb.connect(user="ETL_ADMIN",
                              password="1234",
                              dsn=f"192.168.50.239:1521/XE")

cursor = connection.cursor()

# ## OracleDB의 데이터 가져오기

TableDatas = []
for item in TableInformation:
    # 데이터 프레임으로 뜯어온다. 이때, 컬럼명 또한 복사한다.
    DF_ = pd.DataFrame(cursor.execute(f'SELECT * FROM {item[1]}.{item[2]}').fetchall(), columns=[row[0] for row in item[3]])
    TableDatas.append([item[0],
                       item[1],
                       item[2],
                       DF_])

# -----

# # Maria DB의 빈 테이블에 데이터 집어넣기

# ## 정보 입력

password = 'hnw123'
DBName = 'TESTTABLEUNIVERSE'

DATABASE = f'mariadb+pymysql://ETL_db:{password}@192.168.50.195:3306/testtableuniverse'
engine = create_engine(DATABASE)

# ## 데이터 입력
# DataFrame.to_sql 메소드를 활용해 집어넣는다.<br>
# 주의: to_sql 메소드는 대소문자 구문을 하지 못한다.

for item in TableDatas:
    conn = engine.connect()
    engine.execute(f"use {item[0]}")
    item[3].to_sql(name=f'{item[1]}_{item[2]}', con=engine, if_exists='append', index=False)

# -----------

# # Primary Key 제약조건 추가

for item in PRIMARY_KEY_INFO:
    try:
        TableAlterSQL = f'ALTER TABLE {item[0]}.{item[1]}_{item[2]} ADD CONSTRAINT {item[3]} PRIMARY KEY ('
        for i in range(len(item[4])):
            if i>0:
                TableAlterSQL += ','
            TableAlterSQL = TableAlterSQL + f'{item[4][i]}'
        TableAlterSQL += ');'
        print('-'*20)
        print(TableAlterSQL)
        cur.execute(TableAlterSQL)
    except Exception as e:
        print(e)
        continue

# # Foreign Key 제약조건 추가

for item in FOREIGN_KEY_INFO:
    try:
        TableAlterSQL = f'ALTER TABLE {item[0]}.{item[1]}_{item[2]} ADD CONSTRAINT {item[3]} FOREIGN KEY ({item[4]}) REFERENCES {item[0]}.{item[5]}_{item[6]} ({item[7]}) {item[8]}'
        cur.execute(TableAlterSQL)
    except Exception as e:
        print(e)
        continue

# # Index 가져오기

for item in INDEX_INFO:
    try:
        TableAlterSQL = f'ALTER TABLE {item[0]}.{item[2]}_{item[5]} ADD {item[1]} {item[2]} ('
        for i in range(len(item[6])):
            if i>0:
                TableAlterSQL += ','
            TableAlterSQL += f'{item[6][i]}'
        TableAlterSQL += ')'
        cur.execute(TableAlterSQL)
    except Exception as e:
        print(e)
        continue


