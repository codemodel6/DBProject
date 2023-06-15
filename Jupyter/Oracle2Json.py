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

import pandas as pd
import json
import oracledb

oracledb.init_oracle_client(lib_dir="C:\Oracle\InstantClient")

# -----

# # Oracle에서 정보 가져오기

# ## Oracle DB 정보 입력
# 위의 전부 주석처리된 부분은 한줄씩 입력하는 방식이다.<br>
# 아래쪽은 모든 정보가 하드코딩되어있다.<br>
# 해당 Tablespace에 접근 가능한 계정이여야 한다. (DBA는 물론이고 해당 Tablespace의 모든 Table에 대해 SELECT 연산이 GRANT 되어있어야 함)

USERNAME = "ETL_ADMIN"

connection = oracledb.connect(user=USERNAME,
                              password="1234",
                              dsn=f"192.168.50.239:1521/XE")
cursor = connection.cursor()

# ## 사용자가 접근 가능한 모든 Table과 그 Table의 Comment 추출

# ### Table 정보 추출

TBNameNComment = cursor.execute("""SELECT
                                        A.SEGMENT_NAME AS TableName, B.COMMENTS AS Commentary, C.COLUMN_NAME, C.COMMENTS AS Col_Commentary
                                    FROM
                                        USER_SEGMENTS A, ALL_TAB_COMMENTS B, ALL_COL_COMMENTS C
                                    WHERE
                                        TABLESPACE_NAME ='TESTTABLEUNIVERSE'
                                        AND SEGMENT_TYPE ='TABLE'
                                        AND SEGMENT_NAME NOT LIKE 'BIN%'
                                        AND A.SEGMENT_NAME=B.TABLE_NAME
                                        AND B.TABLE_NAME = C.TABLE_NAME
                                """).fetchall()

# ### Table의 DDL 정보 추출

DDLTXT = []
for i in range(len(TBNameNComment)):
    SQL = f'''SELECT DBMS_METADATA.GET_DDL('TABLE', '{TBNameNComment[i][0]}')
            FROM DUAL'''
    #print(SQL)
    DDLTXT.append(cursor.execute(SQL).fetchone()[0].read().replace("\n",'').replace("\t",''))

# ### DDL에서 메타데이터 추출

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
        ColType = RestraintDetail[0]
        if RestraintDetail[0] in ["NUMBER","FLOAT"]:
            if RestraintDetail[1].split(',')[0] == '*':
                CharLength = [100]
            else:
                CharLength = RestraintDetail[1].strip('),').split(',')
        elif RestraintDetail[0] in ["DATE"]:
            CharLength = ''
        else:
            CharLength = RestraintDetail[1].strip('),').split(',')

        ColumnInfo.append([ColName, ColType, Null_Info, CharLength])
        
    TableInformation.append([Tablespace_Name,
                             Schema_Name,
                             Table_Name,
                             ColumnInfo])
        
    # 제약정보를 추출한다.
    for i in range(len(rowdata)):
        rowword = rowdata[i].split(' ')
        
        if rowword[0] == "CONSTRAINT":
            
            if rowword[2:4] == ["PRIMARY","KEY"]:
                # 테이블 이름, 컬럼 이름
                PKS = rowword[4:]
                PKCols = []
                for j in range(len(PKS)):
                    PKCols.append(PKS[j].strip(',').strip('()').strip('""'))
                PRIMARY_KEY_INFO.append([Tablespace_Name, Schema_Name, Table_Name, rowword[1].strip('""'), PKCols])
                
            if rowword[2:4] == ["FOREIGN","KEY"]:
                # Reference 하고 있는 Column들의 이름이 담긴 리스트
                FKS = rowword[4:]
                #print(FKS)
                FKCols = []
                for k in range(len(FKS)):
                    FKCols.append(FKS[k].strip(',').strip('()').strip('""'))
                    
                FKREF = rowdata[i+1].split(' ')
                
                #print(FKREF)
                RefSchema = FKREF[1].split('.')[0].strip('""')
                RefTable = FKREF[1].split('.')[1].strip('""')
                
                RefCol = FKREF[2]
                Marker = 3
                ReferenceColumns=[]
                EndRefCol = Marker-1
                while(FKREF[EndRefCol][-1] != ')'):
                    Marker+=1
                    EndRefCol+=1
                RefCol=FKREF[2:EndRefCol+1]
                for RefColName in RefCol:
                    RefColName = RefColName.strip('()').strip('""').strip(',').strip('""')
                    ReferenceColumns.append(RefColName)
                    
                Others = ' '.join(FKREF[Marker:-1])
                FOREIGN_KEY_INFO.append([Tablespace_Name, Schema_Name, Table_Name, rowword[1].strip('""'), FKCols, RefSchema, RefTable, ReferenceColumns,Others])
# -

# ## 메타데이터 추출 결과

# ### 테이블-코멘트 데이터프레임

pd.set_option('display.max_columns', None)
TableList = pd.DataFrame(TBNameNComment)
TableList.columns=['테이블명','테이블 코멘트', '컬럼명', '컬럼 코멘트']

TableList

# ### 컬럼 정보 데이터프레임

pd.set_option('display.max_columns', None)
Tables = pd.DataFrame(TableInformation)
Tables.columns = ['테이블스페이스명','스키마명','테이블명','컬럼 정보']

Tables

# ### 기본키 정보 데이터프레임

PKINFO = pd.DataFrame(PRIMARY_KEY_INFO)
PKINFO.columns = ['테이블스페이스명','스키마명','테이블명','제약조건명','PK컬럼']

PKINFO

# ### 외래키 형태

FKINFO = pd.DataFrame(FOREIGN_KEY_INFO)
FKINFO.columns = ['테이블스페이스명','참조스키마명','참조테이블명','제약조건명','참조컬럼명','원본스키마','원본테이블','원본컬럼','기타조건']
FKINFO

# -----

# # JSON 조립

# ## 메타데이터 (ALLDATA) 부분

# +
ALLDATA = {}
TableInfo = {}
TableData = {}
for item in TableInformation:
    # 컬럼 정보가 담긴 딕셔너리
    ColumnInfo = {}
    for ColInfos in item[3]:
        ColumnInfo[ColInfos[0]] = [ColInfos[1],ColInfos[3],ColInfos[2]]
    
    # 코멘트 정보
    Tbl_CommentInfo = str(TableList.loc[TableList['테이블명']==f'{item[2]}']["테이블 코멘트"].tolist()[0])
    
    # 해당 테이블의 컬럼 명을 리스트로 가져오기
    ColumnNames = TableList.loc[TableList['테이블명']==f'{item[2]}']['컬럼명'].tolist()
    Col_CommentInfo = {}
    for ColName in ColumnNames:
        NaeYongMul = str(TableList.loc[TableList['컬럼명']==f'{ColName}']['컬럼 코멘트'].tolist()[0])
        if NaeYongMul == 'None':
            NaeyongMul = ''
        Col_CommentInfo[ColName] = NaeYongMul
    CommentInfo = {}
    CommentInfo['Table'] = Tbl_CommentInfo
    CommentInfo['Columns'] = Col_CommentInfo
    
    # 기본키 정보
    PKInfoList = PKINFO.loc[PKINFO['테이블명'] ==f'{item[2]}']['PK컬럼'].drop_duplicates().tolist()
    if len(PKInfoList) == 0:
        PKInfo = []
    else:
        PKInfo = PKINFO.loc[PKINFO['테이블명'] ==f'{item[2]}']['PK컬럼'].drop_duplicates().tolist()[0]
    
    # 외래키 제약 조건명
    FKNAME = FKINFO.loc[FKINFO['참조테이블명']==f'{item[2]}']['제약조건명'].drop_duplicates().tolist()
    #print(FKNAME)
    
    FKInfo = {}
    for FKRNames in FKNAME:
        #print(FKRNames)
        RefTable = FKINFO.loc[(FKINFO['참조테이블명']==f'{item[2]}') & (FKINFO['제약조건명']==FKRNames)]['참조테이블명'].drop_duplicates().tolist()[0]
        RefColList = FKINFO.loc[(FKINFO['참조테이블명']==f'{item[2]}') & (FKINFO['제약조건명']==FKRNames)]['참조컬럼명'].drop_duplicates().tolist()[0]
        #print(RefColList)
        OriginSchema = FKINFO.loc[(FKINFO['참조테이블명']==f'{item[2]}') & (FKINFO['제약조건명']==FKRNames)]['원본스키마'].drop_duplicates().tolist()[0]
        OriginTable = FKINFO.loc[(FKINFO['참조테이블명']==f'{item[2]}') & (FKINFO['제약조건명']==FKRNames)]['원본테이블'].drop_duplicates().tolist()[0]
        OriginCol = FKINFO.loc[(FKINFO['참조테이블명']==f'{item[2]}') & (FKINFO['제약조건명']==FKRNames)]['원본컬럼'].drop_duplicates().tolist()[0]
        FKETC = FKINFO.loc[(FKINFO['참조테이블명']==f'{item[2]}') & (FKINFO['제약조건명']==FKRNames)]['기타조건'].drop_duplicates().tolist()[0]
    

        FKInfo[FKRNames] = {"FKName":FKRNames,
                             "Columns":RefColList,
                             "Original Schema": OriginSchema,
                             "Original Table": OriginTable,
                             "Original Column": OriginCol,
                             "ETC Info": FKETC}        
    TableData[item[2]] = {"Columns":ColumnInfo, "PKInfo":PKInfo, "FKInfo":FKInfo, "Comment":CommentInfo}
    
TableInfo[USERNAME] = TableData
ALLDATA['TableInfo'] = TableInfo
# -

ALLDATA

# ## 실제데이터 (RealData) 부분

# +
RealData = {}
TableDatas = {}
for item in TableInformation:
    # 데이터 프레임으로 뜯어온다. 이때, 컬럼명 또한 복사한다.
    DF_ = pd.DataFrame(cursor.execute(f'SELECT * FROM {item[1]}.{item[2]}').fetchall(), columns=[row[0] for row in item[3]])
    EmptyDict = {}
    for i in range(len(DF_.columns)):
        EmptyDict[DF_.columns[i]] = DF_.iloc[:,i].astype(str).tolist()
    TableDatas[item[2]] = EmptyDict
    
RealData[USERNAME] = TableDatas
# -

# ## 메타데이터-실제데이터 조립

Result_Dict = {
    "TableData": ALLDATA,
    "RealData": RealData
}

# -----

# # 데이터 출력 (.JSON)

with open("Result_File.json", "w", encoding='UTF-8') as json_file:
    json_file.write(json.dumps(Result_Dict, ensure_ascii=False))


