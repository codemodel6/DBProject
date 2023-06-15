import pymysql
import sqlalchemy.types
from sqlalchemy import create_engine
import json, pandas as pd
import logging

from sqlalchemy.ext.declarative import declarative_base

def remove_owner_name(ddl):
    return ddl.replace(ddl[ddl.find("\""):ddl.find(".")+1], "")
def convert_idx_ddl(ddl_list):
    list=[]
    for ddl in ddl_list:
        ddl=remove_owner_name(ddl)
        ddl=ddl.replace("\"", "")
        list.append(ddl[:ddl.find(")")+1])
    return list




# def insert_data(content):
#     conn, cursor = None, None
#     try:
#         conn = connect()
#         cursor = conn.cursor()
#
#         for data in content:
#             keys = data.keys()
#
#             for tb_name in keys:
#                 rows = data[tb_name]
#                 if len(rows) == 0:
#                     continue
#                 cols = rows[0].keys()
#                 col_cnt = len(cols)
#                 cols_str = ', '.join(cols)
#
#                 params_list = ['%s'] * col_cnt
#                 params_str = ', '.join(params_list)
#
#                 sql = f'INSERT INTO {tb_name}({cols_str}) VALUES({params_str})'
#                 for row in rows:
#                     param = list(row.values())
#                     cursor.execute(sql, param)
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#     finally:
#         cursor.close()
#         conn.close()
#
#     # data = content[0]


def convert_ddl(ddl_list):
    # 0. 테이블명 추출 - .이 처음 나오는 부분부터 다음 공백까지 슬라이싱 후 쿼터 등 제거
    # 1. 첫 괄호가 나오는 부분 - 짝이 맞는 닫히는 괄호까지 자르고
    # 2. 컬럼명, 자료형을 각각 추출
    # 3. 자료형을 maria에 맞게 변형
    tb_ddl_list=[]
    constraint_list = []
    for ddl in ddl_list:
        tb_name = extract_table_name(ddl)
        tb_info = extract_table_info(ddl)
        # tb_info_list = [x.strip() for x in tb_info.split(",")]
        tb_info_list=split_info(tb_info)
        column_list=[]
        for tb_info in tb_info_list:
            tb_info = tb_info.replace('ENABLE', '').strip()
            constraint_str='CONSTRAINT'
            const_idx=tb_info.find(constraint_str)
            #     constraint_list.append((tb_name, tb_info))
            # if tb_info.startswith(constraint_str):
            tb_info=tb_info.strip("\n").strip("\t")
            if(const_idx!=-1 and not check_word_in_double_quot(tb_info, constraint_str)):
                constraint_list.append((tb_name, tb_info))
            else:
                tmp = tb_info.split(" ")
                column_name = tmp[0].strip("\"")
                column_detail_raw = tb_info[len(tmp[0]):].strip()
                column_detail = convert_column_detail(column_detail_raw)

                column_def=f'{column_name} {column_detail}'
                column_list.append(column_def)
        maria_tb_ddl=f'CREATE TABLE {tb_name}({", ".join(column_list)})'
        tb_ddl_list.append(maria_tb_ddl)
    return tb_ddl_list, constraint_list

def split_info(tb_info):
    # tb_info_list = [x.strip() for x in tb_info.split(",")]
    tb_info_list = []
    opened_bracket_cnt=0
    i, j = 0, 0
    while(i<len(tb_info)):
        if tb_info[i]=='(':
            opened_bracket_cnt+=1
        elif tb_info[i]==')':
            opened_bracket_cnt-=1
        if(opened_bracket_cnt==0 and tb_info[i]==','):
            tb_info_list.append(tb_info[j:i])
            j=i+1
        i+=1
    tb_info_list.append(tb_info[j:])
    return tb_info_list
def extract_table_name(ddl):
    start=ddl.find('.')+1
    end=ddl.find(" ", start+1)
    return ddl[start:end].strip("\"")
def extract_table_info(ddl):
    start=ddl.find('(')+1
    i=start
    bracket_cnt=1
    while(bracket_cnt!=0 and i<len(ddl)):
        if ddl[i]=='(':
            bracket_cnt+=1
        elif ddl[i]==')':
            bracket_cnt-=1
        i+=1
    return ddl[start:i-1]

def convert_column_detail(column_detail_raw):
    column_detail=''
    i=column_detail_raw.find("(")
    if i < 0:
        i=column_detail_raw.find(" ")
    type_name=''
    if i < 0:
        type_name=column_detail_raw
    else:
        type_name=column_detail_raw[:i]
    if type_name=='NUMBER':
        decimal_cnt=int(column_detail_raw[column_detail_raw.find(",")+1:column_detail_raw.find(")")])
        number_type = column_detail_raw[:column_detail_raw.find(")")+1]
        if decimal_cnt==0:
            column_detail=column_detail_raw.replace(number_type, "INT")
        else:
            column_detail=column_detail_raw.replace(number_type, "DOUBLE")

    elif type_name=='VARCHAR2':
        column_detail = column_detail_raw.replace(type_name, "VARCHAR")
    elif type_name=='DATE':
        column_detail = column_detail_raw.replace(type_name, "DATETIME")
    return column_detail

def classify_constraints(con_list):
    pk, fk, etc=[],[],[]
    for constraint in con_list:
        if 'PRIMARY' in constraint[1]:
            pk.append(constraint)
        elif 'FOREIGN' in constraint[1]:
            fk.append(constraint)
        else:
            etc.append(constraint)
    return pk, fk, etc

def convert_pk_ddl(pk_constraints):
    list=[]
    for constraint in pk_constraints:
        # keyword=constraint[1].split(" ")[2]
        first_q = constraint[1].find("\"")
        second_q = constraint[1].find("\"", first_q)
        start=constraint[1].find("PRIMARY", second_q)
        end=constraint[1].find(")")
        ext_cst=constraint[1][start:end+1].replace("\"", "")
        sql=f'ALTER TABLE {constraint[0]} ADD {ext_cst}'
        list.append(sql)
    return list
# def execute_create(queries):
#     conn = connect()
#     cursor = conn.cursor()
#     for sql in queries:
#         try:
#             cursor.execute(sql)
#         except Exception as e:
#             print(e)
#     conn.commit()
#     cursor.close()
#     conn.close()
def convert_fk_ddl(fk_constraints):
    list=[]
    for constraint in fk_constraints:
        first_q = constraint[1].find("\"")
        second_q = constraint[1].find("\"", first_q)
        start=constraint[1].find("FOREIGN", second_q)
        remove_start=constraint[1].find("\"", constraint[1].find("REFERENCES"))
        remove_end=constraint[1].find(".", remove_start)
        prefix_removed = constraint[1][start:remove_start]+constraint[1][remove_end+1:]
        fk = prefix_removed.replace("\"", "")
        sql=f'ALTER TABLE {constraint[0]} ADD {fk}'
        list.append(sql)
    return list

def check_word_in_double_quot(str, word):
    i=0
    while i<len(str):
        start = str.find("\"", i)
        if start==-1:
            return False
        end = str.find("\"", start)
        if word in str[start:end]:
            return True
        i=end+1
    return False



####################################################################################################
# json 키
Table = 'Table'
Columns = 'Columns'
PKInfo = 'PKInfo'
FKInfo = 'FKInfo'
Comment = 'Comment'
def build_create_table(meta_data_list):
    tb_ddl_list=[]
    fk_info_dict={}
    tb_col_type_dict={}
    prefix = 'CREATE TABLE'
    for tb_name in meta_data_list:
        tb_info = meta_data_list[tb_name]
        try:
            columns = tb_info[Columns]
            pk_info = tb_info[PKInfo]
            fk_info_dict[tb_name]=tb_info[FKInfo]
            tb_col_type_dict[tb_name]={}
            pk_str = ''
            if len(pk_info) > 0:
                # raise Exception
                pk_str = f', PRIMARY KEY({", ".join(pk_info)})'
            tb_comment, col_comments = extract_comments(tb_info)
            builded_cols = build_columns(columns, tb_col_type_dict[tb_name])
            add_col_comments(builded_cols, col_comments)

            sql=f'{prefix} {tb_name}({", ".join(builded_cols.values())}{pk_str}){tb_comment}'
            tb_ddl_list.append({tb_name:sql})

        except Exception as e:
            logging.error(e)
            logging.error("Failed Building SQL!!!")
            logging.error(f'{tb_name}: {tb_info}')
    return tb_ddl_list, fk_info_dict, tb_col_type_dict

def extract_comments(tb_info):
    tb_comment=''
    col_comments={}
    if Comment in tb_info.keys():
        comments=tb_info[Comment]
        if Table in comments:
            # tb_comment = comments['Table']
            tb_comment = f' COMMENT = "{comments[Table]}"'
        if Columns in comments:
            col_comments = comments[Columns]
    return tb_comment, col_comments
def add_col_comments(builded_cols, col_comments):
    for col in col_comments:
        builded_cols[col]=f'{builded_cols[col]} COMMENT "{col_comments[col]}"'
def build_columns(columns, col_type_dict):
    builded_cols={}
    for col in columns:
        rs, type_name = convert_type(columns[col])
        builded_cols[col]=f'{col} {rs}'
        col_type_dict[col]=type_name
    return builded_cols

class TypeName:
    BINARY='BINARY'
    BIT='BIT'
    BLOB='BLOB'
    TEXT='TEXT'
    CHAR='CHAR'
    DATETIME='DATETIME'
    TIMESTAMP='TIMESTAMP'
    FLOAT='FLOAT'
    DOUBLE='DOUBLE'
    VARCHAR='VARCHAR'
    NUMBER='NUMBER'
    INT='INT'

type_dict = {
    'RAW':TypeName.BINARY,
    'BLOB':TypeName.BLOB,
    'CHAR':TypeName.CHAR,
    'DATE':TypeName.DATETIME,
    'TIMESTAMP':TypeName.DATETIME,
    'FLOAT':TypeName.FLOAT,
    'DOUBLE':TypeName.DOUBLE,
    'VARCHAR':TypeName.VARCHAR,
    'VARCHAR2':TypeName.VARCHAR,
    'CLOB':TypeName.TEXT,
    'NUMBER':TypeName.NUMBER
}
def convert_type(col):
    rs = ''
    null_str = ''
    if len(col[2].strip())>0:
        null_str=f' {col[2]}'
    type_name = type_dict[col[0]]
    if type_name.endswith('CHAR'):
        rs=f'{type_name}({col[1][0]}){null_str}'
    else:
        if type_name == 'NUMBER':
            if len(col[1]) > 1 and int(col[1][1]) > 0:
                type_name = 'DOUBLE'
            else:
                type_name = 'INT'
        rs = f'{type_name}{null_str}'
    return rs, type_name

def build_insert_values(real_data, tb_col_type_dict):
    query_dict={}
    for tb_name in real_data:
        queries=[]
        values = real_data[tb_name]
        cols=list(values.keys())
        row_cnt=len(values[cols[0]])
        if row_cnt == 0:
            continue
        i=0
            # multiple_params=[]
            # while i < row_cnt:
            #     tmp=[]
            #     for col in cols:
            #         tmp.append(values[col][i])
            #     multiple_params.append(f"({','.join(tmp)})")
            #     i+=1
            # cols_str = ', '.join(cols)
            # sql = f'INSERT INTO {tb_name}({cols_str}) VALUES{",".join(multiple_params)}'
            # queries.append(sql)
        # insert 실행시 어떤 데이터에서 예외가 생기는지 확인하기 위해 values(),()로 하지 않음.

        cols_str = ', '.join(cols)
        while i < row_cnt:
            params_str = wrap_quot_by_type(cols, values, i, tb_col_type_dict[tb_name])
            sql = f'INSERT INTO {tb_name}({cols_str}) VALUES({params_str})'
            queries.append(sql)
            i += 1
        query_dict[tb_name] = queries
    return query_dict

unwrap_type = ['INTEGER', 'DOUBLE']
def wrap_quot_by_type(cols, values, i, col_type_dict):
    tmp=[]
    for col in cols:
        if col_type_dict[col] in unwrap_type:
            tmp.append(values[col][i])
        else:
            tmp.append(f'"{values[col][i]}"')
    return ", ".join(tmp)


etc_info= 'ETC Info'
original_column= 'Original Column'
original_schema= 'Original Schema'
original_table= 'Original Table'
target_schema = 'ETL_ADMIN'
def build_add_fk(fk_info_dict, fk_schema_except):
    fk_ddl_dict={}
    for tb_name in fk_info_dict:
        tb_fk_list = fk_info_dict[tb_name]
        list=[]
        for fk_name in tb_fk_list:
            cascade=''
            fk_data=tb_fk_list[fk_name]
            if fk_data[original_schema] != target_schema:
                fk_schema_except[tb_name]=fk_data
                logging.debug("=======================================")
                logging.debug("참조할 스키마가 존재하지 않아 SQL을 빌드할 수 없습니다!!!")
                logging.debug(fk_data)
                logging.debug("=======================================")
                continue
            if etc_info in fk_data:
                cascade = f' {fk_data[etc_info].strip()}'
            sql=f'ALTER TABLE {tb_name} ADD CONSTRAINT {fk_name} FOREIGN KEY({",".join(fk_data[Columns])}) REFERENCES {fk_data[original_table]}({",".join(fk_data[original_column])}){cascade}'
            list.append(sql)
        fk_ddl_dict[tb_name]=list
    return fk_ddl_dict
###########################################################
def extract_key_info(table_info_list):
    pk_info_dict, fk_info_dict={}, {}
    for tb_name in table_info_list:
        fk_info_dict[tb_name]=table_info_list[tb_name][FKInfo]
        pk_info_dict[tb_name]=table_info_list[tb_name][PKInfo]
    return pk_info_dict, fk_info_dict
def extract_convert_type(table_info_list, pk_info_dict):
    col_type_dict={}
    for tb_name in table_info_list:
        cols=table_info_list[tb_name][Columns]
        types={}
        for col in cols:
            column = cols[col]
            col_type = convert_type2(column)
            pk_col = col in pk_info_dict[tb_name]
            null_col = not len(column[2].strip()) > 0
            types[col] = sqlalchemy.Column(col.lower(), col_type, primary_key=pk_col, nullable=null_col)

        col_type_dict[tb_name]=types
    return col_type_dict

def convert_type2(col):
    rs = None
    type_name=convert_type_name(col)

    if type_name==TypeName.INT:
        rs = sqlalchemy.types.Integer()
    elif type_name==TypeName.VARCHAR:
        rs = sqlalchemy.types.VARCHAR(length=int(col[1][0]))
    elif type_name==TypeName.CHAR:
        rs = sqlalchemy.types.CHAR(length=int(col[1][0]))
    elif type_name==TypeName.DATETIME:
        rs = sqlalchemy.types.DateTime()
    elif type_name==TypeName.FLOAT:
        rs = sqlalchemy.types.Float()
    elif type_name==TypeName.BLOB:
        rs = sqlalchemy.types.BLOB()
    return rs

def convert_type_name(col):
    type_name = type_dict[col[0]]
    if type_name == TypeName.NUMBER:
        if len(col[1]) > 1 and int(col[1][1]) > 0:
            type_name = TypeName.FLOAT
        else:
            type_name = TypeName.INT
    return type_name

def get_dtype(tb_col_type_dict):
    tb_dtype={}
    for tb_name in tb_col_type_dict:
        col_dict=tb_col_type_dict[tb_name]
        dtype = {}
        for col in col_dict:
            column=col_dict[col]
            name=column.name
            dtype[name]=column.type
        tb_dtype[tb_name]=dtype
    return tb_dtype
