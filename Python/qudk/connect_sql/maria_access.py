import pymysql
import sqlalchemy
from sqlalchemy import create_engine
import json, pandas as pd
import logging

from Python.qudk.utils import maria_util

config={
    "host": "localhost",
    "username": "ETL_db",
    "password": "hnw123",
    "db": "test"
}
# config={
#     "host": "localhost",
#     "username": "root",
#     "password": "1234",
#     "db": "test"
# }

def connect():
    return pymysql.connect(host=config['host'],
                           user=config['username'],
                           password=config['password'],
                           db=config['db'])

def execute_create_tables(tb_ddl_list, err_list):
    conn = connect()
    cursor = conn.cursor()

    for tb_ddl in tb_ddl_list:
        tb_name=list(tb_ddl.keys())[0]
        sql=list(tb_ddl.values())[0]
        try:
            cursor.execute(sql)
        except Exception as e:
            logging.error("=======================================")
            logging.error(e)
            logging.error("SQL Exception!!! Failed Creating Table!!!")
            logging.error(f"Table: {tb_name}")
            logging.error(f"SQL: {sql}")
            logging.error("=======================================")

            err_list.append(tb_name)

    conn.commit()
    cursor.close()
    conn.close()
def execute_insert_values(queries_dict, tb_err_list, insert_err_dict):
    conn=connect()
    cursor=conn.cursor()
    for tb_name in queries_dict:
        err_list = []
        queries = queries_dict[tb_name]
        # 테이블 생성 에러 목록에 있을 경우 그대로 insert_err_dict 에 추가
        if tb_name in tb_err_list:
            err_list=queries
        else:
            for sql in queries:
                try:
                    cursor.execute(sql)
                except Exception as e:
                    logging.error("=======================================")
                    logging.error(e)
                    logging.error("SQL Exception!!! Failed Inserting Data!!!")
                    logging.error(f"Table: {tb_name}")
                    logging.error(f"SQL: {sql}")
                    logging.error("=======================================")
                    err_list.append(sql)
        insert_err_dict[tb_name]=err_list
    conn.commit()
    cursor.close()
    conn.close()
def execute_add_fk(fk_ddl_dict, tb_error_list, fk_err_dict):
    conn = connect()
    cursor = conn.cursor()

    for tb_name in fk_ddl_dict:
        fk_ddl_list = fk_ddl_dict[tb_name]
        error_list = []
        # create에 실패한 테이블 목록에 있을 경우 그대로 fk_err_dict 에 추가
        if tb_name in tb_error_list:
            error_list = fk_ddl_list
        else:
            for sql in fk_ddl_list:
                try:
                    cursor.execute(sql)
                except Exception as e:
                    logging.error("=======================================")
                    logging.error(e)
                    logging.error("SQL Exception!!! Failed Adding Foreign Key!!!")
                    logging.error(f"Table: {tb_name}")
                    logging.error(f"SQL: {sql}")
                    logging.error("=======================================")
                    error_list.append(sql)
        fk_err_dict[tb_name]=error_list

    conn.commit()
    cursor.close()
    conn.close()



##########################################################################################################

def insert_data(schema_name, real_data_dict, tb_dtype_dict, tb_err_list, insert_err_dict):
    DATABASE = f'mariadb+pymysql://{config["username"]}:{config["password"]}@localhost:3306/{config["db"]}'

    engine = create_engine(DATABASE)
    tb_list = list(real_data_dict)
    Datas = []
    for tb_name in real_data_dict:
        Datas.append(real_data_dict[tb_name])
    conn = engine.connect()

    for i in range(len(Datas)):
        DictDF = pd.DataFrame.from_dict(Datas[i])
        table = tb_list[i].lower()
        dtype = tb_dtype_dict[tb_list[i]]
        try:
            DictDF.to_sql(name=table, con=engine, dtype=dtype, if_exists='append', index=False)
        except Exception as e:
            TableStatus = pd.read_sql(f'SELECT * FROM {tb_list[i]}', con=engine)
            if DictDF.equals(TableStatus)==False:
                print('>>> INSERTING...')
                try:
                    DictDF.to_sql(name=table, con=engine, dtype=dtype, if_exists='append', index=False)
                except Exception as e:
                    logging.error("="*30)
                    logging.error(f'>>> ERR : 데이터프레임 {tb_list[i]}를 업로드하지 못했습니다:')
                    logging.error(f'사유: {e}')
                    logging.error(f'data: {Datas[i]}')
                    logging.error("="*30)
                    continue
                print('-'*30)
            else:
                print('>>> ERR : 해당 자료는 최신입니다.')
                print('-'*30)

def create_tables(tb_list, tb_col_type_dict):
    DATABASE = f'mariadb+pymysql://{config["username"]}:{config["password"]}@localhost:3306/{config["db"]}'
    engine=create_engine(DATABASE)
    metadata = sqlalchemy.MetaData(bind=engine)

    for tb_name in tb_list:
        prepared_cols=tb_col_type_dict[tb_name]
        sqlalchemy.Table(tb_name, metadata, *(prepared_cols[col] for col in prepared_cols))
    metadata.create_all()
