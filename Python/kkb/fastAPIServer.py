import cx_Oracle
import json
from handleOracleServer.oracleBindData import getOracleData,getOracleIndex,getOracleDic,getOracleContent
from datetime import datetime
import subprocess

# 오라클 DB 연결 정보
db_user = 'ETL_ADMIN'
db_password = '1234'
db_host = 'localhost'
db_port = 1521
db_sid = 'XE'

# 오라클 DB에 연결
db_connection = cx_Oracle.connect(f"{db_user}/{db_password}@{db_host}:{db_port}/{db_sid}")
cursor = db_connection.cursor()

# SQL 쿼리 실행
tableQuery = 'SELECT table_name FROM user_tables'
indexQuery = "SELECT INDEX_NAME FROM USER_INDEXES WHERE TABLE_OWNER = 'ETL_ADMIN'"

# 오라클DB로부터 query를 실행한 결과를 가져옴
# 테이블 가져오는 코드
tableInfoDic = getOracleData(cursor, tableQuery)

# 인덱스 가져오는 코드
indexList = getOracleIndex(cursor, indexQuery)

# 테이블 내용 가져오는 코드
tableContentList = getOracleContent(cursor, tableQuery)

# 딕셔너리로 묶는 코드
oracleDic = getOracleDic(tableInfoDic,indexList,tableContentList)
print(oracleDic)

# datetime 객체를 문자열로 변환하는 함수
def convert_datetime(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')

# 쿼리 결과를 파일에 작성
# jsonData = json.dumps(oracleDic)
jsonData = json.dumps(oracleDic, default=convert_datetime)
file_name = 'result.json'
with open(file_name, 'w') as f:
    f.write(jsonData)

# fast api를 실행한다.
def run_fastapi_app():
    command = "uvicorn main:app --reload"
    subprocess.run(command, shell=True)

if __name__ == "__main__":
    run_fastapi_app()

run_fastapi_app()