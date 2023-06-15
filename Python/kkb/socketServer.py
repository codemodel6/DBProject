import socket
import cx_Oracle
import json
from handleOracleServer.oracleBindData import getOracleData, getOracleIndex,getOracleContent, getOracleDic
from datetime import datetime

# 오라클 DB 연결 정보
db_user = 'ETL_ADMIN'
db_password = '1234'
db_host = 'localhost'
db_port = 1521
db_sid = 'XE'

# 서버 소켓 생성
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# 소켓을 특정 포트에 바인딩
server_socket.bind(('192.168.50.43', 4000))

print('서버 실행 중')

# 클라이언트의 요청을 대기
server_socket.listen()

while True:
    # 클라이언트와 연결이 되면 데이터 수신
    client_socket, addr = server_socket.accept()
    data = client_socket.recv(1024)

    # 수신된 데이터 출력
    # print('Received from client:', data.decode())

    # 오라클 DB에 연결
    db_connection = cx_Oracle.connect(f"{db_user}/{db_password}@{db_host}:{db_port}/{db_sid}")
    cursor = db_connection.cursor()

    # SQL 쿼리 실행
    tableQuery = 'SELECT table_name FROM user_tables'
    indexQuery = "SELECT INDEX_NAME FROM USER_INDEXES WHERE TABLE_OWNER = 'ETL_ADMIN'"

    # 오라클 DB로부터 query를 실행한 결과를 가져옴
    # 테이블 가져오는 코드
    tableInfoDic = getOracleData(cursor, tableQuery)

    # 인덱스 가져오는 코드
    indexList = getOracleIndex(cursor, indexQuery)

    # 테이블 내용 가져오는 코드
    tableContentList = getOracleContent(cursor, tableQuery)

    # 딕셔너리로 묶는 코드
    oracleDic = getOracleDic(tableInfoDic, indexList, tableContentList)


    # datetime 객체를 문자열로 변환하는 함수
    def convert_datetime(obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

    # 쿼리 결과를 파일에 작성
    jsonData = json.dumps(oracleDic, default=convert_datetime)
    file_name = 'result.json'
    with open(file_name, 'w') as f:
        f.write(jsonData)

    # 파일을 읽으며 클라이언트에게 보내기
    with open(file_name, 'rb') as f:
        chunk = f.read(1024)
        while chunk:
            client_socket.send(chunk)
            chunk = f.read(1024)

    # 연결 종료
    client_socket.close()
    cursor.close()
    db_connection.close()

server_socket.close()