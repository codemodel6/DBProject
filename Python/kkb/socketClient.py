import socket
import json

from handleOracleClient.oracleTableFunction import oracleTableFunctions


# 서버 IP, 포트
host = '192.168.50.43'
port = 4000

# 서버에 연결
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((host, port))

# 서버에 파일 요청
file_name = 'result.json'
client_socket.sendall(file_name.encode())

# 파일 내용 수신
file_content = b''
while True:
    data = client_socket.recv(1024)
    if not data:
        break
    file_content += data

# 수신된 파일 저장
with open(file_name, 'wb') as f:
    f.write(file_content)

# 연결 종료
client_socket.close()

# 수신된 파일 내용을 JSON 형식으로 파싱
json_data = file_content.decode()
result = json.loads(json_data)

# table 값
serverTable = result['table']

# index 값
serverIndex = result['index']

# JSON 데이터 내용 확인
# for key, value in result.items():
#     print(f"key: {key}")
#     print(f"value: {', '.join(value)}")
#     print()

# 마리아에 줄 딕셔너리 리스트 생성
# mariaList = []
# for i in serverTable:
#     mariaList.append({})
mariaDic = {}

#---- Table 값 관리 -----#
# 오라클 함수들을 작성할 페이지로 이동
oracleTableFunctions(serverTable)
print(mariaDic)


