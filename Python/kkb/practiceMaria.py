import mysql.connector

# MariaDB 연결 정보
host = 'localhost'
port = 3306
user = 'root'
password = '0937'
db = 'sys'

# MariaDB 연결
connection = mysql.connector.connect(host=host, port=port, user=user, password=password, database=db)

# 연결 확인
if connection.is_connected():
    print('MariaDB connected.')

# SELECT문 실행
cursor = connection.cursor()
query = "SELECT * FROM std2"
cursor.execute(query)

# 결과 가져오기
results = cursor.fetchall()

# 결과 출력
for row in results:
    print(row)

# 연결 종료
connection.close()