import json

from handleMaria.realData import handleRealData
from handleMaria.tableData import handleTableData

# JSON 파일 불러오기
with open('Result_File.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# 마리아 db에 넣을 리스트

# UserInfomanager 가져오기(주인 이름 | ETL_ADMIN)
mariaScript = ""
userInfoKeys = list(data['TableData']["TableInfo"].keys())
realDataKeys = list(data['RealData'].keys())

# Colums로 maria script 만들기
# 테이블 CREATE문 가져오기
tableData = handleTableData(data,realDataKeys)

# 쿼리문 완성
mariaScript = tableData

with open("maria_script.sql", "w", encoding="utf-8") as file:
    file.write(mariaScript)

print("maria_script.sql 파일이 생성되었습니다.")