import json

from handleOracleClient.oracleTableFunction import oracleTableFunctions

# 파일 경로
file_path = "C:/Users/kbkim/Downloads/result.json"

# 파일 읽어오기
def read_file_contents(file_path):
    with open(file_path, "r") as file:
        contents = file.read()
    return contents

# 읽어온 파일을 json 형식으로 변수에 저장
serverData = read_file_contents(file_path)
result = json.loads(serverData)
data = json.dumps(result, indent=4)

# table 값
serverTable = result['table']

# index 값
serverIndex = result['index']

# 최종적으로 만들 json문
mariaDic = {}

#---- Table 값 관리 -----#
# 오라클 함수들을 작성할 페이지로 이동

oracleTable = oracleTableFunctions(serverTable)
mariaDic['TableData'] = oracleTable
print(mariaDic)


