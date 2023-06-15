# 파이썬을 이용한 오라클DB -> 마리아DB 스크립트 파일 전달

## 1. 웹소켓을 사용한 오라클 db의 생성 정보 넘기기

cx_Oracle을 이용해 파이썬과 오라클 db를 연결하였습니다.
이후 파이썬에서 오라클db로 SELECT table_name FROM user_tables 쿼리를 보내
데이터베이스 스키마에 있는 모든 테이블의 이름을 가져왔습니다.

또한 SELECT INDEX_NAME FROM USER_INDEXES WHERE TABLE_OWNER = 'ETL_ADMIN' 쿼리문을 사용해
'ETL_ADMIN'이라는 테이블 소유자에 속한 모든 인덱스의 이름을 가져왔습니다.

이후 2개의 쿼리의 결과를 합쳐 result.json이라는 파일을 만들고 이 파일을 클라이언트로 전달했습니다.

클라이언트는 result.json 파일을 불러와 딕셔너리 리스트의 값을 알맞게 잘라내었으며
mariaDB에 형식에 맞게 oracleDB의 쿼리문을 변경하는 작업을 진행하였습니다.

## 2. FastAPI를 사용한 오라클 db의 생성 정보 넘기기

FastAPI를 사용해 제 로컬의 주소에서 다른 로컬의 주소를 연결할 수 있습니다. 명령프롬프트에서
실행해야 하는 FastAPI를 파이참에서 실행하면 실행할 수 있게 만들었으며 ngrok를 통해 다른 로컬이
제 로컬 주소에 들어와 fastAPI의 실행 html을 들어올 수 있습니다. html 파일의 다운로드 버튼을 누르면
오라클DB의 result.json을 가져올 수 있습니다.

# 3. 오라클DB의 정보를 마리아DB의 문법으로 변경
오라클DB를 받고 마리아DB의 문법을 적용해 변경하였습니다. 코드를 실행하면 각 스키마에 맞는 마리아DB의
SQL문이 작성되고 이 SQL문을 전체 실행하면 오라클DB와 내용이 같은 마리아DB를 만들 수 있습니다.
CREATE, ALTER, INSERT문과 주석을 가져옵니다.
