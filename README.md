# 파이썬을 이용한 오라클DB -> 마리아DB

## 1. 웹소켓을 사용한 오라클 db의 생성 정보 넘기기

<img src="https://github.com/codemodel6/DBProject/assets/110915850/cf58a61b-d685-4836-95b7-0d2e8eca47b6"/>
<img src="https://github.com/codemodel6/DBProject/assets/110915850/9b3b4572-2b53-4260-a4c4-517366dd306b"/>
<img src="https://github.com/codemodel6/DBProject/assets/110915850/83ae5318-18de-4433-9789-b23f6e63f80b"/>

cx_Oracle을 이용해 파이썬과 오라클 db를 연결하였습니다.
이후 파이썬에서 오라클db로 SELECT table_name FROM user_tables 쿼리를 보내
데이터베이스 스키마에 있는 모든 테이블의 이름을 가져왔습니다.

또한 SELECT INDEX_NAME FROM USER_INDEXES WHERE TABLE_OWNER = 'ETL_ADMIN' 쿼리문을 사용해
'ETL_ADMIN'이라는 테이블 소유자에 속한 모든 인덱스의 이름을 가져왔습니다.

이후 2개의 쿼리의 결과를 합쳐 result.json이라는 파일을 만들고 이 파일을 클라이언트로 전달했습니다.

클라이언트는 result.json 파일을 불러와 딕셔너리 리스트의 값을 알맞게 잘라내었으며
mariaDB에 형식에 맞게 oracleDB의 쿼리문을 변경하는 작업을 진행하였습니다.

## 2. FastAPI를 사용한 오라클 db의 생성 정보 넘기기

<img src="https://github.com/codemodel6/DBProject/assets/110915850/3bde1bf2-d054-43b4-ba21-c92c574cac99"/>
<img height="500px" src="https://github.com/codemodel6/DBProject/assets/110915850/d8f84928-a1c0-41d2-af93-e42b88c6e1ee"/>
<img height="500px" width="350px" src="https://github.com/codemodel6/DBProject/assets/110915850/06cc0025-4fcb-40d0-8f36-e8f3e391b428"/>

FastAPI를 사용해 제 로컬의 주소에서 다른 로컬의 주소를 연결할 수 있습니다. 명령프롬프트에서
실행해야 하는 FastAPI를 파이참에서 실행하면 실행할 수 있게 만들었으며 ngrok를 통해 다른 로컬이
제 로컬 주소에 들어와 fastAPI의 실행 html을 들어올 수 있습니다. html 파일의 다운로드 버튼을 누르면
오라클DB의 result.json을 가져올 수 있습니다.

# 3. 오라클DB의 정보를 마리아DB의 문법으로 변경

<img src="https://github.com/codemodel6/DBProject/assets/110915850/f0e163ba-f48d-4e38-8665-d88e59d582cd"/>
<img height="500px" src="https://github.com/codemodel6/DBProject/assets/110915850/8570237d-e354-45b9-80f2-e05824c9f853"/>
<img src="https://github.com/codemodel6/DBProject/assets/110915850/c996ea34-7abd-4709-89ba-3b43ff1eb3b0"/>

오라클DB를 받고 마리아DB의 문법을 적용해 변경하였습니다. 코드를 실행하면 각 스키마에 맞는 마리아DB의
SQL문이 작성되고 이 SQL문을 전체 실행하면 오라클DB와 내용이 같은 마리아DB를 만들 수 있습니다.
CREATE, ALTER, INSERT문과 주석을 가져옵니다.
