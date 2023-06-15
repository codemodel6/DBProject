## fastAPI
fastAPI를 통한 서버와 클라이언트 통신
result.json을 현재 폴더에 저장하고 불러온다

## socket
웹소켓을 이용한 서버와 클라이언트 통신
result.json을 다운로드 폴더에 저장하고 불러온다

## scriptMaria
오라클의 json 데이터인 Result_File을 마리아 스크립트로 만들어줌
로직은 handleMaria 폴더 안에 있음
결과는 maria_script.sql

## Result.json 파일 구조
TableData : 테이블 정보  
-TableInfo : 테이블 정보(메타데이터)  
--UserInfomanager : ETL_ADMIN  
---User_Info : 테이블이름 Student  
----Columns : 정보들 name age  
----PKInfo : 키값 정보  
-RealData : (실제데이터)  