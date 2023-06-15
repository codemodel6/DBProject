CREATE TABLE USER_LOG (
	USER_ID VARCHAR (100)  ,
	BOUGHT_ITEM VARCHAR (50)  ,
	ACTION_TIME DATETIME   
);

/* 유저들이 구매한 아이템 기록이 저장된 테이블입니다.
 * USER_ID : None
 * BOUGHT_ITEM : None
 * ACTION_TIME : None
*/

CREATE TABLE USER_LEVEL (
	USER_ID VARCHAR (20) NOT NULL ,
	PLAYER_LEVEL INT (100) NOT NULL 
);

/* 유저들의 레벨이 저장된 테이블입니다.
 * USER_ID : None
 * PLAYER_LEVEL : 레벨 정보
*/

CREATE TABLE USER_INFO (
	USER_ID VARCHAR (20) NOT NULL ,
	USER_NAME VARCHAR (50) NOT NULL ,
	PRIMARY KEY (USER_ID)
);

/* 유저들의 정보가 담긴 테이블입니다.
 * USER_ID : None
 * USER_NAME : 이름
*/

CREATE TABLE USER_CREATEDDATE (
	USER_ID VARCHAR (20) NOT NULL ,
	CREATED_DATE DATETIME  NOT NULL ,
	PRIMARY KEY (USER_ID)
);

/* 유저 계정이 생성된 날짜입니다.
 * USER_ID : None
 * CREATED_DATE : None
*/

CREATE TABLE DOUBLE_PKT (
	ALLIDX VARCHAR (100) NOT NULL ,
	ALLIDS VARCHAR (100) NOT NULL ,
	NAME VARCHAR (100) NOT NULL ,
	COLUMN1 DOUBLE  NOT NULL ,
	FLOATEST INT (38) NOT NULL ,
	PRIMARY KEY (ALLIDX,ALLIDS)
);

/* 두개의 PK를 가진 테이블을 실험하기 위한 테이블입니다.
 * ALLIDX : 아이디 테스트
 * ALLIDS : 아이디기본 테스트
 * NAME : 이름
 * COLUMN1 : 이건 플로트입니다.
 * FLOATEST : 이건 플로트가 아닙니다.
*/

CREATE TABLE DOUBLE_FKT (
	ANSIS VARCHAR (100) NOT NULL ,
	UTFS VARCHAR (100) NOT NULL ,
	EUCKR VARCHAR (100) NOT NULL 
);

/* None
 * ANSIS : ANSI 캐릭터 테이블
 * UTFS : UTF 8 캐릭터 테이블
 * EUCKR : EUC-KR 캐릭터 테이블
*/


INSERT INTO USER_LOG (USER_ID,BOUGHT_ITEM,ACTION_TIME)
VALUES
	('GN0_001','EPIC_100_13_1','2020-05-20 10:50:59'),
	('GN0_001','EPIC_100_13_1','2020-05-20 11:00:00'),
	('TVK_S02','EPIC_100_17_3','2020-05-20 18:05:00'),
	('TVK_B02','EPIC_100_13_4','2020-05-21 18:07:02'),
	('TVK_B02','EPIC_100_17_4','2020-05-21 19:09:15'),
	('GNS_001','EPIC_100_17_2','2020-05-21 22:45:10'),
	('TVK_B02','EPIC_100_13_4','2020-05-21 23:59:59'),
	('TVK_B01','EPIC_100_13_3','2020-05-22 00:01:01'),
	('GNS_001','EPIC_100_13_3','2020-05-22 00:03:14'),
	('TVK_B04','EPIC_100_17_5','2020-05-22 00:06:56');

INSERT INTO USER_LEVEL (USER_ID,PLAYER_LEVEL)
VALUES
	('GN0_001','30'),
	('GNS_001','28'),
	('TVK_B01','43'),
	('TVK_B02','15'),
	('TVK_B03','16'),
	('TVK_S01','19'),
	('TVK_S02','18'),
	('TVK_B04','15');

INSERT INTO USER_INFO (USER_ID,USER_NAME)
VALUES
	('GN0_001','세츠나'),
	('GNS_001','키라'),
	('TVK_B01','히메코'),
	('TVK_B02','키아나'),
	('TVK_B03','메이'),
	('TVK_B04','브로냐'),
	('TVK_S01','듀란달'),
	('TVK_S02','리타');

INSERT INTO USER_CREATEDDATE (USER_ID,CREATED_DATE)
VALUES
	('GN0_001','2019-01-02'),
	('GNS_001','2019-01-02'),
	('TVK_B01','2019-01-03'),
	('TVK_B03','2019-01-03'),
	('TVK_B02','2019-01-03'),
	('TVK_S01','2019-01-05'),
	('TVK_S02','2019-01-05'),
	('TVK_B04','2019-01-05');

INSERT INTO DOUBLE_PKT (ALLIDX,ALLIDS,NAME,COLUMN1,FLOATEST)
VALUES
	('1','G','TEST','7.9','7');


ALTER TABLE USER_LOG
ADD CONSTRAINT BuyLog_FK_User_Info
FOREIGN KEY (USER_ID) REFERENCES ETL_ADMIN.USER_INFO (USER_ID)
ON DELETE SET NULL;

/*
ALTER TABLE USER_LOG
ADD CONSTRAINT FK_PRODUCT_CODE
FOREIGN KEY (BOUGHT_ITEM) REFERENCES TEST_USER1.PRODUCT_CODE (PRODUCT_ID)
ON DELETE SET NULL;
*/

ALTER TABLE USER_LEVEL
ADD CONSTRAINT Level_FK_User_Info
FOREIGN KEY (USER_ID) REFERENCES ETL_ADMIN.USER_INFO (USER_ID)
ON DELETE CASCADE;

ALTER TABLE USER_CREATEDDATE
ADD CONSTRAINT FK_User_Info
FOREIGN KEY (USER_ID) REFERENCES ETL_ADMIN.USER_INFO (USER_ID)
ON DELETE CASCADE;

ALTER TABLE DOUBLE_FKT
ADD CONSTRAINT FK_DOUBLE_PKT
FOREIGN KEY (ANSIS,UTFS) REFERENCES ETL_ADMIN.DOUBLE_PKT (ALLIDX,ALLIDS)
;

