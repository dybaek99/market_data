SELECT 
	a.TABLE_NAME '테이블명',
	b.ORDINAL_POSITION '순번', 
	b.COLUMN_NAME '필드명', 
	b.DATA_TYPE 'DATA TYPE', 
	b.COLUMN_TYPE '데이터길이', 
	b.COLUMN_KEY 'KEY', 
	b.IS_NULLABLE 'NULL값여부', 
	b.EXTRA '자동순번', 
	b.COLUMN_DEFAULT '기본값', 
	b.COLUMN_COMMENT '필드설명' 
FROM information_schema.TABLES a 
	JOIN information_schema.COLUMNS b 
	ON a.TABLE_NAME = b.TABLE_NAME 
	AND a.TABLE_SCHEMA = b.TABLE_SCHEMA 
WHERE a.TABLE_SCHEMA = 'invest' 
-- AND a.TABLE_NAME = '테이블명'   // 특정 테이블만 조회
ORDER BY a.TABLE_NAME, b.ORDINAL_POSITION