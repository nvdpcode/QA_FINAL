# config.py

# Connection strings
ORACLE_CONN_STR = "AGILE_RO/agilerospt@sulvdbz22:1522/agilespt"
SOLR_URL = "http://ol-bvsolr.formfactor.com:8983/solr/MEMO_test"

# SQL Queries
PARENT_QUERY = """
SELECT 
    P2P3.ITEM_NUMBER, 
    P2P3.DESCRIPTION AS DESCRIPTION, 
    N.DESCRIPTION AS SUBCLASS,
    (SELECT ENTRYVALUE FROM LISTENTRY WHERE ENTRYID = R.RELEASE_TYPE AND PARENTID = 4539) AS LIFECYCLE,
    (SELECT TEXT FROM AGILE_FLEX WHERE ATTID = 2000003227 AND ID = P2P3.ID) AS EXTENDED_DESC,
    U.FIRST_NAME || ' ' || U.LAST_NAME AS CREATED_BY,
    R.RELEASE_DATE,
    R.REV_NUMBER,
    (SELECT FIRST_NAME || ' ' || LAST_NAME FROM AGILEUSER WHERE ID = P2P3.LIST02) AS AUTHOR,
    (SELECT LISTAGG(FIRST_NAME || ' ' || LAST_NAME, '; ') WITHIN GROUP (ORDER BY FIRST_NAME || ' ' || LAST_NAME) 
     FROM AGILEUSER WHERE ID IN (
        SELECT REGEXP_SUBSTR(MULTILIST32,'[^,]+', 1, LEVEL) FROM DUAL
        CONNECT BY REGEXP_SUBSTR(MULTILIST32, '[^,]+', 1, LEVEL) IS NOT NULL)) AS DISTRIBUTION,
    (SELECT LISTAGG(FIRST_NAME || ' ' || LAST_NAME, '; ') WITHIN GROUP (ORDER BY FIRST_NAME || ' ' || LAST_NAME) 
     FROM AGILEUSER WHERE ID IN (
        SELECT REGEXP_SUBSTR(MULTILIST33,'[^,]+', 1, LEVEL) FROM DUAL
        CONNECT BY REGEXP_SUBSTR(MULTILIST33, '[^,]+', 1, LEVEL) IS NOT NULL)) AS SIGNER,
    (SELECT TEXT FROM AGILE_FLEX WHERE ATTID = 1568 AND ID = P2P3.ID) AS SUMMARY,
    (SELECT TEXT FROM AGILE_FLEX WHERE ATTID = 1567 AND ID = P2P3.ID) AS REFERENCE,
    (SELECT TEXT FROM AGILE_FLEX WHERE ATTID = 2000003297 AND ID = P2P3.ID) AS KEYWORD,
    (SELECT ENTRYVALUE FROM LISTENTRY WHERE ENTRYID = P2P3.LIST35 AND PARENTID = 2484515) AS CONFIDENTIAL
FROM 
    AGILE.ITEM_P2P3 P2P3
INNER JOIN 
    AGILE.REV R ON R.ITEM = P2P3.ID AND R.RELEASE_TYPE = 974
INNER JOIN 
    AGILE.NODETABLE N ON N.ID = P2P3.SUBCLASS
INNER JOIN 
    AGILEUSER U ON U.ID = P2P3.CREATE_USER
WHERE 
    N.DESCRIPTION = 'Memo Document'
    AND R.LATEST_FLAG = 1
    AND P2P3.DELETE_FLAG IS NULL
"""

CHILD_QUERY = """
SELECT 
    AGILE.ITEM.ITEM_NUMBER, 
    AGILE.FILES.FILENAME, 
    AGILE.FILES.FILE_TYPE, 
    AGILE.FILE_INFO.IFS_FILEPATH,
    AGILE.FILE_INFO.HFS_FILEPATH
FROM 
    AGILE.ITEM
JOIN 
    AGILE.ATTACHMENT_MAP ON AGILE.ATTACHMENT_MAP.PARENT_ID = AGILE.ITEM.ID AND AGILE.ITEM.CLASS = 9000
JOIN 
    AGILE.FILES ON AGILE.FILES.ID = AGILE.ATTACHMENT_MAP.FILE_ID
JOIN 
    AGILE.FILE_INFO ON AGILE.FILE_INFO.FILE_ID = AGILE.ATTACHMENT_MAP.FILE_ID
JOIN 
    AGILE.NODETABLE ON AGILE.NODETABLE.ID = AGILE.ITEM.SUBCLASS
WHERE 
    AGILE.NODETABLE.DESCRIPTION = 'Memo Document'
    AND AGILE.ITEM.ID NOT IN (
        45377136,
        49523767,
        45377145,
        24262189,
        24262167,
        24262240,
        24262312
    )
    AND AGILE.ITEM.DELETE_FLAG IS NULL
    AND AGILE.ATTACHMENT_MAP.PARENT_ID2 = (
        SELECT CHANGE FROM REV WHERE RELEASE_DATE = (
            SELECT MAX(R.RELEASE_DATE) FROM REV R, CHANGE C
            WHERE ITEM = (
                SELECT ID FROM ITEM WHERE ITEM_NUMBER = AGILE.ITEM.ITEM_NUMBER
            )
            AND C.ID = R.CHANGE
            AND C.CLASS = 6000
            AND C.DELETE_FLAG IS NULL
            AND C.RELEASE_DATE IS NOT NULL
        )
        AND ITEM = AGILE.ITEM.ID
    )
"""
