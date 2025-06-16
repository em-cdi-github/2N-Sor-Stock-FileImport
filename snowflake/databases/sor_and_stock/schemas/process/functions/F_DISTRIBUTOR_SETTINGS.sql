USE ROLE sysadmin;
{% if environment == 'PROD' %}
    {% set sufix = '' %}
{% elif environment == 'TEST' %}
    {% set sufix = '_'+ environment %}
{% elif environment == 'DEV' %}
    {% set sufix = '_'+ environment %}
{% endif %} 

CREATE OR REPLACE FUNCTION SOR_AND_STOCK{{ sufix }}.PROCESS.F_DISTRIBUTOR_SETTINGS("FILENAME" VARCHAR, "FILE_TYPE" VARCHAR)
RETURNS TABLE ("DISTRIBUTOR_NAME" VARCHAR, "PARSING_PATTERN" VARCHAR, "LIST_SKIP_ROWS" VARCHAR, "LIST_HEADER" VARCHAR, "LIST_NAMES" VARCHAR, "PRIORITY" NUMBER(38,0), "OTHER_SETTINGS" VARCHAR, "CSV_DELIMITER" VARCHAR)
LANGUAGE SQL
AS '
    WITH
    -----------------
    MATCH AS (
        SELECT 
            ID      AS ID, 
            VALUE   AS PATTERN 
        FROM 
            SOR_AND_STOCK{{ sufix }}.PROCESS.DISTRIBUTOR_SETTINGS, 
            LATERAL SPLIT_TO_TABLE(SOR_AND_STOCK{{ sufix }}.PROCESS.DISTRIBUTOR_SETTINGS.NAME_PATTERNS, '','')
        WHERE
            UPPER(file_type) = TYPE),
    -----------------
    RESULT AS
        (SELECT DISTINCT TOP 1
            CASE 
                WHEN UPPER(filename) ILIKE ANY(PATTERN) THEN DISTRIBUTOR_NAME
                ELSE ''ERROR: DISTRIBUTOR NAME NOT FOUND!''
            END     AS DISTRIBUTOR_NAME,
            CASE 
                WHEN UPPER(filename) ILIKE ANY(PATTERN) THEN PARSING_PATTERN
                ELSE ''ERROR: PARSING PATTERN NOT FOUND!''
            END     AS PARSING_PATTERN,
            CASE 
                WHEN UPPER(filename) ILIKE ANY(PATTERN) THEN LIST_SKIP_ROWS
                ELSE ''ERROR: PARSING PATTERN NOT FOUND!''
            END     AS LIST_SKIP_ROWS,            
            CASE 
                WHEN UPPER(filename) ILIKE ANY(PATTERN) THEN LIST_HEADER
                ELSE ''ERROR: PARSING PATTERN NOT FOUND!''
            END     AS LIST_HEADER,
            CASE 
                WHEN UPPER(filename) ILIKE ANY(PATTERN) THEN LIST_NAMES
                ELSE ''ERROR: PARSING PATTERN NOT FOUND!''
            END     AS LIST_NAMES,
            CASE 
                WHEN UPPER(filename) ILIKE ANY(PATTERN) THEN 1
                ELSE 0
            END     AS PARSING_PRIORITY,
            CASE 
                WHEN UPPER(filename) ILIKE ANY(PATTERN) THEN OTHER_SETTINGS
                ELSE ''ERROR: PARSING PATTERN NOT FOUND!''
            END     AS OTHER_SETTINGS,
            CASE 
                WHEN UPPER(filename) ILIKE ANY(PATTERN) THEN CSV_DELIMITER
                ELSE ''ERROR: PARSING PATTERN NOT FOUND!''
            END     AS CSV_DELIMITER
        FROM 
            SOR_AND_STOCK{{ sufix }}.PROCESS.DISTRIBUTOR_SETTINGS D JOIN 
            MATCH M             ON D.ID = M.ID
        WHERE
            D.TYPE = UPPER(file_type)
        ORDER BY 
            PARSING_PRIORITY DESC)
    -----------------
    SELECT * FROM RESULT WHERE DISTRIBUTOR_NAME != ''ERROR: DISTRIBUTOR NAME NOT FOUND!''
    ';