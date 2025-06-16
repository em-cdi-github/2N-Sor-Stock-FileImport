USE ROLE sysadmin;
{% if environment == 'PROD' %}
    {% set sufix = '' %}
{% elif environment == 'TEST' %}
    {% set sufix = '_'+ environment %}
{% elif environment == 'DEV' %}
    {% set sufix = '_'+ environment %}
{% endif %} 

CREATE OR REPLACE PROCEDURE SOR_AND_STOCK{{ sufix }}.DATA.DATA_IMPORT_GENERAL("SOURCE_TABLE" VARCHAR, "FINAL_TABLE" VARCHAR, "TYPE" VARCHAR, "PROFILE_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
        e EXCEPTION (-20001, $$Unsupported value in input variable TYPE$$);
BEGIN
    IF (UPPER(TYPE) = $$SOR$$) THEN
        DELETE FROM IDENTIFIER(:FINAL_TABLE)
            WHERE YEAR_MONTH = (
                SELECT 
                    MAX(TO_VARCHAR(INSERT_DATE,$$YYYY-MM$$)) AS YEAR_MONTH
                FROM 
                    IDENTIFIER(:SOURCE_TABLE)
                ) AND
                PROFILE = :PROFILE_NAME
                ;
                
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS DATE_OF_SALES STRING;
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS CUSTOMER_NAME STRING;
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS CUSTOMER_TAX_ID STRING;
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS CUSTOMER_VAT STRING;
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS "2N_ITEM_REFERENCE_NUMBER_(SKU)" STRING;
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS "2N_PRODUCT_NAME" STRING;
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS QUANTITY STRING;
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS COUNTRY_OF_DESTINATION STRING;
    
        
        INSERT INTO IDENTIFIER(:FINAL_TABLE)(
            YEAR_MONTH,
            PROFILE,
            DATE_OF_SALES,
            CUSTOMER_NAME,
            CUSTOMER_TAX_ID,
            CUSTOMER_VAT,
            ITEM_REFERENCE_NUMBER,
            PRODUCT_NAME,
            QUANTITY,
            COUNTRY_OF_DESTINATION
            )
        WITH 
        SOR_DATA AS (
            SELECT 
                TO_VARCHAR(INSERT_DATE,$$YYYY-MM$$) AS YEAR_MONTH,
                :PROFILE_NAME                       AS PROFILE,
                REGEXP_REPLACE(TO_VARCHAR(TRIM(DATE_OF_SALES)), $$[a-zA-Z()]$$) AS REPLACED_DATE_OF_SALES,
                CUSTOMER_NAME                       AS CUSTOMER_NAME,
                CUSTOMER_TAX_ID                     AS CUSTOMER_TAX_ID,
                CUSTOMER_VAT                        AS CUSTOMER_VAT,
                "2N_ITEM_REFERENCE_NUMBER_(SKU)"    AS ITEM_REFERENCE_NUMBER,
                "2N_PRODUCT_NAME"                   AS PRODUCT_NAME,
                QUANTITY                            AS QUANTITY,
                PROCESS.F_COUNTRY_CODE(COUNTRY_OF_DESTINATION) AS COUNTRY_OF_DESTINATION
            FROM 
                IDENTIFIER(:SOURCE_TABLE))
        SELECT 
            YEAR_MONTH                          AS YEAR_MONTH,
            PROFILE                             AS PROFILE,
            CASE 
                WHEN REPLACED_DATE_OF_SALES IS NULL THEN NULL
                WHEN LENGTH(REPLACED_DATE_OF_SALES)< 10
                    THEN 
                        CASE 
                            WHEN RLIKE(REPLACED_DATE_OF_SALES,$$^\\d{2}-\\d{4}$$) = TRUE
                                THEN TO_DATE(REPLACED_DATE_OF_SALES,$$MM-YYYY$$)
                            WHEN RLIKE(REPLACED_DATE_OF_SALES,$$^\\d{4}-\\d{2}$$) = TRUE
                                THEN TO_DATE(REPLACED_DATE_OF_SALES,$$YYYY-MM$$)
                            ELSE REPLACED_DATE_OF_SALES
                        END
                ELSE
                    CASE 
                        WHEN RLIKE(REPLACED_DATE_OF_SALES,$$^\\d{2}\\/\\d{2}\\/\\d{4}$$) = TRUE
                            THEN TO_DATE(REPLACED_DATE_OF_SALES,$$DD/MM/YYYY$$)
                        WHEN RLIKE(REPLACED_DATE_OF_SALES,$$^\\d{2}-\\d{2}-\\d{4}$$) = TRUE
                            THEN TO_DATE(REPLACED_DATE_OF_SALES,$$DD-MM-YYYY$$)
                        WHEN RLIKE(REPLACED_DATE_OF_SALES,$$^\\d{2}\\.\\d{2}\\.\\d{4}$$) = TRUE
                            THEN  TO_DATE(REPLACED_DATE_OF_SALES,$$DD.MM.YYYY$$)
                        ELSE TO_DATE(REPLACED_DATE_OF_SALES)
                    END
            END                                 AS DATE_OF_SALES,
            CUSTOMER_NAME                       AS CUSTOMER_NAME,
            CUSTOMER_TAX_ID                     AS CUSTOMER_TAX_ID,
            CUSTOMER_VAT                        AS CUSTOMER_VAT,
            ITEM_REFERENCE_NUMBER               AS ITEM_REFERENCE_NUMBER,
            PRODUCT_NAME                        AS PRODUCT_NAME,
            QUANTITY                            AS QUANTITY,
            COUNTRY_OF_DESTINATION              AS COUNTRY_OF_DESTINATION
        FROM
            SOR_DATA
        WHERE
            QUANTITY > 0
            AND TRY_TO_DATE(DATE_OF_SALES) IS NOT NULL;
            
        RETURN $$Data inserted successfully into $$ || TYPE || $$ final table: Rows inserted: $$ || SQLROWCOUNT;
    ELSEIF (UPPER(TYPE) = $$STOCK$$) THEN
        DELETE FROM IDENTIFIER(:FINAL_TABLE)
            WHERE YEAR_MONTH = (
                SELECT 
                    MAX(TO_VARCHAR(INSERT_DATE,$$YYYY-MM$$)) AS YEAR_MONTH
                FROM 
                    IDENTIFIER(:SOURCE_TABLE)
                ) AND
                PROFILE = :PROFILE_NAME;
                
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS "2N_ITEM_REFERENCE_NUMBER_(SKU)" STRING;
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS "2N_PRODUCT_NAME" STRING;
        ALTER TABLE IF EXISTS IDENTIFIER(:SOURCE_TABLE) ADD COLUMN IF NOT EXISTS QUANTITY_OF_STOCK STRING;
                
        INSERT INTO IDENTIFIER(:FINAL_TABLE)(
            YEAR_MONTH,
            PROFILE,
            ITEM_REFERENCE_NUMBER,
            PRODUCT_NAME,
            QUANTITY_OF_STOCK
            )
        SELECT 
            TO_VARCHAR(INSERT_DATE,$$YYYY-MM$$) AS YEAR_MONTH,
            :PROFILE_NAME                       AS PROFILE,
            "2N_ITEM_REFERENCE_NUMBER_(SKU)"    AS ITEM_REFERENCE_NUMBER,
            "2N_PRODUCT_NAME"                   AS PRODUCT_NAME,
            QUANTITY_OF_STOCK                   AS QUANTITY_OF_STOCK
        FROM 
            IDENTIFIER(:SOURCE_TABLE)
        WHERE
            QUANTITY_OF_STOCK > 0; 
            
        RETURN $$Data inserted successfully into $$ || TYPE || $$ final table: Rows inserted: $$ || SQLROWCOUNT;
    ELSE 
        RAISE e;
    END IF;
END';