USE ROLE sysadmin;
{% if environment == 'PROD' %}
    {% set sufix = '' %}
{% elif environment == 'TEST' %}
    {% set sufix = '_'+ environment %}
{% elif environment == 'DEV' %}
    {% set sufix = '_'+ environment %}
{% endif %} 

CREATE OR REPLACE PROCEDURE SOR_AND_STOCK{{ sufix }}.DATA.DATA_IMPORT_OPREMA("SOURCE_TABLE" VARCHAR, "FINAL_TABLE" VARCHAR, "TYPE" VARCHAR, "PROFILE_NAME" VARCHAR)
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
        SELECT 
            TO_VARCHAR(INSERT_DATE,$$YYYY-MM$$)         AS YEAR_MONTH,
            :PROFILE_NAME                               AS PROFILE,
            TRY_TO_DATE(s.DATE)                         AS DATE_OF_SALES,
            "CUST._NAME"                                AS CUSTOMER_NAME,
            $$NA$$                                      AS CUSTOMER_TAX_ID,
            "CUST._VAT_NO."                             AS CUSTOMER_VAT,
            "MANUFACTURER_PART_NO."                     AS ITEM_REFERENCE_NUMBER,
            "DESCRIPTION"                               AS PRODUCT_NAME,
            TRY_TO_DECIMAL(QTY)                         AS QUANTITY,
            F_COUNTRY_CODE("CUST._COUNTRY")             AS COUNTRY_OF_DESTINATION
        FROM 
            IDENTIFIER(:SOURCE_TABLE) s
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
            "MANUFACTURER_PART_NO."             AS ITEM_REFERENCE_NUMBER,
            DESCRIPTION                         AS PRODUCT_NAME,
            TRY_TO_DECIMAL(QTY)                 AS QUANTITY_OF_STOCK
        FROM 
            IDENTIFIER(:SOURCE_TABLE)
        WHERE 
            QUANTITY_OF_STOCK > 0 ;           
        RETURN $$Data inserted successfully into $$ || TYPE || $$ final table: Rows inserted: $$ || SQLROWCOUNT;
    ELSE 
        RAISE e;
    END IF;
END';