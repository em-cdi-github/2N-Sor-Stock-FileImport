USE ROLE sysadmin;
{% if environment == 'PROD' %}
    {% set sufix = '' %}
{% elif environment == 'TEST' %}
    {% set sufix = '_'+ environment %}
{% elif environment == 'DEV' %}
    {% set sufix = '_'+ environment %}
{% endif %} 

CREATE OR REPLACE PROCEDURE SOR_AND_STOCK{{ sufix }}.DATA.DATA_IMPORT_SALTECO("SOURCE_TABLE" VARCHAR, "FINAL_TABLE" VARCHAR, "TYPE" VARCHAR, "PROFILE_NAME" VARCHAR)
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
            TO_VARCHAR(INSERT_DATE,$$YYYY-MM$$)     AS YEAR_MONTH,
            :PROFILE_NAME                           AS PROFILE,
            TRY_TO_DATE(BELEGDATUM,$$DD.MM.YYYY$$)  AS DATE_OF_SALES,
            KUNDENNUMMER                            AS CUSTOMER_NAME,
            $$NA$$                                  AS CUSTOMER_TAX_ID,
            $$NA$$                                  AS CUSTOMER_VAT,
            BESTELLNUMMER                           AS ITEM_REFERENCE_NUMBER,
            ARTIKELBEZEICHNUNG                      AS PRODUCT_NAME,
            TRY_TO_DECIMAL(SPLIT_PART(MENGE, $$.$$, 1)) AS QUANTITY,
            F_COUNTRY_CODE(LAND)                    AS COUNTRY_OF_DESTINATION
        FROM 
            IDENTIFIER(:SOURCE_TABLE)
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
            BESTELLNUMMER                       AS ITEM_REFERENCE_NUMBER,
            BEZEICHNUNG                         AS PRODUCT_NAME,
            TRY_TO_DECIMAL(SPLIT_PART(LAGERBESTAND, $$,$$, 1)) AS QUANTITY_OF_STOCK
        FROM 
            IDENTIFIER(:SOURCE_TABLE)
        WHERE 
            QUANTITY_OF_STOCK > 0;
            
        RETURN $$Data inserted successfully into $$ || TYPE || $$ final table: Rows inserted: $$ || SQLROWCOUNT;
    ELSE 
        RAISE e;
    END IF;
END';