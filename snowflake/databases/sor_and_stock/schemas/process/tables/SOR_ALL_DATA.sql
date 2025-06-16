USE ROLE sysadmin;
{% if environment == 'PROD' %}
    {% set sufix = '' %}
{% elif environment == 'TEST' %}
    {% set sufix = '_'+ environment %}
{% elif environment == 'DEV' %}
    {% set sufix = '_'+ environment %}
{% endif %} 

create or ALTER TABLE SOR_AND_STOCK{{ sufix }}.PROCESS.SOR_ALL_DATA (
	YEAR_MONTH VARCHAR(16777216),
	PROFILE VARCHAR(16777216),
	DATE_OF_SALES DATE,
	CUSTOMER_NAME VARCHAR(16777216),
	CUSTOMER_TAX_ID VARCHAR(16777216),
	CUSTOMER_VAT VARCHAR(16777216),
	ITEM_REFERENCE_NUMBER VARCHAR(16777216),
	PRODUCT_NAME VARCHAR(16777216),
	QUANTITY NUMBER(38,0),
	COUNTRY_OF_DESTINATION VARCHAR(16777216)
);