USE ROLE sysadmin;
{% if environment == 'PROD' %}
    {% set sufix = '' %}
{% elif environment == 'TEST' %}
    {% set sufix = '_'+ environment %}
{% elif environment == 'DEV' %}
    {% set sufix = '_'+ environment %}
{% endif %} 

CREATE OR ALTER TABLE SOR_AND_STOCK{{ sufix }}.PROCESS.SOR_STOCK_PROCESS_STATUS (
	ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	FILENAME VARCHAR(16777216),
	STATUS VARCHAR(16777216),
	STATUS_TEXT VARCHAR(16777216),
	STATUS_DATA VARIANT,
	INSERT_DT TIMESTAMP_NTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('Europe/Prague', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)),
	LAST_UPDATE_DT TIMESTAMP_NTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('Europe/Prague', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9))
);