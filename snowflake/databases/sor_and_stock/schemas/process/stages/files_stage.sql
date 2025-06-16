USE ROLE sysadmin;

  {% if environment == 'PROD' %}
      {% set sufix = '' %}
	{% else %}
      {% set sufix = '_'+ environment %}
  {% endif %} 

CREATE STAGE IF NOT EXISTS SOR_AND_STOCK{{ sufix }}.PROCESS.FILES 
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'stage for files to be processed in DEV env.';
