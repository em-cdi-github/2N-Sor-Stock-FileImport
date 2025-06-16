--!jinja
{% if environment == 'PROD' %}
    {% set sufix = '' %}
{% else %}
    {% set sufix = '_'+ environment %}
{% endif %} 

{% set path_part_1 = 'governance_db'+ sufix +'.integration.'+project+'git_internal_repo/branches' %}
{% set path_part_2 = 'snowflake/databases/sor_and_stock/schemas' %}
{% set path_templates = path_part_1 + '/' + git_branch + '/templates' %}
{% set path_governance_db = path_part_1 + '/' + git_branch + '/' + path_part_2 %}
{% set dbname = 'GOVERNANCE_DB' + sufix %}
--**************************************************************PPROCESS DB**************************************************************************************
-- tables
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/process/tables/DISTRIBUTOR_SETTINGS.sql                      using(environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/process/tables/SOR_STOCK_PROCESS_STATUS.sql                  using(environment=>'{{ environment }}');
--__________________________________________________________________________________________________________________________________________
-- functions
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/process/functions/F_COUNTRY_CODE.sql                          using(environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/process/functions/F_DISTRIBUTOR_SETTINGS.sql                  using(environment=>'{{ environment }}');
--__________________________________________________________________________________________________________________________________________
-- procedures
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/process/procedures/SOR_STOCK_PROCESS_FILES.sql                 using(environment=>'{{ environment }}');
--__________________________________________________________________________________________________________________________________________
-- tasks     
--EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/****.sql                USING (environment=>'{{ environment }}');
--__________________________________________________________________________________________________________________________________________
-- stages     
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/process/stages/files_stage.sql                          using(environment=>'{{ environment }}');
--__________________________________________________________________________________________________________________________________________
--integration DB -- table data
--EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/****.sql                USING (environment=>'{{ environment }}');


--**************************************************************DATA DB**************************************************************************************
-- procedures
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_ADI.sql                    USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_ALLTRADE.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_ALLTRON.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_ANIXTER.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_AVALON.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_CEBEO.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_CIE.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_CONTROL.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_CREATEL.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_EDOX.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_EET.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_ELKOTECH_ROMANIA.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_ELMAT.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_ESPRINET.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_EUROSAT.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_GC24_EXPRESS.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_GENERAL.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_HERWECK.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_ITESA.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_LYDIS.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_OPREMA.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_PORTERALIA.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_PROVU.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_SALTECO.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_SAVANT.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_TEVAH.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_UNIPLUS.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_VARNET.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_VIDEOR.sql                USING (environment=>'{{ environment }}');
EXECUTE IMMEDIATE FROM @{{ path_governance_db }}/data/procedures/DATA_IMPORT_ZELIATECH.sql                USING (environment=>'{{ environment }}');