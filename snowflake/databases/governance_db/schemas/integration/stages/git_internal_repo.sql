USE ROLE sysadmin;

  {% if environment == 'PROD' %}
      {% set sufix = '' %}
	{% else %}
      {% set sufix = '_'+ environment %}
  {% endif %} 

CREATE STAGE IF NOT EXISTS governance_db{{ sufix }}.integration.{{ project }}git_internal_repo 
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'into this stage are automatically copied files from GIT repository as local copy... 
    because referencing to repository itself every time is slow.';
ALTER GIT REPOSITORY governance_db{{ sufix }}.deployment.{{ project }}DEPLOYMENT_REPOSITORY{{ sufix }} FETCH;

COPY FILES
  INTO '@governance_db{{ sufix }}.integration.{{ project }}git_internal_repo/branches/{{ git_branch }}/'
  FROM '@governance_db{{ sufix }}.deployment.{{ project }}DEPLOYMENT_REPOSITORY{{ sufix }}/branches/{{ git_branch }}/';




 