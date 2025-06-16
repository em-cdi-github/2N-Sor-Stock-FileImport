USE ROLE sysadmin;
{% if environment == 'PROD' %}
    {% set sufix = '' %}
{% elif environment == 'TEST' %}
    {% set sufix = '_'+ environment %}
{% elif environment == 'DEV' %}
    {% set sufix = '_'+ environment %}
{% endif %} 

CREATE OR REPLACE PROCEDURE SOR_AND_STOCK{{ sufix }}.PROCESS.SOR_STOCK_PROCESS_FILES()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl','xlrd')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook
import pandas as pd
from datetime import datetime
import json
import xlrd
 
def main(session):
    fileDict = fileDictList(session)
    db_schema = "SOR_AND_STOCK{{ sufix }}.PROCESS"
    appDictData = { 
                "fileDict" : fileDict,
                "db_schema" : db_schema,
                "fileDictData" : []
                }
    fileCounter = 0

    if len(fileDict) == 0:
        appDictData["Response"] = "No data in stage dictionary. process finished."
        return appDictData
    for file in fileDict:
        try:
            fileCounter = fileCounter + 1
            remove_matches = ["HIDDEN", "HELPER", "READ ME", "README", "STOCKVALORE", "PARAMETRI"]
            sor_matches = ["SOR", "SELL OUT", "SELLOUT", "SELLOUTREPORT", "SALES OUT", "SALES_OUT", "SALESOUT","POS-REPORT","POS REPORT","POS DETAIL","VVA"]
            stock_matches = ["STOCK","INVENTORY REPORT", "INVENTORYREPORT", "INVENTORY_REPORT", "INVENTORY", "BESTANDSSTRUKTURANALYSE", "STAV_SKLADU", "STAV SKLADU"]
            sor_df =      pd.DataFrame()
            stock_df =    pd.DataFrame()
            fileDictData = {
                "fileCounter" :     fileCounter,
                "file_path" :       file["name"],
                "status_message" :  "NA",
                "status" :          "NA",
                }
            fileDictData["filename"] =    fileDictData["file_path"].split("/")[-1]
            statusTableInfo(session, fileDictData, "INSERT", "START", "new file in processing")
            fileDictData["ext_type"] =    fileDictData["filename"].split(".")[-1]
            fileDictData["scope_url"] =   create_scope_url(session,db_schema,fileDictData["file_path"])

            with SnowflakeFile.open(fileDictData["scope_url"], "rb") as f:
                #_____________________________________________________________________ 
                try:
                    if fileDictData["ext_type"].upper() in ("XLS","XLSX"):
                        df = pd.ExcelFile(f)
                        sheets = df.sheet_names
                        #for sheet in sheets:
                        #    if any(x in sheet.upper() for x in remove_matches):
                        #        sheets.remove(sheet)
                        all_sheets = df.sheet_names
                        for sheet in all_sheets:
                            data_df = pd.read_excel(df,sheet_name=sheet,dtype="string")
                            if data_df.empty or any(x in sheet.upper() for x in remove_matches):
                                sheets.remove(sheet)
                        #return sheets
                        fileDictData["sheets"] = sheets
                        if len(sheets) == 1: 
                            fileDictData["onlyOneSheet"] = True 
                        else: 
                            fileDictData["onlyOneSheet"] = False
                        if ("EUROSAT" in fileDictData["filename"].upper() and (any(x in fileDictData["filename"].upper() for x in sor_matches))):
                            #this is Eurosat sor file with special handling
                            #_____________________________________________________________________
                            fileDictData["condition_type"] = "EXCEL_SOR_EUROSAT"
                            fileDictData["profile_prop_sor"] = check_profile(session,fileDictData["filename"], "SOR")
                            fileDictData["sor_excel"] = sor_eurosat(session,fileDictData,f,sheets)
                        else:
                            #_____________________________________________________________________
                            invalid_sheets = 0
                            for sheet in sheets:
                                if (any(x in sheet.upper() for x in sor_matches) or 
                                    (fileDictData["onlyOneSheet"] and (any(x in fileDictData["filename"].upper() for x in sor_matches)))):
                                    
                                    fileDictData["condition_type"] = "EXCEL_SOR"
                                    fileDictData["profile_prop_sor"] = check_profile(session,fileDictData["filename"], "SOR")
                                    fileDictData["sor_excel"] = sor(session,fileDictData,f,sheet)
                                elif (any(x in sheet.upper() for x in stock_matches) or 
                                    (fileDictData["onlyOneSheet"] and (any(x in fileDictData["filename"].upper() for x in stock_matches)))):
                                    
                                    fileDictData["condition_type"] = "EXCEL_STOCK"
                                    fileDictData["profile_prop_stock"] = check_profile(session,fileDictData["filename"], "STOCK")
                                    fileDictData["stock_excel"] = stock(session,fileDictData,f,sheet)
                                else:
                                    invalid_sheets = invalid_sheets + 1 
                                if invalid_sheets == len(sheets):
                                    #all sheets in file are invalid
                                    fileDictData["status_message"] = """error, not supported file_type: {0}. Filename or sheets not match SOR or STOCK filename pattern for XLS. All sheets are invalid for process: {1}.""".format(fileDictData["filename"],sheets)
                                    fileDictData["status"] = "EXCEPITON"
                    #_____________________________________________________________________ 
                    elif fileDictData["ext_type"].upper() in ("CSV"):
                        if (any(x in fileDictData["filename"].upper() for x in sor_matches)):
    
                            fileDictData["condition_type"] = "CSV_SOR"
                            fileDictData["profile_prop_sor"] = check_profile(session,fileDictData["filename"], "SOR")
                            fileDictData["sor_csv"] = sor(session,fileDictData,f,0)
                        elif (any(x in fileDictData["filename"].upper() for x in stock_matches)):
    
                            fileDictData["condition_type"] = "CSV_STOCK"
                            fileDictData["profile_prop_stock"] = check_profile(session,fileDictData["filename"], "STOCK")
                            fileDictData["stock_csv"] = stock(session,fileDictData,f,0)
                        else:
                            fileDictData["status_message"] = """error, not supported file_type: {0}. Filename not match SOR or STOCK filename pattern for CSVs.""".format(fileDictData["filename"])
                            fileDictData["status"] = "EXCEPITON"
                    #_____________________________________________________________________ 
                    else:
                        fileDictData["status_message"] = """error, not supported file extension: {0}""".format(fileDictData["filename"])
                        fileDictData["status"] = "EXCEPITON"
                except Exception as e:
                    fileDictData["status_message"] = """error: {0}""".format(e)
                    fileDictData["status"] = "EXCEPITON"
                    appDictData["fileDictData"].append(fileDictData)
            #___________________________________________________________________
            fileDictData["archivation"] = fileArchive(session,db_schema,fileDictData["file_path"])
            if fileDictData["status"] != "EXCEPITON":
                fileDictData["status_message"] = "Succesfully processed and archived"
                fileDictData["status"] = "FINISH"
            statusTableInfo(session, fileDictData, "UPDATE", fileDictData["status"],fileDictData["status_message"])
            appDictData["fileDictData"].append(fileDictData)
        except Exception as e:
            appDictData["Response"] = "Error: {0}.".format(e)
            #return appDictData
            raise (e)        
    return appDictData
#______________________________________________________________________________________________________________
def sor(session,fileDictData,f,sheet):
    if fileDictData["ext_type"].upper() in ("CSV"):
        sor_df = pd.read_csv(
                            f, 
                            header=fileDictData["profile_prop_sor"]["list_header"],
                            skiprows=(fileDictData["profile_prop_sor"]["list_skip_rows"]),
                            dtype="string",
                            encoding="ISO-8859-1",
                            sep=fileDictData["profile_prop_sor"]["csv_delimiter"]
                            )
    elif fileDictData["ext_type"].upper() in ("XLS","XLSX"):
        sor_df = pd.read_excel(
                            f, 
                            sheet_name=sheet,
                            header=fileDictData["profile_prop_sor"]["list_header"],
                            skiprows=(fileDictData["profile_prop_sor"]["list_skip_rows"]),
                            dtype="string"
                            )
    else:
        fileDictData["status_message"] = "Exception in sor function - invalid extension"
        fileDictData["status"] = "EXCEPTION"
        
    sor_df.columns = sor_df.columns.str.upper()
    sor_df.columns = sor_df.columns.str.strip()
    sor_df.columns = sor_df.columns.str.replace(" ", "_")
    sor_df.columns = sor_df.columns.str.replace("\\n", "_")
    sor_df = sor_df.dropna(axis=0, how="all")
    sor_df = sor_df.dropna(axis=1, how="all")
    sor_df.insert(0,"INSERT_DATE",datetime.today() ,True)
    #---------------------------------------------
    if fileDictData["profile_prop_sor"]["distributor_name"] == "CREATEL":
        for index, row in sor_df.iterrows():
            if( pd.isnull(row["JOURNAL"]) and 
                pd.isnull(row["DATE"]) and 
                pd.isnull(row["CUSTOMER"]) and 
                pd.isnull(row["TAX_ID"]) and 
                pd.isnull(row["VAT_ID"]) and 
                pd.isnull(row["CTRY"]) ):
                sor_df["JOURNAL"].values[index] =   sor_df["JOURNAL"].values[index-1]
                sor_df["DATE"].values[index] =      sor_df["DATE"].values[index-1]
                sor_df["CUSTOMER"].values[index] =  sor_df["CUSTOMER"].values[index-1]
                sor_df["TAX_ID"].values[index] =    sor_df["TAX_ID"].values[index-1]
                sor_df["VAT_ID"].values[index] =    sor_df["VAT_ID"].values[index-1]
                sor_df["CTRY"].values[index] =      sor_df["CTRY"].values[index-1]
    #---------------------------------------------
    sor_sdf = session.create_dataframe(sor_df)
    sor_sdf.write.mode("overwrite").save_as_table(fileDictData["profile_prop_sor"]["distributor_name"] + "_SOR")
    
    sql_string = fileDictData["profile_prop_sor"]["other_settings"]["final_insert"]
    df = session.sql(sql_string).collect()
    sorDict = {"ImportResponse" : "Data inserted succesfully"}
    return sorDict
#______________________________________________________________________________________________________________
def sor_eurosat(session,fileDictData,f,sheets):
    sor_df = pd.DataFrame() 
    for sheet in sheets:
        tmp_sor_df = pd.read_excel(
                            f, 
                            sheet_name=sheet,
                            header=fileDictData["profile_prop_sor"]["list_header"],
                            skiprows=(fileDictData["profile_prop_sor"]["list_skip_rows"]),
                            dtype="string"
                            )
        tmp_sor_df.columns = tmp_sor_df.columns.str.upper()
        tmp_sor_df.columns = tmp_sor_df.columns.str.strip()
        tmp_sor_df.columns = tmp_sor_df.columns.str.replace(" ", "_")
        tmp_sor_df.columns = tmp_sor_df.columns.str.replace("\\n", "_")
        tmp_sor_df = tmp_sor_df.dropna(axis=0, how="all")
        tmp_sor_df = tmp_sor_df.dropna(axis=1, how="all")
        tmp_sor_df.insert(0,"INSERT_DATE",datetime.today() ,True)
        sor_df = pd.concat ([sor_df,tmp_sor_df], sort = False)

    #---------------------------------------------
    sor_sdf = session.create_dataframe(sor_df)
    sor_sdf.write.mode("overwrite").save_as_table(fileDictData["profile_prop_sor"]["distributor_name"] + "_SOR")
    
    sql_string = fileDictData["profile_prop_sor"]["other_settings"]["final_insert"]
    df = session.sql(sql_string).collect()
    sorDict = {"ImportResponse" : "Data for EUROSAT inserted succesfully"}
    return sorDict
#______________________________________________________________________________________________________________
def stock(session,fileDictData,f,sheet):
    if fileDictData["ext_type"].upper() in ("CSV"):
        stock_df = pd.read_csv(
                            f, 
                            header=fileDictData["profile_prop_stock"]["list_header"],
                            skiprows=(fileDictData["profile_prop_stock"]["list_skip_rows"]),
                            dtype="string",
                            encoding="ISO-8859-1",
                            sep=fileDictData["profile_prop_stock"]["csv_delimiter"]
                            )
    elif fileDictData["ext_type"].upper() in ("XLS","XLSX"):
        stock_df = pd.read_excel(
                            f, 
                            sheet_name=sheet,
                            header= fileDictData["profile_prop_stock"]["list_header"],
                            skiprows=(fileDictData["profile_prop_stock"]["list_skip_rows"]),
                            dtype="string"
                            )
    else:
        fileDictData["status_message"] = "Exception in stock function - invalid extension"
        fileDictData["status"] = "EXCEPTION"
        
    stock_df.columns = stock_df.columns.str.upper()
    stock_df.columns = stock_df.columns.str.strip()
    stock_df.columns = stock_df.columns.str.replace(" ", "_")
    stock_df.columns = stock_df.columns.str.replace("\\n", "_")
    stock_df = stock_df.dropna(axis=0, how="all")
    stock_df = stock_df.dropna(axis=1, how="all")
    stock_df.insert(0,"INSERT_DATE",datetime.today() ,True)
    #---------------------------------------------
    stock_sdf = session.create_dataframe(stock_df)
    stock_sdf.write.mode("overwrite").save_as_table(fileDictData["profile_prop_stock"]["distributor_name"] + "_STOCK")
    
    sql_string = fileDictData["profile_prop_stock"]["other_settings"]["final_insert"]
    df = session.sql(sql_string).collect()
    stockDict = {"ImportResponse" : "Data inserted succesfully"}
    return stockDict
#______________________________________________________________________________________________________________
def create_scope_url(session,db_schema,path):
    stage_name = "@"+db_schema + "." + path.split("/")[0]
    filepath_without_stage = path.replace(path.split("/")[0]+"/","")
    scoped_url_str = f"SELECT BUILD_SCOPED_FILE_URL($${stage_name}$$,$${filepath_without_stage}$$)"
    df = session.sql(scoped_url_str).collect()
    scoped_url = df[0][0]
    return scoped_url

#______________________________________________________________________________________________________________
def check_profile(session,filename, filetype):
    profile_prop = []
    f_distributor_settings = f"SELECT * FROM TABLE (SOR_AND_STOCK{{ sufix }}.PROCESS.F_DISTRIBUTOR_SETTINGS(''{filename}'',''{filetype}''))"
    df = session.sql(f_distributor_settings).collect()
    if len(df) > 0 :
        profile_prop = {    
                "distributor_name": df[0][0],
                "parsing_pattern":  df[0][1],
                "list_skip_rows":   df[0][2],
                "list_header":      df[0][3],
                "list_names":       df[0][4],
                "priority":         df[0][5],
                "other_settings":   json.loads(df[0][6]),
                "csv_delimiter":    df[0][7]
                    }
        if  profile_prop["list_skip_rows"] == "None":
            profile_prop["list_skip_rows"] = None
        else:
            profile_prop["list_skip_rows"] = list(map(int, profile_prop["list_skip_rows"].split(",")))
        
        if  profile_prop["list_header"] == "None":
            profile_prop["list_header"] = None
        else:
            profile_prop["list_header"] = list(map(int, profile_prop["list_header"].split(",")))
        
        if  profile_prop["list_names"] == "None":
            profile_prop["list_names"] = None
        else:
            profile_prop["list_names"] = profile_prop["list_names"].split(",")
        if  profile_prop["csv_delimiter"] == "None" or is_string_empty(profile_prop["csv_delimiter"]):
            profile_prop["csv_delimiter"] = None
    else:
        raise Exception(f"Exception in check_profile function: No data matches given parameters. filename: ''{filename}'' and filetype: ''{filetype}''")
    return profile_prop
    
#______________________________________________________________________________________________________________
# return list of files in the stage/folder
def fileDictList(session):
    try:
        sql_string = """LIST @SOR_AND_STOCK{{ sufix }}.PROCESS.files/IN/"""
        df = session.sql(sql_string).collect()
        sql_string = """SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"""
        df = session.sql(sql_string)
        fileDict = df.to_pandas().to_dict("records")
    except Exception as e:
        raise (e)
    return fileDict

#______________________________________________________________________________________________________________
# archive processed file
def fileArchive(session,db_schema,filename_withpath):
    try:
        date = datetime.today().strftime("%Y%m%d_%H%M%S")
        sql_string = """
                        COPY FILES INTO 
                            $$@{0}.files/IN_ARC/{2}_$$ 
                        FROM 
                            $$@{0}.{1}$$ 
                        DETAILED_OUTPUT = TRUE;
                    """.format(db_schema,filename_withpath,date)
        df = session.sql(sql_string).collect()
        sql_string = """
                        REMOVE 
                            $$@{0}.{1}$$;
                    """.format(db_schema,filename_withpath)
        df = session.sql(sql_string).collect()
    except Exception as e:
        fileDictData["status_message"] = """Exception in fileArchive function: {0}""".format(e)
        fileDictData["status"] = "EXCEPTION"
    ArcDict = {"ArchiveResponse" : "Data archived succesfully"}
    return ArcDict
#______________________________________________________________________________________________________________
def is_string_empty(my_string):
    return my_string is None or len(str(my_string).strip()) == 0  

#__________________________________________________________________________________________________________________________________________________        
def statusTableInfo(session, fileDictData, type, status, status_text):
    try:
        if type.upper() == "INSERT":
            #---------------------------------------------
            # initial insert into log table
            sql_string = """
                    INSERT INTO SOR_AND_STOCK{{ sufix }}.PROCESS.SOR_STOCK_PROCESS_STATUS (FILENAME,STATUS,STATUS_TEXT) 
                        values ($${0}$$,$${1}$$,$${2}$$);
                    """.format(fileDictData["filename"], status, status_text)
            df = session.sql(sql_string).collect()
            #---------------------------------------------
            # select inserted row id - AT(STATEMENT=>LAST_QUERY_ID()) - is pointing to the moment of the last query id, despite new rows were inserted.
            sql_string = """
                    SELECT MAX(ID) AS ID FROM SOR_AND_STOCK{{ sufix }}.PROCESS.SOR_STOCK_PROCESS_STATUS AT(STATEMENT=>LAST_QUERY_ID());
                    """
            df = session.sql(sql_string).collect()
            fileDictData["process_id"] = df[0].ID
                
        else:
            sql_string = """
                    UPDATE SOR_AND_STOCK{{ sufix }}.PROCESS.SOR_STOCK_PROCESS_STATUS 
                        SET 
                            STATUS = $${0}$$, 
                            STATUS_TEXT = $${1}$$, 
                            STATUS_DATA = $${3}$$,
                            LAST_UPDATE_DT = CONVERT_TIMEZONE($$Europe/Prague$$,CURRENT_TIMESTAMP)::TIMESTAMP_NTZ 
                    WHERE ID = {2}""".format(
                            status, 
                            status_text, 
                            fileDictData["process_id"],
                            json.dumps(fileDictData).replace("$","/$"))
            df = session.sql(sql_string).collect()      
    except Exception as e:
        raise (e)
';