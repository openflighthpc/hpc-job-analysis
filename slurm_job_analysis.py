# python script to analyse the slurm job script, and persist the script information in the script
# 
# Create virtualenv and activate virtualenv
# python3.10 -m venv venv && source venv/bin/activate
#
# Install packages in the venv
# pip install langchain==0.1.0 pydt==0.1.0 mysql-connector-python==8.2.0
#
#
# Note: Update following variables:
# cluster_job_table_name: Name of cluster job table where slurmdbd stores information about all jobs executed in the cluster, can be found in  database of slurmdbd.
# cluster_job_script_table: Name of cluster job script table where slurmdbd stores information about all scripts of jobs executed in the cluster, can be found in  database of slurmdbd.
# output_slurm_job_table: Name of the table where results from LLM are stroed.
# db_name: Name of the slurmdbd database.
# db_user_name: Username of the slurmdbd database.
# db_password: Password of the slurmdbd database.
# db_host_name: Hostname of the slurmdbd database.
#
#
# Also update <cluster-name>_job_table in the script   
#
#
# Run the script 
# python script.py



import os
import mysql.connector
from typing import List
from pydantic import BaseModel, Field

from langchain.prompts import ChatPromptTemplate, HumanMessagePromptTemplate
from langchain_community.chat_models import ChatOllama
from langchain.output_parsers import PydanticOutputParser, OutputFixingParser
from langchain.schema import OutputParserException



model = "mixtral"  
cluster_job_table_name = 'my-cluster_job_table'
cluster_job_script_table = 'my-cluster_job_script_table'
output_slurm_job_table = 'slurm_job_info'
db_name = 'slurm_acct_db'
db_user_name = 'slurm'
db_password = ''
db_host_name = 'localhost'

# MySQL connection details
mysql_config = {
    'host': db_host_name,
    'user': db_user_name,
    'password': db_password,
    'database': db_name
}



class ScriptInfo(BaseModel):
    """
    Model for LLM output structure
    """
    language: str = Field(description="programming language of the script provided.")
    module: List[str] = Field(description="modules used in the script.")
    package: List[str] = Field(description="packages used in the script.")
    technique: List[str] = Field(description="techniques used in the script.")
    summary: str = Field(description="summary of the script.")

parser = PydanticOutputParser(pydantic_object=ScriptInfo)

# Update the prompt to match the new query and desired format.
prompt = ChatPromptTemplate(
    messages=[
        HumanMessagePromptTemplate.from_template(
            "answer the users question as best as possible.\n{format_instructions}\n{question}"
        )
    ],
    input_variables=["question"],
    partial_variables={
        "format_instructions": parser.get_format_instructions(),
    },
)

# chat model for mixtral
chat_model = ChatOllama(
    model="mixtral:latest",
)

def check_create_slurm_info_table_exist():
    """
    Function to check and create `output_slurm_job_table` where parsed result from
    LLM is stored.
    """
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{output_slurm_job_table}'")
        table_exists = cursor.fetchone() is not None
        if not table_exists:
            slurm_table_str = f"""create table {output_slurm_job_table} (
                                    id int primary key auto_increment,
                                    language varchar(255) NULL,
                                    module text NULL,
                                    package text NULL,
                                    technique text NULL,
                                    summary text NULL,
                                    user_id int NULL,
                                    job_id int NULL,
                                    error_summary text NULL
                                    );"""
            cursor.execute(slurm_table_str)
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Close the cursor and connection
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

def fetch_scripts_from_slurm():
    """
    Fetch all the job scripts from slurm that are stored by slurmdbd whenever a new job is scheduled to slurm.
    """
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        query = f"select id_job, id_user, batch_script from `{cluster_job_table_name}` as t1 join `{cluster_job_script_table}` as t2 ON t1.script_hash_inx = t2.hash_inx;"
        cursor.execute(query)
        column_details = cursor.fetchall()
        return column_details
    
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Close the cursor and connection
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

def check_job_exists(job_id):
    """
    
    """
    try:
        check_create_slurm_info_table_exist()
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        cursor.execute(f"select * from slurm_job_info where job_id = %s", (job_id,))
        record = cursor.fetchone()
        if record:
            return True
        return False

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Close the cursor and connection
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
   

def analyse_script(file_string):
    """
    Analyse job script using LLM model
    """

    query_string = (f"please tell programming languange in one word?\n"
    f"please tell the modules and packages used in single words?\n"
    f"please tell techniques used in single words?\n"
    f"Please tell the summary of the script?\n"
    f"please return the result in json\n"
    f"{file_string}\n"
    )
    _input = prompt.format_prompt(question=query_string)
    output = chat_model(_input.to_messages())
    return output

        
def parse_result(output, user_id, job_id):
    """
    Parse results and structure the results.
    """
    result_dict = {}
    try:
        parsed_res = parser.parse(output.content)
    except OutputParserException as e:
        print(e)
        new_parser = OutputFixingParser.from_llm(parser=parser, llm=ChatOllama())
        parsed_res = new_parser.parse(output.content)
        print("Fixed parsing errors.")

    
    result_dict.update({
        'language': parsed_res.language,
        'module': ','.join(list(parsed_res.module)),
        'package': ','.join(list(parsed_res.package)),
        'technique': ','.join(list(parsed_res.technique)),
        'summary': parsed_res.summary,
        'user_id': user_id, 
        'job_id': job_id,
        'error_summary': None})
    return result_dict

def populate_result_db(result_dict):
    """
    function to populate result in slurm_output_table that is provided by LLM.
    """
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()


        new_entry = (result_dict['language'], result_dict['module'], result_dict['package'], 
                     result_dict['technique'], result_dict['summary'], result_dict['user_id'],
                     result_dict['job_id'], result_dict['error_summary'])

        insert_query = f"INSERT INTO {output_slurm_job_table}(language, module, package, technique, summary, user_id, job_id, error_summary) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(insert_query, new_entry)
        connection.commit()
        

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Close the cursor and connection
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()


if __name__ == "__main__":
    # Fetch all the job_scripts from slurmdbd
    column_details = fetch_scripts_from_slurm()

    for column in column_details:
        job_id, user_id, file_string = column[0], column[1], column[2]
        if not check_job_exists(job_id=job_id):
            
            # Analyse the job script using LLM model(mixtral)
            llm_output = analyse_script(file_string=file_string)

            # Parse the result that is obtained from LLM model(mixtral)
            result_dict = parse_result(output=llm_output, user_id=user_id, job_id=job_id)

            # Populate the result in database
            populate_result_db(result_dict=result_dict)
            print(f'Job with ID: {job_id} Porcessed.')
        else:
            print(f'Job with ID: {job_id} Exists, Skipping.')
