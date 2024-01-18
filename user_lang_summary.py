# python script to analyse highlighting the most frequently used programming languages
# by the user.
# 
# Create virtualenv and activate virtualenv
# python3.10 -m venv venv && source venv/bin/activate
#
# Install packages in the venv
# pip install  mysql-connector-python==8.2.0
#
#
# db_name: Name of the slurmdbd database.
# db_user_name: Username of the slurmdbd database.
# db_password: Password of the slurmdbd database.
# db_host_name: Hostname of the slurmdbd database.
# 
#
# Run the script 
# python get-script.py
# output => {'1000': {'language': ['bash', 'Python'], 'job_count': 5}}



import mysql.connector

db_name = 'slurm_acct_db'
db_user_name = 'slurm'
db_password = ''
db_host_name = 'localhost'
output_slurm_job_table = 'slurm_job_info'

# MySQL connection details
mysql_config = {
    'host': db_host_name,
    'user': db_user_name,
    'password': db_password,
    'database': db_name
}
# Establish a connection to MySQL
try:
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{output_slurm_job_table}'")
    table_exists = cursor.fetchone() is not None
    if table_exists:
        query = f"select user_id, count(*) as job_count, language from {output_slurm_job_table} group by user_id, language;"
        cursor.execute(query)
        column_details = cursor.fetchall()

    result_dict = {}
    for column in column_details:
        user_id, job_count, language = column[0], column[1], column[2]
        if str(user_id) not in result_dict:
            result_dict[str(user_id)] = {'language': [], 'job_count': 0}
        result_dict[str(user_id)]['language'].append(language)
        result_dict[str(user_id)]['job_count'] += job_count

    print(result_dict)
        
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    # Close the cursor and connection
    if 'cursor' in locals() and cursor is not None:
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
