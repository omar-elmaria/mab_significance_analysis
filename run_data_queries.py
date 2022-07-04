# This .py script runs the queries in the data_queries.sql file
import datetime
from google.cloud import bigquery
from datetime import datetime

def run_data_queries_func():
    # Instantiate the BQ client
    client = bigquery.Client(project = 'logistics-data-staging-flat')

    # Read the SQL file
    f = open("G:/My Drive/APAC/Autopricing/Bayesian Statistics and MAB/mab_significance_analysis/data_queries_v5.sql", "r")
    sql_script = f.read()
    f.close()

    # Run the SQL script
    parent_job = client.query(sql_script).result()

    # Print a message that shows that the script ran successfully
    print("The BQ script was executed successfully at {}".format(datetime.now()))