# Purpose: get the most recent date that we've loaded statcast data to BigQuery
# File Name: gbq_lookup_most_recent_data.py
# File Path: helper_functions/gbq_lookup_most_recent_data.py
# Author: John Tyler
# Created: 2023-04-08

# import the bigquery library
from google.cloud import bigquery

def get_most_recent_date(project_id, dataset_name, statcast_table_name, json_key_path):
    # create a client object using the `python-sandbox-31204-1b0c0c5b5f8e.json` service account key in service_account_keys folder
    client = bigquery.Client.from_service_account_json(json_key_path)
    # define the name of the dataset
    dataset_ref = client.dataset(dataset_name)
    # define the name of the table
    table_ref = dataset_ref.table(statcast_table_name)

    # check to see whether the table exists
    try:
        # try to load the table
        client.get_table(table_ref)
    except:
        # if the table doesn't exist print a message
        print('Table does not exist.')
        # return None
        return '2021-01-01'

    # get the most recent date that we've loaded statcast data
    query = f"""
    SELECT
        MAX(game_date) AS most_recent_date
    FROM
        `{project_id}.{dataset_name}.{statcast_table_name}`
    """

    # run the query 
    query_job = client.query(query)
    # get the results of the query
    results = query_job.result()

    # loop through the results
    for row in results:
        # get the most recent date that we've loaded statcast data
        most_recent_date = row.most_recent_date

    # return the most recent date that we've loaded statcast data as a string
    return most_recent_date.strftime('%Y-%m-%d')

if __name__ == '__main__':
    # get the most recent date that we've loaded statcast data
    most_recent_date = get_most_recent_date()
    # print the most recent date that we've loaded statcast data
    print(most_recent_date)