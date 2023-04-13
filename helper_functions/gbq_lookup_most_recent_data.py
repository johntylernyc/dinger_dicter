from google.cloud import bigquery

def get_most_recent_date(project_id, dataset_name, statcast_batter_table_name, json_key_path):
    # create a client object using the `python-sandbox-31204-1b0c0c5b5f8e.json` service account key in service_account_keys folder
    client = bigquery.Client.from_service_account_json(json_key_path)
    # define the name of the dataset
    dataset_ref = client.dataset(dataset_name)
    # define the name of the table
    table_ref = dataset_ref.table(statcast_batter_table_name)

    # check to see whether the table exists
    try:
        # try to load the table
        client.get_table(table_ref)
        # if the table exists print a message that shows the full table name and that it exists
        print(f'Table {project_id}.{dataset_name}.{statcast_batter_table_name} exists.')
    except:
        # if the table doesn't exist print a message
        print(f'Table {project_id}.{dataset_name}.{statcast_batter_table_name} does not exist. Backfilling data to 2021-01-01.')
        # return the first date we're willing to backload the table to as a string.
        return '2021-01-01'

    # get the most recent date that we've loaded statcast data
    query = f"""
    SELECT
        MAX(game_date) AS most_recent_date
    FROM
        `{project_id}.{dataset_name}.{statcast_batter_table_name}`
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