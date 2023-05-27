from google.cloud import bigquery
import datetime


def get_most_recent_date(project_id, dataset_name, table_name, json_key_path):
    client = bigquery.Client.from_service_account_json(json_key_path)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    try:
        client.get_table(table_ref)
        print(f'Table {project_id}.{dataset_name}.{table_name} exists.')
    except:
        print(f'Table {project_id}.{dataset_name}.{table_name} does not exist. Backfilling data to 2021-01-01.')
        return '2021-01-01'

    query = f"""
    SELECT
        MAX(game_date) AS most_recent_date
    FROM
        `{project_id}.{dataset_name}.{table_name}`
    """

    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        most_recent_date = row.most_recent_date

    if most_recent_date is not None:
        # Check if most_recent_date is a datetime object
        if isinstance(most_recent_date, datetime.datetime):
            return most_recent_date.strftime('%Y-%m-%d')
        else:
            return most_recent_date.strftime('%Y-%m-%d')
    else:
        # Handle the case when most_recent_date is None
        # For example, return a default date or raise a custom exception
        return '2021-03-01'  # Replace '2021-01-01' with an appropriate default date value