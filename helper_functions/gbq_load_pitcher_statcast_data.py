import pandas as pd
from google.cloud import bigquery

def schema_from_dataframe(df):
    schema = []
    for column_name, data_type in zip(df.columns, df.dtypes):
        schema.append(bigquery.SchemaField(column_name, data_type.name.upper()))
    return schema

def load_data_to_gbq(daily_statcast_data_csv, dataset_name, table_name, json_key_path, project_id):
    client = bigquery.Client(project=project_id).from_service_account_json(json_key_path)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    df = pd.read_csv(daily_statcast_data_csv)
    inferred_schema = schema_from_dataframe(df)

    try:
        table = client.get_table(table_ref)
        print('Table already exists.')

        if table.schema != inferred_schema:
            print('Schema has changed, updating schema.')
            table.schema = inferred_schema
            client.update_table(table, ['schema'])

    except:
        print('Table does not exist. Creating table with schema.')
        table = bigquery.Table(table_ref, schema=inferred_schema)
        table = client.create_table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        write_disposition='WRITE_APPEND',
        schema=inferred_schema
    )

    with open(daily_statcast_data_csv, 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_name, table_name))