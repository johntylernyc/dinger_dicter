import pandas as pd
from google.cloud import bigquery
from datetime import datetime

def pandas_dtype_to_bigquery_dtype(dtype):
    dtype_mapping = {
        'int64': 'INT64',
        'float64': 'FLOAT64',
        'bool': 'BOOL',
        'object': 'STRING',
        'datetime64': 'TIMESTAMP',
        # Add more data types if necessary
    }
    return dtype_mapping.get(dtype, 'STRING')

def schema_from_dataframe(df):
    schema = []
    for column_name, data_type in zip(df.columns, df.dtypes):
        # Check if the column_name is 'release_spin_rate' and set the type to 'FLOAT64'
        if column_name == 'release_spin_rate':
            schema.append(bigquery.SchemaField(column_name, 'FLOAT64'))
        else:
            schema.append(bigquery.SchemaField(column_name, pandas_dtype_to_bigquery_dtype(data_type.name)))
    return schema

def load_data_to_gbq(daily_statcast_data, dataset_name, table_name, json_key_path, project_id):
    client = bigquery.Client(project=project_id).from_service_account_json(json_key_path)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    # Use the DataFrame directly, without reading from a CSV
    df = daily_statcast_data
    # Force the 'launch_speed' column to be float
    df['launch_speed'] = df['launch_speed'].astype(float)
    df['launch_angle'] = df['launch_angle'].astype(float)
    df['release_speed'] = df['release_speed'].astype(float)
    df['release_spin_rate'] = df['release_spin_rate'].astype(float)

    # Add a new column 'load_timestamp' with the current date and time
    df['load_timestamp'] = datetime.utcnow()

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
        schema=inferred_schema,
        write_disposition='WRITE_APPEND',
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_name, table_name))
