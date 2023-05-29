from helper_functions.config_bigquery import project_id, dataset_name, json_key_path
import pandas as pd
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_key_path

statcast_batter_table_name = 'homerun_batter_statcast_data'

# use pandas_gbq to read the homerun_batter_statcast_data table from BigQuery
homerun_batter_statcast_data = pd.read_gbq(f'SELECT * FROM `{project_id}.{dataset_name}.{statcast_batter_table_name}`',
                                           project_id=project_id, dialect='standard')

print(homerun_batter_statcast_data.head())

import matplotlib.pyplot as plt

plt.scatter(homerun_batter_statcast_data['release_spin_rate'], homerun_batter_statcast_data['launch_speed'])
plt.show()