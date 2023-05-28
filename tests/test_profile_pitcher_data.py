from helper_functions.profile_pitcher_data import load_pitcher_summary_data, create_new_pitcher_df_with_features_and_tests, load_pitcher_profile_data, create_pitcher_summary_dataframe


df = create_new_pitcher_df_with_features_and_tests()
job_id = load_pitcher_profile_data(df)
print(f'Job ID: {job_id}')
print('The batter profile data has been loaded to BigQuery.')
summary_df = create_pitcher_summary_dataframe(df)
job_id, output_rows = load_pitcher_summary_data(summary_df)
print(f'Job ID: {job_id}')
print(f'Output Rows: {output_rows}')
print('The batter profile summary data has been loaded to BigQuery.')
