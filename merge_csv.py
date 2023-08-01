import os
import pandas as pd

#import os
import pandas as pd

# Function to merge CSV files for each table and convert to Parquet
def merge_csv_and_convert_to_parquet(table_name, num_batches):
    dfs = []
    for batch_num in range(num_batches + 1):
        file_path = f'{table_name}/{table_name}_batch_{batch_num}.csv'
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            dfs.append(df)

    # Concatenate dataframes
    merged_df = pd.concat(dfs, ignore_index=True)

    # Create the "merged" directory if it doesn't exist
    if not os.path.exists('merged_small'):
        os.makedirs('merged_small')

    # Save as merged CSV
    csv_file = f'merged_small/{table_name}.csv'
    merged_df.to_csv(csv_file, index=False)

    # Convert to Parquet format
    # parquet_file = f'merged/{table_name}_merged.parquet'
    # merged_df.to_parquet(parquet_file, engine='pyarrow')
# Merge and convert for each table
merge_csv_and_convert_to_parquet('users', num_batches=50) 
merge_csv_and_convert_to_parquet('products', num_batches=2)
merge_csv_and_convert_to_parquet('transactions', num_batches=100)