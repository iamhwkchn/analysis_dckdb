import os
import pandas as pd

# Function to merge CSV files for each table and convert to Parquet
def merge_csv_and_convert_to_parquet(table_name, num_batches, chunk_size, merge_interval):
    for batch in range(num_batches // merge_interval):
        dfs = []
        for batch_num in range(batch * merge_interval, (batch + 1) * merge_interval):
            file_path = f'{table_name}/{table_name}_batch_{batch_num}.csv'
            if os.path.exists(file_path):
                for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                    dfs.append(chunk)

        # Concatenate dataframes
        merged_df = pd.concat(dfs, ignore_index=True)

        # Create the "merged" directory if it doesn't exist
        if not os.path.exists('merged'):
            os.makedirs('merged')

        # Save as merged CSV
        csv_file = f'merged/{table_name}_merged_{batch}.csv'
        merged_df.to_csv(csv_file, index=False)

        # Convert to Parquet format
        parquet_file = f'merged/{table_name}_merged_{batch}.parquet'
        merged_df.to_parquet(parquet_file, engine='pyarrow')

# Batch processing parameters
num_batches = 1000
chunk_size = 5000  # Adjust this based on your system's memory capacity
merge_interval = 100

# Batch processing for the "transactions" table, merging every 100 batches
merge_csv_and_convert_to_parquet('transactions', num_batches, chunk_size, merge_interval)

