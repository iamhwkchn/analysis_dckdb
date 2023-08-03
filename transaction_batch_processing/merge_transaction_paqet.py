import os
import pyarrow.parquet as pq
import pandas as pd

def merge_all_parquet_files():
    # Create the "merged_all" directory if it doesn't exist
    if not os.path.exists('merged/merged_all'):
        os.makedirs('merged/merged_all')

    # Get a list of all Parquet files in the "merged" directory
    for x in range(0,2):
        parquet_files = [f'merged/transactions_merged_{i}.parquet' for i in range(x*5 , x*5 +5)]

        # Read and concatenate the Parquet files
        dfs = [pq.read_table(file).to_pandas() for file in parquet_files]
        merged_df = pd.concat(dfs, ignore_index=True)

        # Save the merged DataFrame as a new Parquet file
        merged_parquet_file = 'merged/merged_all/transactions_merged_all_{x}.parquet'
        table = pq.Table.from_pandas(merged_df)
        pq.write_table(table, merged_parquet_file)

    # Call the function to merge all Parquet files
merge_all_parquet_files()

# import dask.dataframe as dd

# def merge_all_parquet_files():
#     # Create the "merged_all" directory if it doesn't exist
#     if not os.path.exists('merged/merged_all'):
#         os.makedirs('merged/merged_all')

#     # Get a list of all Parquet files in the "merged" directory
#     parquet_files = [f'merged/transactions_merged_{i}.parquet' for i in range(5)]

#     # Read and concatenate the Parquet files using Dask
#     df = dd.read_parquet(parquet_files, engine='pyarrow')

#     # Save the merged DataFrame as a new Parquet file
#     merged_parquet_file = 'merged/merged_all/transactions_merged_all.parquet'
#     df.to_parquet(merged_parquet_file, engine='pyarrow')

# # Call the function to merge all Parquet files
# merge_all_parquet_files()