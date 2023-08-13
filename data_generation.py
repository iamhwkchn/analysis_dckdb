
import os
import random
import csv
from faker import Faker
from multiprocessing import Pool
import pandas as pd

fake = Faker()


# create directories for batches
def create_directories(table_name):
    if not os.path.exists(table_name):
        os.makedirs(table_name)

# Generate fake data for Transactions table
def generate_transactions_batch(batch_num, batch_size):
    start_index = batch_num * batch_size + 1

    with open(f'transactions/transactions_batch_{batch_num}.csv', 'w', newline='') as csvfile:
        fieldnames = ['transaction_id', 'user_id', 'product_id', 'product_name', 'amount', 'transaction_date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(batch_size):
            transaction_id = start_index + i
            user_id = random.randint(1001, 2000)
            product_id = 'P' + str(random.randint(100, 999))
            product_name = fake.word()
            amount = round(random.uniform(10.0, 100.0), 2)
            transaction_date = fake.date_time_this_year()

            writer.writerow({
                'transaction_id': transaction_id,
                'user_id': user_id,
                'product_id': product_id,
                'product_name': product_name,
                'amount': amount,
                'transaction_date': transaction_date
            })

# Generate fake data for User dimension table
def generate_users_batch(batch_num, batch_size):
    start_index = batch_num * batch_size + 1001

    with open(f'users/users_batch_{batch_num}.csv', 'w', newline='') as csvfile:
        fieldnames = ['user_id', 'user_name', 'email', 'date_of_birth', 'address', 'state', 'country']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(batch_size):
            user_id = start_index + i
            user_name = fake.name()
            email = fake.email()
            date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=80)
            address = fake.address()
            state = fake.state()
            country = fake.country()

            writer.writerow({
                'user_id': user_id,
                'user_name': user_name,
                'email': email,
                'date_of_birth': date_of_birth,
                'address': address,
                'state': state,
                'country': country
            })

# Generate fake data for Product dimension table
def generate_products_batch(batch_num, batch_size):
    with open(f'products/products_batch_{batch_num}.csv', 'w', newline='') as csvfile:
        fieldnames = ['product_id', 'product_name', 'category', 'price']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        categories = ['Electronics', 'Accessories', 'Tools', 'Home', 'Clothing']

        for i in range(batch_size):
            product_id = 'P' + str(random.randint(100, 999))
            product_name = fake.word()
            category = random.choice(categories)
            price = round(random.uniform(10.0, 200.0), 2)

            writer.writerow({
                'product_id': product_id,
                'product_name': product_name,
                'category': category,
                'price': price
            })

# Function to merge CSV files for each table and convert to Parquet
def merge_csv_and_convert_to_parquet(table_name, num_batches):
    dfs = []
    for batch_num in range(num_batches + 1):
        file_path = f'{table_name}/{table_name}_batch_{batch_num}.csv'
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            dfs.append(df)

    merged_df = pd.concat(dfs, ignore_index=True)

    if not os.path.exists('merged'):
        os.makedirs('merged')

    csv_file = f'merged/{table_name}.csv'
    if os.path.exists(csv_file):
        existing_df = pd.read_csv(csv_file)
        merged_df = pd.concat([existing_df, merged_df], ignore_index=True)

    
    merged_df.to_csv(csv_file, index=False)

    # Convert to Parquet format
    parquet_file = f'merged/{table_name}_merged.parquet'
    merged_df.to_parquet(parquet_file, engine='pyarrow')

if __name__ == '__main__':

    total_rows_transactions = 1000
    total_rows_users = 100
    total_rows_products = 20
    batch_size = 10

    num_batches_transactions = total_rows_transactions // batch_size
    num_batches_users = total_rows_users // batch_size
    num_batches_products = total_rows_products // batch_size

    # Create directories for each table
    create_directories('users')
    create_directories('products')
    create_directories('transactions')

    with Pool(processes=5) as pool:
        pool.starmap(generate_transactions_batch, [(batch_num, batch_size) for batch_num in range(num_batches_transactions)])

    with Pool(processes=5) as pool:
        pool.starmap(generate_users_batch, [(batch_num, batch_size) for batch_num in range(num_batches_users)])

    with Pool(processes=5) as pool:
        pool.starmap(generate_products_batch, [(batch_num, batch_size) for batch_num in range(num_batches_products)])

    # Merge and convert for each table
    merge_csv_and_convert_to_parquet('users', num_batches=num_batches_users)
    merge_csv_and_convert_to_parquet('products', num_batches=num_batches_products)
    merge_csv_and_convert_to_parquet('transactions', num_batches=num_batches_transactions)
