import os
from faker import Faker
import random
import csv

# Create an instance of Faker
fake = Faker()

# Create directories for each table if they don't exist
if not os.path.exists('users'):
    os.makedirs('users')

if not os.path.exists('products'):
    os.makedirs('products')

if not os.path.exists('transactions'):
    os.makedirs('transactions')

# Generate fake data for Transactions table
def generate_transactions(num_rows):
    batch_size = 100000 # Set the batch size
    num_batches = num_rows // batch_size

    for batch_num in range(num_batches + 1):
        with open(f'transactions/transactions_batch_{batch_num}.csv', 'w', newline='') as csvfile:
            fieldnames = ['transaction_id', 'user_id', 'product_id', 'product_name', 'amount', 'transaction_date']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(batch_size):
                transaction_id = batch_num * batch_size + i + 1
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
def generate_users(num_rows):
    batch_size = 100000
    num_batches = num_rows // batch_size

    for batch_num in range(num_batches + 1):
        with open(f'users/users_batch_{batch_num}.csv', 'w', newline='') as csvfile:
            fieldnames = ['user_id', 'user_name', 'email', 'date_of_birth', 'address' ,'state', 'country']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for i in range(batch_size):
                user_id = batch_num * batch_size + 1001 + i
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
                    'state':state,
                    'country':country
                })

# Generate fake data for Product dimension table
def generate_products(num_rows):
    batch_size = 100000
    num_batches = num_rows // batch_size

    categories = ['Electronics', 'Accessories', 'Tools', 'Home', 'Clothing']

    for batch_num in range(num_batches + 1):
        with open(f'products/products_batch_{batch_num}.csv', 'w', newline='') as csvfile:
            fieldnames = ['product_id', 'product_name', 'category', 'price']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

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

# Call the functions to generate the data
# generate_users(10000000)      
generate_products(200000)    
# generate_transactions(100000000)  