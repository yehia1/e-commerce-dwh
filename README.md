# Sales Data Mart (Adventure Works)
- [Introduction](#Introduction)
- [Technologies](#Technologies)
- [Designing DWH](#Designing_DWH)
- [ETL](#ETL)
- [Dashboard](#Dashboard)
- [Insights](#Insights)
***

## Introduction

The project aims to build dwh for answering business questions regarding e-commerce website using kimball desgin to implement and ingest data into the dwh then using it to answer these questions for state city time before and time after ordering orders numbers and kind of orders finally how to increase the order count and total revenue.

## Technologies
- Visual Studio
- pythin 3.11.2
- MySQL
- Jupyter notebook

## Designing_DWH 

Kimball Star schema design offers a robust data organization approach for data warehouses. This multi-dimensional model separates factual data (measures) in a central fact table from descriptive attributes (dimensions) in surrounding tables. This structure facilitates efficient querying and analysis by enabling users to slice and dice data along various dimensions, providing deeper insights into business performance.

1 Fact table: **Fact order**

6 dimension tables:
- date
- user
- product
- payment
- feedback
- seller

using these [scripts](./Sql%20Scripts/)

<img src="./DW schema.png" alt="Image" width="800" height="650"></img> 

adding the attributes is_active, start_date and end_date to audit

## ETL

Using python and mysql-connector acts as the data pipeline in your data warehouse. It automates the movement and transformation of your data, like a tireless worker behind the scenes, ensuring your DWH stays filled with the freshest information.
I used it to flow the data from the CSV files to the DWH using ETL proccess

6 Dimension Ingestion code 
1 Fact 

### ETL Functions
```py
# 1. Extract: Read the data from the CSV file
def extract(file_path):
    # Load data from CSV file into pandas DataFrame
    if isinstance(file_path, str):
        df = pd.read_csv(file_path)
    else :
        return file_path
    return df

# 2. Transform: Clean and transform the data (you can add more transformation logic as needed)
def transform(df):
    if 'order_id' in df.columns :
        df.drop('order_id',axis= 1,inplace = True)
    # Example transformation: Convert 'is_active' to boolean if it's not
    df['is_active'] = 1
    
    # You can add more transformations here as needed
    return df

def load(df, connection_params, table_name):
    try:
        # Establish a connection using the connection parameters
        conn = mysql.connector.connect(**connection_params)
        cursor = conn.cursor()
        # Iterate over each row of the DataFrame
        for _, row in df.iterrows():
            # Filter out the columns with null (None) values in the row
            non_null_row = row.dropna()  # This drops columns with null values in the current row
            
            if non_null_row.empty:
                print(f"Skipping row with no non-null data: {row}")
                continue

            # Dynamically generate the columns and placeholders for the current row
            columns = ', '.join(non_null_row.index)  # Get the non-null column names
            placeholders = ', '.join(['%s'] * len(non_null_row))  # Create placeholders for non-null values
            values = tuple(non_null_row.values)  # Get the non-null values as a tuple

            # Construct the INSERT query for the current row
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            # Execute the insert query for the current row
            cursor.execute(insert_query, values)
        
        # Commit the transaction after inserting all rows
        conn.commit()
        print(f"Data loaded successfully into the table: {table_name}")
    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Main dim_dim_dim_dim_etl Function
def dim_etl(file_path, connection_params, table_name):
    # Extract
    df = extract(file_path)
    
    # Transform
    df_transformed = transform(df)
    
    # Load
    load(df_transformed, connection_params, table_name)
```

- Extarct : read data from csv and add it to Dataframe
- Transorm : Adding the isactive row to audit
- Load : Ingest data into the DWH

### DIM User

- First Dimension to add with attriubtes related to customer 
- It's Linked to the fact order using the user name inside the csv file of orders

### DIM Product

- Second Dimension to add with attriubtes related to product 
- It's Linked to the fact order using the product_id inside the csv file of orders

### DIM Seller

- Third Dimension to add with attriubtes related to Seller Itself  
- It's Linked to the fact order using the seller_id inside the csv file of orders

### DIM Payment

- Fourth Dimension to add with attriubtes related to payment and methods cash,visa etc 
- It's Linked to the fact order using the order id inside the csv file of payment


### DIM Feedback

- Fifth Dimension to add with attriubtes related to user feedback on order
- It's Linked to the fact order using the order id inside the csv file of feedback

### DIM Date

- Role playing dimension for all datetime attriubtes
- Lowest level of grain is minutes
- to get all date values combined all data columns in data and got all unique values of it and then ingest it inside the date dimension
- Extraction of month, day hour min etc is used in python and all ingested in one time
- `other solution is using trigger on datetime after ingesting`
- Dropped all the seconds attributes


### Fact Table `fact order`

Fact table data ingestion was in two parts 

#### Part 1 adding data which matches in order csv files like user,seller,product and all date attributes

```py 
def map_user_key(order_df,date_columns, connection_params):
    """
    Map user_name in order_df to user_key from dim_user and update order_df.
    
    Parameters:
    - order_df: pandas DataFrame containing the order data with user_name.
    - connection_params: dictionary with MySQL connection parameters.
    
    Returns:
    - Updated order_df with user_key instead of user_name.
    """
    try:
        # Connect to the database
        conn = mysql.connector.connect(**connection_params)
        cursor = conn.cursor()

        # Fetch user_key and user_id from dim_user
        query = "SELECT user_id, user_name FROM dim_user"
        cursor.execute(query)
        user_mapping = {user_id: user_key for user_key, user_id in cursor.fetchall()}
        # Map user_name (order_df) to user_key (dim_user) using user_id
        order_df['user_id'] = order_df['user_name'].map(user_mapping).apply(lambda x: pd.NA if pd.isna(x) else int(x))  # Use -1 for unknown users

        # Fetch seller_key and seller_id from dim_seller
        query = "SELECT seller_id, seller_key FROM dim_seller"
        cursor.execute(query)
        seller_mapping = {seller_key: seller_id for seller_key, seller_id in cursor.fetchall()}
        order_df['seller_id'] = order_df['seller_id'].map(seller_mapping).apply(lambda x: pd.NA if pd.isna(x) else int(x))

        # Fetch product_key and user_id from dim_user
        query = "SELECT product_id, product_key FROM dim_product"
        cursor.execute(query)
        product_mapping = {product_key: product_id for product_key, product_id in cursor.fetchall()}
        order_df['product_id'] = order_df['product_id'].map(product_mapping).apply(lambda x: pd.NA if pd.isna(x) else int(x))

        # Fetch id and fulldate from dim_date
        query = "SELECT date_id, full_timestamp FROM dim_date"
        cursor.execute(query)
        date_mapping = {full_timestamp: date_id for date_id,full_timestamp in cursor.fetchall()}

        for column in date_columns:
            order_df[column] = pd.to_datetime(df[column]).apply(lambda x: x.replace(second=0, microsecond=0))
            order_df[f'{column}_id'] = order_df[column].map(date_mapping).apply(lambda x: pd.NA if pd.isna(x) else int(x))
        
        

        order_df.drop(columns=['user_name']+date_columns, inplace=True)  # Drop user_name column
        print("dimensions mapped successfully.")
        return order_df

    except Error as e:
        print(f"Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
```
Used by fetching matched ids or keys then ingest back with corresponeded values for all dimensions


#### Part 2 which ingest data which got order id inside it like feedback and payment

we used ingestion index for these 2 tables and matches it from data to get correspondad order id from these 2 tables
then match it with order id from orders data 
```py
feedback_df = pd.read_csv('./data/feedback_dataset.csv')
payment_df = pd.read_csv('./data/payment_dataset.csv')

def add_feedback_and_payment(feedback_df,payment_df,df):

    """
    Adds feedback and payment information to the main DataFrame.
    This function takes three DataFrames: feedback_df, payment_df, and df. It adds two new columns to df:
    'feedback_id' and 'payment_id', which are mapped from the feedback_df and payment_df respectively based on 'order_id'.
    Parameters:
    feedback_df (pd.DataFrame): DataFrame containing feedback information with 'order_id'.
    payment_df (pd.DataFrame): DataFrame containing payment information with 'order_id'.
    df (pd.DataFrame): Main DataFrame to which feedback and payment information will be added.
    Returns:
    None: The function modifies the input DataFrame df in place.
    """

    feedback_df['feedback_key'] = feedback_df.index + 1
    feedback_mapper = {row['order_id']: row['feedback_key'] for _, row in feedback_df.iterrows()}
    df['feedback_id'] = df['order_id'].map(feedback_mapper).apply(lambda x: pd.NA if pd.isna(x) else int(x))
    payment_df['payment_key'] = payment_df.index + 1
    payment_mapper = {row['order_id']: row['payment_key'] for _, row in payment_df.iterrows()} 
    df['payment_id'] = df['order_id'].map(payment_mapper).apply(lambda x: pd.NA if pd.isna(x) else int(x))
```

#### Last step is dropping all duplictes of order data `which was all data matches except order item id i think it's bug`



## Business Questions

When is the peak season of our ecommerce ?
What time users are most likely make an order or using the ecommerce app?
What is the preferred way to pay in the ecommerce?
How many installment is usually done when paying in the ecommerce?
What is the average spending time for user for our ecommerce?
What is the frequency of purchase on each state?
Which logistic route that have heavy traffic in our ecommerce?
How many late delivered order in our ecommerce? Are late order affecting the customer satisfaction?
How long are the delay for delivery / shipping process in each state?
How long are the difference between estimated delivery time and actual delivery time in each state?

All Questions in this html format [Business Questions](Business%20questions.html) \\
All Questions in this pdf format [Business Questions](Business%20questions.pdf)

## Insights (to be added soon) 
