# THE ICONIC test

## Requirements
- Python 3.8
- Install necessary packages: `pip install -r requirements.txt`

## DB Setup
- Create a database in your Postgres instance
- Create a table `customer` in the schema `dev` using the file [db_setup.sql](db_setup%2Fcreate_tbl_customers.sql)

## How to run
- Set the below environment variables as these sensitive info should not be hardcoded in the code
    - `iconic_keyword`: The keyword to generate the password to unzip the data file
    - `host`: Host of the Postgres instance
    - `post`: Port of the Postgres instance
    - `user`: Username to connect to the database
    - `password`: Password to connect to the database
    - `name`: Name of the database
- Go to the root of the project
- Run

```bash
export PYTHONPATH=$(pwd):$PYTHONPATH
python etl/main.py
```

## Some notes
- The validation of the record in the data file is done using Pydantic validators. As a demo, I add some validation rules:
  - Some fields must be non-negative.
  - Field `is_newsletter_subscriber` must be either `Y` or `N`.
  - Some fields like `cancels` and `returns` must be less than or equal to `orders`. It is very convenient to add 
  more fields to this check if needed.
  - `days_since_last_order` must be less than or equal to `days_since_first_order`. The majority of records fails 
  this check. To alleviate this issue, I decided to set these fields to -1 as a sign to notify users that the data of
  these fields is not reliable and should not be used.
  - There are probably more issues but for the demo purpose, I just add these rules as an example of how powerful
  and convenient Pydantic validators are.
- I wrote a small package `rdbms` to load data from a csv file to the database. It uses `psycopg2` to connect to the database.

## SQL queries

```sql
-- What was the total revenue to the nearest dollar for customers who have paid by credit card?
SELECT ROUND(SUM(revenue)) AS revenue
FROM dev.customers
WHERE cc_payments > 0;

-- What percentage of customers who have purchased female items have paid by credit card?
SELECT ROUND(SUM(CASE WHEN cc_payments > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) * 100, 2) AS percentage
FROM dev.customers
WHERE female_items > 0;

-- What was the average revenue for customers who used either iOS, Android or Desktop?
SELECT ROUND(AVG(revenue)) AS revenue
FROM dev.customers
WHERE ios_orders + android_orders + desktop_orders > 0;

-- We want to run an email campaign promoting a new mens luxury brand. Can you provide a list of customers we should send to?
-- The idea is to get top 20% customers who have purchased male items and have high avg ordered item value
WITH final AS (
    SELECT customer_id,
           revenue,
           items,
           revenue / items                                AS avg_ordered_item_value,
           PERCENT_RANK() OVER (ORDER BY revenue / items) AS avg_ordered_item_value_percentile
    FROM dev.customers
    WHERE male_items > 0
)
SELECT *
FROM final
WHERE avg_ordered_item_value_percentile >= 0.8;
```

## Productionisation
- The code is written in a way that it can be easily productionised. The main script `etl/main.py` can be run
  as a scheduled job in Airflow.
- The Pydantic models should be used as a contract between producers and consumers. The producers should produce
  data that conforms to the models and the consumers should consume data that conforms to the models.
- In a production environment, both validated and invalidated data should be stored in a storage like S3 or GCS. When 
  the data is invalidated, the consumers should be notified and they should not use the invalidated data. The invalidated
  data can be used for debugging or reprocessing later.
- In case the input data is huge, we should not save all the processed data to a csv file. Instead, we can publish each 
  record to a message queue like Kafka. The consumers can consume the data from the queue and save it to the database.
