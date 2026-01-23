from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, regexp_replace,trim, upper, when, create_map, lit, current_date, to_date, last_day, concat, count, sum,avg
from pyspark.sql.types import DecimalType, StringType

spark=SparkSession.builder\
.appName('data engineering project')\
.getOrCreate()


transactions=spark.read.csv('data/transactions_data.csv',
                           header=True,
                           inferSchema=True,
                           sep=',',
                           quote='"',
                           escape='"',
                           multiLine=True,
                           mode='PERMISSIVE')
transactions.printSchema()
transactions.describe()

users=spark.read.csv('data/users_data.csv',
                     header=True,
                     inferSchema=True,
                     sep=',',
                     quote='"',
                     escape='"',
                     multiLine=True,
                     mode='PERMISSIVE')
users.printSchema()

cards=spark.read.csv('data/cards_data.csv',
                     header=True,
                     inferSchema=True,
                     sep=',',
                     quote='"',
                     escape='"',
                     multiLine=True,
                     mode='PERMISSIVE')
cards.printSchema()


transactions.filter(transactions.id.isNotNull())\
    .select('id')\
    .show(truncate=False)

transactions_processed=transactions\
    .withColumnRenamed('id','transaction_id')\
    .withColumn('amount', regexp_replace('amount', '[^0-9.]','').cast(DecimalType(12,2)))\
    .withColumn('transaction_date', to_date('date'))\
    .withColumn('zip_code',col('zip').cast('string'))\
    .withColumn(
        'has_error',
        when(col('errors').isNotNull(), True).otherwise(False))\
    .withColumn('merchant_city', upper(trim(col('merchant_city'))))\
    .withColumn('merchant_state', upper(trim(col('merchant_state'))))\
    .withColumn(
        'use_chip',
        when(upper(col('use_chip')) == 'ONLINE TRANSACTION', 'Yes')
        .when(upper(col('use_chip')) == 'SWIPE TRANSACTION', 'Yes')
        .when(upper(col('use_chip')) == 'NULL', 'No')
        .otherwise(None))\
    .fillna({
    'zip_code': 'ONLINE',
    'merchant_state': 'ONLINE'})\
    .filter(col('transaction_id').isNotNull())\
    .filter(col('client_id').isNotNull())\
    .filter(col('amount') > 0)\
    .drop('date')\
    .drop('errors')\
    .drop('zip')\
    .dropDuplicates(['transaction_id'])
transactions_processed.show(truncate=False)


month_map = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR",
    5: "MAY", 6: "JUN", 7: "JUL", 8: "AUG",
    9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC"}

mapping_expr = create_map(
    *[lit(x) for pair in month_map.items() for x in pair])

users_processed= users\
    .withColumnRenamed('id', 'client_id')\
    .withColumn('birth_month_name', mapping_expr[col('birth_month')])\
    .withColumn('yearly_income', regexp_replace(col('yearly_income'), '[^0-9.]', '').cast(DecimalType(12,2)))\
    .withColumn('per_capita_income', regexp_replace(col('per_capita_income'), '[^0-9.]', '').cast(DecimalType(12,2)))\
    .withColumn('total_debt', regexp_replace(col('total_debt'), '[^0-9.]', '').cast(DecimalType(12,2)))\
    .filter((col('credit_score') >= 300) & (col('credit_score') <=850))\
    .filter((col('current_age') >0) & (col('current_age')<=100))\
    .filter(col('client_id').isNotNull())\
    .dropDuplicates(['client_id'])
users_processed.show(truncate=False)


cards_processed=cards\
    .withColumnRenamed('id', 'card_id')\
    .withColumn('credit_limit', regexp_replace(col('credit_limit'), '[^0-9.]', '').cast(DecimalType(12,2)))\
    .withColumn('expires', last_day(to_date(col('expires'), 'MM/yyyy')))\
    .withColumn('acct_open_date', to_date(concat(lit('01/'), col('acct_open_date')),'dd/MM/yyyy'))\
    .withColumn(
        'is_expired',
        when(col('expires') < current_date(), True).otherwise(False))\
    .withColumn('card_brand', upper(trim(col('card_brand'))))\
    .withColumn('card_type', upper(trim(col('card_type'))))\
    .withColumn(
        'has_chip',
        when(upper(col('has_chip')) == 'YES', True)
        .when(upper(col('has_chip')) == 'NO', False)
        .otherwise(None))\
    .withColumn(
        'card_on_dark_web', when(upper(col('card_on_dark_web')) == 'YES', True)
        .when(upper(col('card_on_dark_web')) == 'NO', False)
        .otherwise(None))\
    .filter(col('card_id').isNotNull())\
    .filter(col('client_id').isNotNull())\
    .drop('cvv','card_number')\
    .dropDuplicates(['card_id','client_id'])
cards_processed.show(30, truncate=False)




cards_processed\
    .write\
    .parquet('data/processed/cards')

transactions_processed\
    .write\
    .parquet('data/processed/transactions')

users_processed\
    .write\
    .parquet('data/processed/users')

transactions_df = spark.read.parquet('data/processed/transactions')
users_df = spark.read.parquet('data/processed/users')
cards_df = spark.read.parquet('data/processed/cards')

transactions_df.createOrReplaceTempView('transactions')
users_df.createOrReplaceTempView('users')
cards_df.createOrReplaceTempView('cards')


spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_financial_metrics AS
SELECT
    u.client_id,
    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    SUM(t.amount) AS total_spent,
    AVG(t.amount) AS avg_transaction_value,
    u.current_age,
    u.gender,
    u.credit_score,
    SUM(c.credit_limit) AS total_credit_limit,
    COUNT(DISTINCT c.card_id) AS num_cards,
    MAX(c.card_on_dark_web) AS card_on_dark_web,
    u.yearly_income,
    u.per_capita_income,
    u.total_debt,
    MIN(t.transaction_date) AS first_transaction_date,
    MAX(t.transaction_date) AS last_transaction_date
FROM users u
LEFT JOIN transactions t
    ON u.client_id = t.client_id
LEFT JOIN cards c
    ON u.client_id = c.client_id
GROUP BY
    u.client_id,
    u.current_age,
    u.gender,
    u.credit_score,
    u.yearly_income,
    u.per_capita_income,
    u.total_debt
    """)
spark.sql("SELECT * FROM customer_financial_metrics").show()


spark.sql("""
CREATE OR REPLACE TEMP VIEW card_usage_risk_metrics AS
SELECT
    c.card_id,
    c.client_id,
    c.card_brand,
    c.card_type,
    c.is_expired,
    COUNT(t.transaction_id) AS transaction_count,
    SUM(t.amount) AS total_spend,
    SUM(CASE WHEN t.use_chip = true THEN 1 ELSE 0 END) AS chip_transactions,
    COUNT(t.transaction_id) AS total_transactions,
    SUM(CASE WHEN t.has_error = true THEN 1 ELSE 0 END) AS error_transactions,
    SUM(CASE WHEN t.has_error = true THEN 1 ELSE 0 END) * 1.0
        / COUNT(CASE WHEN t.transaction_id = 0 THEN 1 ELSE 0 END) AS error_rate
FROM cards c
LEFT JOIN transactions t
    ON c.card_id = t.card_id
GROUP BY
    c.card_id,
    c.client_id,
    c.card_brand,
    c.card_type,
    c.is_expired
    """)
spark.sql("SELECT * FROM card_usage_risk_metrics").show()


spark.sql("""
CREATE OR REPLACE TEMP VIEW merchant_geography_metrics AS
SELECT
    merchant_city,
    merchant_state,
    mcc,
    COUNT(transaction_id) AS transaction_count,
    SUM(amount) AS total_spend,
    SUM(CASE WHEN has_error = true THEN 1 ELSE 0 END) AS error_count,
    SUM(CASE WHEN has_error = true THEN 1 ELSE 0 END) * 1.0
        / COUNT(transaction_id ) AS error_rate
FROM transactions
GROUP BY
    merchant_city,
    merchant_state,
    mcc
    """)
spark.sql('SELECT * FROM merchant_geography_metrics').show()


spark.table('customer_financial_metrics')\
    .write\
    .parquet('data/curated/customer_financial_metrics')

spark.table('card_usage_risk_metrics')\
    .write\
    .parquet('data/curated/card_usage_risk_metrics')

spark.table('merchant_geography_metrics')\
    .write\
    .parquet('data/curated/merchant_geography_metrics')


city_transaction_metrics = transactions_processed\
        .filter(col('merchant_city').isNotNull())\
        .groupBy('merchant_city')\
        .agg(
            count('transaction_id').alias('total_transactions'),
            sum('amount').alias('total_spent'),
            avg('amount').alias('avg_transaction_value'))\
        .orderBy(col('total_transactions').desc())

city_transaction_metrics.show(10, truncate=False)


card_type_metrics = cards_processed\
        .filter(col('card_type').isNotNull())\
        .groupBy('card_type')\
        .agg(
            count('credit_limit').alias('total_card_value'),
            sum('credit_limit').alias('total_credit_limit'),
            avg('credit_limit').alias('avg_credit_value'))\
        .orderBy(col('total_card_value').desc())

card_type_metrics.show(truncate=False)


credit_score_metrics = users_processed\
        .filter(col('current_age').isNotNull())\
        .groupBy('current_age')\
        .agg(
            sum('credit_score').alias('total_credit_score'),
            avg('credit_score').alias('avg_credit_score'))\
        .orderBy(col('total_credit_score'))
            
credit_score_metrics.show(10, truncate=False)


c = cards_processed.alias('c')
t = transactions_processed.alias('t')

card_type_metrics = c.join(t, c.client_id == t.client_id, 'Inner')\
        .select(col('c.client_id').alias('client_id'),col('c.card_type'), col('t.amount'))\
        .groupBy('card_type')\
        .agg(
            count('*').alias('transaction_count'),
            sum('t.amount').alias('total_transaction_amount'),
            avg('t.amount').alias('avg_transaction_value'))\
        .filter(col('c.card_type').isNotNull())\
        .orderBy(col('total_transaction_amount').desc())

card_type_metrics.show(10, truncate=False)

