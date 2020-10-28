create external table stabsumalam_db.user_pageviews
(
email string,
page_view bigint,
created_date date,
last_active 'date'
)
stored as parquet
location '/user/stabsumalam/pyspark-tdd-template/user_pageviews/';
