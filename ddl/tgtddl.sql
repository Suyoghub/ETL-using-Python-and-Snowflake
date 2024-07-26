create or replace table dwh_d_country_lu(
country_id  Int ,
country_key  int autoincrement start 1 increment 1,
country_desc Varchar,
active_flg Varchar(2),
created_ts Timestamp,
updated_ts Timestamp,
Primary Key (country_id)
);

create or replace table dwh_d_region_lu(
region_id Int ,
region_key Int autoincrement start 1 increment 1,
country_key Int,
region_desc Varchar,
active_flg Varchar(2),
created_ts Timestamp,
updated_ts Timestamp,
Primary Key (region_id)
--Foreign Key (country_key) References dwh_d_country_lu(country_key)
);

create or replace table dwh_d_store_lu (
store_id Int Unique Primary Key,
store_key Int autoincrement start 1 increment 1 ,
region_key Int,
country_key Int,
store_desc Varchar,
active_flg Varchar(2),
created_ts Timestamp,
updated_ts Timestamp
-- Foreign Key (region_key) References dwh_d_region_lu(region_key),
-- Foreign Key (country_key) References dwh_d_country_lu(country_key)
);

create or replace table dwh_d_category_lu(
    category_id Int Unique,
    category_key Int autoincrement start 1 increment 1,
    category_desc Varchar,
    active_flg Varchar(2),
    created_ts Timestamp,
    updated_ts Timestamp,
    primary key(category_id)
);

create or replace table dwh_d_subcategory_lu(
    sub_category_id Int ,
    sub_category_key Int autoincrement start 1 increment 1,
    category_key Int,
    sub_category_desc Varchar,
    active_flg Varchar(2),
    created_ts Timestamp,
    updated_ts Timestamp,
    primary key (sub_category_id)
    --Foreign Key (category_key) References dwh_d_category_lu (category_key)
);

create or replace table dwh_d_product_lu(
    product_id Int ,
    product_key Int autoincrement start 1 increment 1,
    category_key Int,
    sub_category_key Int,
    product_desc Varchar,
    active_flag Varchar(2),
    created_ts Timestamp,
    updated_ts Timestamp,
    primary key (product_id)
    -- Foreign key (category_key) References dwh_d_category_lu(category_key),
    -- Foreign key (sub_category_key) References dwh_d_sub_category_lu(sub_category_key)
);

create or replace table dwh_d_customer_lu(
    customer_id Int Unique,
    customer_key Int autoincrement start 1 increment 1,
    customer_first_name Varchar,
    customer_middle_name Varchar,
    customer_last_name Varchar,
    active_flag Varchar(2),
    created_ts Timestamp,
    updated_ts Timestamp,
    primary key(customer_id)
);
create or replace table dwh_d_sales_lu
(
sales_id int,
sales_key int autoincrement start 1 increment 1,
store_id NUMBER NOT NULL,
product_id NUMBER NOT NULL,
customer_id NUMBER,
transaction_time TIMESTAMP,
quantity NUMBER,
amount NUMBER(20,2),
discount NUMBER(20,2),
primary key (sales_id),
active_flag Varchar(2),
created_ts Timestamp,
updated_ts Timestamp
---FOREIGN KEY (store_id) references STORE(id),
-- FOREIGN KEY (product_id) references PRODUCT(id),
-- FOREIGN KEY (customer_id) references CUSTOMER(id)
);
