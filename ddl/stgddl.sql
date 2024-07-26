
create or replace table stg_d_country_lu
(
id NUMBER,
country_desc VARCHAR(256),
PRIMARY KEY (id)
);

create or replace table BHATBHATENI.STG.STG_D_REGION_LU
(
id NUMBER,
country_id NUMBER,
region_desc VARCHAR(256),
PRIMARY KEY (id)
--FOREIGN KEY (country_id) references COUNTRY(id)
);

create or replace table stg_d_store_lu
(
id NUMBER,
region_id NUMBER,
store_desc VARCHAR(256),
PRIMARY KEY (id)
--FOREIGN KEY (region_id) references REGION(id)
);

create or replace table stg_d_category_lu
(
id NUMBER,
category_desc VARCHAR(1024),
PRIMARY KEY (id)
);

create or replace table stg_d_SUBCATEGORY_lu
(
id NUMBER,
category_id NUMBER,
subcategory_desc VARCHAR(256),
PRIMARY KEY (id)
--FOREIGN KEY (category_id) references CATEGORY(id)
);

create or replace table stg_d_product_lu
(
id NUMBER,
subcategory_id NUMBER,
product_desc VARCHAR(256),
PRIMARY KEY (id)
-- FOREIGN KEY (subcategory_id) references SUBCATEGORY(id)
);

create or replace table stg_d_customer_lu
(
id NUMBER,
customer_first_name VARCHAR(256),
customer_middle_name VARCHAR(256),
customer_last_name VARCHAR(256),
customer_address VARCHAR(256) ,
primary key (id)
);

create or replace table stg_d_sales_lu
(
id NUMBER,
store_id NUMBER NOT NULL,
product_id NUMBER NOT NULL,
customer_id NUMBER,
transaction_time TIMESTAMP,
quantity NUMBER,
amount NUMBER(20,2),
discount NUMBER(20,2),
primary key (id)
---FOREIGN KEY (store_id) references STORE(id),
-- FOREIGN KEY (product_id) references PRODUCT(id),
-- FOREIGN KEY (customer_id) references CUSTOMER(id)
);