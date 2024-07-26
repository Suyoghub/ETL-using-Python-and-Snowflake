Create database BHATBHATENI;
Create schema TRANSACTIONS;

create or replace table COUNTRY
(
id NUMBER,
country_desc VARCHAR(256),
PRIMARY KEY (id)
)
;

create or replace table REGION
(
id NUMBER,
country_id NUMBER,
region_desc VARCHAR(256),
PRIMARY KEY (id),
FOREIGN KEY (country_id) references COUNTRY(id)
)
;

create or replace table STORE
(
id NUMBER,
region_id NUMBER,
store_desc VARCHAR(256),
PRIMARY KEY (id),
FOREIGN KEY (region_id) references REGION(id)
)
;

create or replace table CATEGORY
(
id NUMBER,
category_desc VARCHAR(1024),
PRIMARY KEY (id)
);

create or replace table SUBCATEGORY
(
id NUMBER,
category_id NUMBER,
subcategory_desc VARCHAR(256),
PRIMARY KEY (id),
FOREIGN KEY (category_id) references CATEGORY(id)
);

create or replace table PRODUCT
(
id NUMBER,
subcategory_id NUMBER,
product_desc VARCHAR(256),
PRIMARY KEY (id),
FOREIGN KEY (subcategory_id) references SUBCATEGORY(id)
);

create or replace table CUSTOMER
(
id NUMBER,
customer_first_name VARCHAR(256),
customer_middle_name VARCHAR(256),
customer_last_name VARCHAR(256),
customer_address VARCHAR(256) ,
primary key (id)
);

create or replace table SALES
(
id NUMBER,
store_id NUMBER NOT NULL,
product_id NUMBER NOT NULL,
customer_id NUMBER,
transaction_time TIMESTAMP,
quantity NUMBER,
amount NUMBER(20,2),
discount NUMBER(20,2),
primary key (id),
FOREIGN KEY (store_id) references STORE(id),
FOREIGN KEY (product_id) references PRODUCT(id),
FOREIGN KEY (customer_id) references CUSTOMER(id)
);
