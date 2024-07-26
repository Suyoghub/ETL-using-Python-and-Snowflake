
create or replace table STG_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T
(
id NUMBER,
store_id NUMBER NOT NULL,
product_id NUMBER NOT NULL,
customer_id NUMBER,
transaction_day TIMESTAMP,
quantity NUMBER,
amount NUMBER(20,2),
discount NUMBER(20,2),
primary key (id)
---FOREIGN KEY (store_id) references STORE(id),
-- FOREIGN KEY (product_id) references PRODUCT(id),
-- FOREIGN KEY (customer_id) references CUSTOMER(id)
);

create or replace table TMP_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T
(
id NUMBER,
store_id NUMBER NOT NULL,
product_id NUMBER NOT NULL,
customer_id NUMBER,
transaction_month varchar,
total_quantity NUMBER,
total_amount NUMBER(20,2),
total_discount NUMBER(20,2),
primary key (id)
---FOREIGN KEY (store_id) references STORE(id),
-- FOREIGN KEY (product_id) references PRODUCT(id),
-- FOREIGN KEY (customer_id) references CUSTOMER(id)
);

create or replace TABLE BHATBHATENI.TGT.DWH_F_BHATBHATENI_SLS_TRXN_B (
	SALES_ID NUMBER(38,0) NOT NULL,
	SALES_KEY NUMBER(38,0) autoincrement,
	STORE_ID NUMBER(38,0) NOT NULL,
	PRODUCT_ID NUMBER(38,0) NOT NULL,
	CUSTOMER_ID NUMBER(38,0),
	TRANSACTION_DAY TIMESTAMP_NTZ(9),
	QUANTITY NUMBER(38,0),
	AMOUNT NUMBER(20,2),
	DISCOUNT NUMBER(20,2),
	ACTIVE_FLAG VARCHAR(2),
	CREATED_TS TIMESTAMP_NTZ(9),
	UPDATED_TS TIMESTAMP_NTZ(9),
	primary key (SALES_ID)
);

