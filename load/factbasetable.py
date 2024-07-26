import snowflake
import connection.connection as connect
from datetime import datetime
cursor=connect.cursor

#create a table named factbasetable DWH_F_BHATBHATENI_SLS_TRXN_B

# sqlquery= f"""
#   create or replace TABLE BHATBHATENI.TGT.DWH_F_BHATBHATENI_SLS_TRXN_B (
# 	SALES_ID NUMBER(38,0) NOT NULL,
# 	SALES_KEY NUMBER(38,0) autoincrement,
# 	STORE_ID NUMBER(38,0) NOT NULL,
# 	PRODUCT_ID NUMBER(38,0) NOT NULL,
# 	CUSTOMER_ID NUMBER(38,0),
# 	TRANSACTION_DAY number(38,0),
# 	QUANTITY NUMBER(38,0),
# 	AMOUNT NUMBER(20,2),
# 	DISCOUNT NUMBER(20,2),
# 	ACTIVE_FLAG VARCHAR(2),
# 	CREATED_TS TIMESTAMP_NTZ(9),
# 	UPDATED_TS TIMESTAMP_NTZ(9),
# 	primary key (SALES_ID)
# );
# """
# cursor.execute(sqlquery)

def int_base():
    cursor.execute("create or replace stage BHATBHATENI.STG.int_base file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);")
    print(f"internal named stage base created succesfully")
    try:
        cursor.execute(
            r"PUT 'file:///C:/Users/suyog.pandey/Desktop/ETL/extracted files/sales.csv' @BHATBHATENI.STG.int_base/sales.csv overwrite=true")
        print(f"sales.csv copied to @int_base")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

def stg_base():
    try:
        int_base()
        cursor.execute(f"TRUNCATE table STG.STG_F_BHATBHATENI_SLS_TRXN_B")
        cursor.execute(f"COPY INTO STG.STG_F_BHATBHATENI_SLS_TRXN_B FROM @STG.int_base ")
        print(f"data copied from internal stage to stage table base")

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

def tmp_base():
    try:
        temp_table = 'TMP.TMP_F_BHATBHATENI_SLS_TRXN_B'
        stg_table = 'STG.STG_F_BHATBHATENI_SLS_TRXN_B'
        sqlquerys = f"""insert into {temp_table} (select * from {stg_table})"""
        stg_base()
        cursor.execute(f"TRUNCATE table TMP.TMP_F_BHATBHATENI_SLS_TRXN_B")
        cursor.execute(sqlquerys)
        print(f"data copied from stage to temp base")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

    # finally:
    #     cursor.close()

def tgt_base():

    # current_datetime = datetime.now()
    # ins_time = current_datetime.day
    # updated_time= current_datetime.day
    tgtquerys = f"""
        Merge into TGT.DWH_F_BHATBHATENI_SLS_TRXN_B as t
        using (select id,store_id,product_id,customer_id,
        to_Date(transaction_day) as transaction_day,quantity
        ,amount,discount from TMP.TMP_F_BHATBHATENI_SLS_TRXN_B )as s
        on t.sales_id=s.id
        when matched then
            update set
            t.sales_id=s.id,
            t.store_id=s.store_id,
            t.product_id=s.product_id,
            t.customer_id=s.customer_id,
            t.transaction_day=s.transaction_day,
            t.quantity=s.quantity,
            t.amount=s.amount,
            t.discount=s.discount,
            t.active_flag= 'Y', t.updated_ts= current_timestamp()
        when not matched then
            insert (sales_id,store_id,product_id,customer_id,transaction_day,quantity,amount,discount,active_flag,created_ts,updated_ts)
            values (s.id,s.store_id,s.product_id,s.customer_id,s.transaction_day,s.quantity,s.amount,s.discount,'Y',current_timestamp(),current_timestamp() )
    """

    try:
        tmp_base()
        cursor.execute(tgtquerys)
        print(f"data copied from TEMP to target base")

    # except snowflake.connector.errors.ProgrammingError as e:
    #     print(f"error")

    finally:
        cursor.close()

tgt_base()