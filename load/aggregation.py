import snowflake
import connection.connection as connect
import datetime
cursor=connect.cursor

# sqlquery= f"""
#     create or replace TABLE TGT.DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T (
# 	agg_ID NUMBER(38,0) NOT NULL,
# 	agg_KEY NUMBER(38,0) autoincrement,
# 	STORE_ID NUMBER(38,0) NOT NULL,
# 	store varchar,
# 	PRODUCT_ID NUMBER(38,0) NOT NULL,
# 	TRANSACTION_time number(38,0),
# 	TOTAL_QUANTITY NUMBER(38,0),
# 	TOTAL_AMOUNT NUMBER(20,2),
# 	ACTIVE_FLAG VARCHAR(2),
# 	CREATED_TS varchar,
# 	UPDATED_TS TIMESTAMP_NTZ(9),
# 	primary key (agg_ID)
# )
#
# """
# cursor.execute(sqlquery)
def int_agg():
    cursor.execute(
        "create or replace stage BHATBHATENI.STG.int_agg file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);")
    print(f"internal named stage sales_Agg created succesfully")
    try:
        cursor.execute(
            r"PUT 'file:///C:/Users/suyog.pandey/Desktop/ETL/extracted files/sales.csv' @BHATBHATENI.STG.int_agg/sales.csv overwrite=true")
        print(f"sales.csv copied to @int_agg")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

def stg_agg():
    try:
        int_agg()
        cursor.execute(f"TRUNCATE table STG.STG_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T")
        cursor.execute(f"COPY INTO STG.STG_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T FROM @STG.int_agg ")
        print(f"data copied from internal stage to stage table agg")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def tmp_agg():
    try:
        temp_table = 'TMP.TMP_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T'
        stg_table = 'STG.STG_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T'
        sqlquerys = f"""insert into {temp_table} (select id,store_id,product_id,customer_id,
        replace(to_char(to_Date(transaction_day),'YYYYMMDD'),'-','') as transaction_day,quantity
        ,amount,discount  from {stg_table})"""
        stg_agg()
        cursor.execute(f"TRUNCATE table TMP.TMP_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T")
        cursor.execute(sqlquerys)
        print(f"data copied from stage to temp agg")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

    # finally:
    #     cursor.close()


def tgt_agg():

    # current_datetime = datetime.now()
    # ins_time = current_datetime.day
    # updated_time= current_datetime.day
    tgtquerys = f"""
        Merge into TGT.DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T as t
        using (select m.id,m.product_id,k.store_desc as store_desc,a.month_ky as transaction_month,
        sum(total_quantity) as total_quantity,sum(m.total_amount) as total_sales
        from
        tmp.TMP_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T  m
        join TMP.TMP_D_STORE_LU k on m.store_id=k.id
        join TGT.D_RETAIL_TIME_DAY_T a
        on m.transaction_month=a.day_ky
        group by 1,2,3,4
        order by m.id
        ) as s
        on t.agg_id=s.id
        when matched then
            update set
            t.agg_id=s.id,         
            t.store=s.store_desc,
            t.product_id=s.product_id,
            t.transaction_time=s.transaction_month,
            t.total_quantity=s.total_quantity,
            t.total_amount=s.total_sales,
            t.active_flag= 'Y', t.updated_ts= current_timestamp()
        when not matched then
            insert (agg_id,store,product_id,transaction_time,total_quantity,total_amount,active_flag,created_ts,updated_ts)
            values(s.id,s.store_desc,s.product_id,s.transaction_month,s.total_quantity,s.total_sales,'Y',dayname(current_timestamp()),current_timestamp())

    """

    try:
        tmp_agg()
        cursor.execute(tgtquerys)
        print(f"data copied from TEMP to target base")

    # except snowflake.connector.errors.ProgrammingError as e:
    #     print(f"error")

    finally:
        cursor.close()

tgt_agg()