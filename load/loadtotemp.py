import connection.connection as connect
import snowflake
import loadtostaging as stg

# Use your Snowfake user credentials to connect

# conn = snowflake.connector.connect(
#     user='SuyogPandey',
#     password='@986068sP#',
#     account='fo32592.central-india.azure',
#     database='BHATBHATENI',
#     schema='TMP'
# )

cursor = connect.cursor
temp_table=''
stg_table=''
# sqlquerys = f"insert into BHATBHATENI.TMP.tmp_d_region_lu(select * from BHATBHATENI.STG.stg_d_region_lu)"
# sqlquerys = f"insert into {temp_table} (select * from {stg_table})"

def tmp_country():
    try:
        temp_table = 'TMP.tmp_d_country_lu'
        stg_table = 'STG.stg_d_country_lu'
        sqlquerys = f"insert into {temp_table} (select * from {stg_table})"
        stg.stg_country()
        cursor.execute(f"TRUNCATE table TMP.tmp_d_country_lu")
        cursor.execute(sqlquerys)
        print(f"data copied from stage to temp country")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")


def tmp_region():
    try:
        temp_table = 'TMP.tmp_d_region_lu'
        stg_table = 'STG.stg_d_region_lu'
        sqlquerys = f"insert into {temp_table} (select * from {stg_table})"

        stg.stg_region()
        cursor.execute(f"TRUNCATE table TMP.tmp_d_region_lu")
        cursor.execute(sqlquerys)
        print(f"data copied from stage to temp region")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")
    # finally:
    #     cursor.close()


def tmp_store():
    try:
        temp_table = 'TMP.tmp_d_store_lu'
        stg_table = 'STG.stg_d_store_lu'
        sqlquerys = f"insert into {temp_table} (select * from {stg_table})"
        stg.stg_store()
        cursor.execute(f"TRUNCATE table TMP.tmp_d_store_lu")
        cursor.execute(sqlquerys)
        print(f"data copied from stage to temp store")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def tmp_category():
    try:
        temp_table = 'TMP.tmp_d_category_lu'
        stg_table = 'STG.stg_d_category_lu'
        sqlquerys = f"insert into {temp_table} (select * from {stg_table})"
        stg.stg_category()
        cursor.execute(f"TRUNCATE table TMP.tmp_d_category_lu")
        cursor.execute(sqlquerys)
        print(f"data copied from stage to temp category")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def tmp_subcategory():
    try:
        temp_table = 'TMP.tmp_d_subcategory_lu'
        stg_table = 'STG.stg_d_subcategory_lu'
        sqlquerys = f"insert into {temp_table} (select * from {stg_table})"
        stg.stg_subcategory()
        cursor.execute(f"TRUNCATE table TMP.tmp_d_subcategory_lu")
        cursor.execute(sqlquerys)
        print(f"data copied from stage to temp subcategory")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def tmp_product():
    try:
        temp_table = 'TMP.tmp_d_product_lu'
        stg_table = 'STG.stg_d_product_lu'
        sqlquerys = f"insert into {temp_table} (select * from {stg_table})"
        stg.stg_product()
        cursor.execute(f"TRUNCATE table TMP.tmp_d_product_lu")
        cursor.execute(sqlquerys)
        print(f"data copied from stage to temp product")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def tmp_customer():
    try:
        temp_table = 'TMP.tmp_d_customer_lu'
        stg_table = 'STG.stg_d_customer_lu'
        sqlquerys = f"insert into {temp_table} (select * from {stg_table})"
        stg.stg_customer()
        cursor.execute(f"TRUNCATE table TMP.tmp_d_customer_lu")
        cursor.execute(sqlquerys)
        print(f"data copied from stage to temp customer")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def tmp_sales():
    try:
        temp_table = 'TMP.tmp_d_sales_lu'
        stg_table = 'STG.stg_d_sales_lu'
        sqlquerys = f"insert into {temp_table} (select * from {stg_table})"
        stg.stg_sales()
        cursor.execute(f"TRUNCATE table TMP.tmp_d_sales_lu")
        cursor.execute(sqlquerys)
        print(f"data copied from stage to temp sales")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")


