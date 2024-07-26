import snowflake
import connection.connection as connect
import internalstage as i
#Use your Snowfake user credentials to connect

# conn = snowflake.connector.connect(
#         user='SuyogPandey',
#         password='@986068sP#',
#         account='fo32592.central-india.azure',
#                 database='BHATBHATENI',
#                 schema='STG'
#     )

cursor=connect.cursor

def stg_country():
    try:
        i.internalcountry()
        cursor.execute(f"TRUNCATE table STG.stg_d_country_lu")
        cursor.execute(f"COPY INTO STG.stg_d_country_lu FROM @STG.int_country ")
        print(f"data copied from internal stage to stage table country")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def stg_region():
    try:
        i.internalregion()
        cursor.execute(f"TRUNCATE table STG.stg_d_region_lu")
        cursor.execute(f"COPY INTO STG.stg_d_region_lu FROM @STG.int_region ")
        print(f"data copied from internal stage to stage table region")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def stg_store():
    try:
        i.internalstore()
        cursor.execute(f"TRUNCATE table STG.stg_d_store_lu")
        cursor.execute(f"COPY INTO STG.stg_d_store_lu FROM @STG.int_store ")
        print(f"data copied from internal stage to stage table store")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def stg_category():
    try:
        i.internalcategory()
        cursor.execute(f"TRUNCATE table STG.stg_d_category_lu")
        cursor.execute(f"COPY INTO STG.stg_d_category_lu FROM @STG.int_category ")
        print(f"data copied from internal stage to stage table category")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def stg_subcategory():
    try:
        i.internalsubcategory()
        cursor.execute(f"TRUNCATE table STG.stg_d_subcategory_lu")
        cursor.execute(f"COPY INTO STG.stg_d_subcategory_lu FROM @STG.int_subcategory ")
        print(f"data copied from internal stage to stage table subcategory")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def stg_product():
    try:
        i.internalproduct()
        cursor.execute(f"TRUNCATE table STG.stg_d_product_lu")
        cursor.execute(f"COPY INTO STG.stg_d_product_lu FROM @STG.int_product ")
        print(f"data copied from internal stage to stage table product")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def stg_customer():
    try:
        i.internalcustomer()
        cursor.execute(f"TRUNCATE table STG.stg_d_customer_lu")
        cursor.execute(f"COPY INTO STG.stg_d_customer_lu FROM @STG.int_customer ")
        print(f"data copied from internal stage to stage table customer")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

def stg_sales():
    try:
        i.internalsales()
        cursor.execute(f"TRUNCATE table STG.stg_d_sales_lu")
        cursor.execute(f"COPY INTO STG.stg_d_sales_lu FROM @STG.int_sales ")
        print(f"data copied from internal stage to stage table sales")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")
