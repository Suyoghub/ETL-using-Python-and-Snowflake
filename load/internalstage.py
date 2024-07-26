import snowflake.connector
import connection.connection as connect

# #cursor = m.cursor
# conn = snowflake.connector.connect(
#         user='SuyogPandey',
#         password='@986068sP#',
#         account='fo32592.central-india.azure',
#                 database='BHATBHATENI',
#                 schema='STG'
#     )

cursor=connect.cursor
'''
cursor.execute("show stages")
stages = cursor.fetchall()
print("internal named stage:")
for stage in stages:
    print(stage[0])
'''
def internalcountry():
    cursor.execute("create or replace stage BHATBHATENI.STG.int_country file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);")
    print(f"internal named stage country created succesfully")
    try:
        cursor.execute(r"PUT 'file:///C:/Users/suyog.pandey/Desktop/ETL/extracted files/country.csv' @BHATBHATENI.STG.int_country/country.csv overwrite=true")
        print(f"country.csv copied to @int_country")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

def internalregion():
    cursor.execute("create or replace stage BHATBHATENI.STG.int_region file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);")
    print(f"internal named stage created succesfully")
    try:
        cursor.execute(r"PUT 'file:///C:/Users/suyog.pandey/Desktop/ETL/extracted files/region.csv' @BHATBHATENI.STG.int_region/region.csv overwrite=true")
        print(f"region.csv copied to @int_region")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

def internalstore():
    cursor.execute("create or replace stage BHATBHATENI.STG.int_store file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);")
    print(f"internal named stage store created succesfully")
    try:
        cursor.execute(r"PUT 'file:///C:/Users/suyog.pandey/Desktop/ETL/extracted files/store.csv' @BHATBHATENI.STG.int_store/store.csv overwrite=true")
        print(f"store.csv copied to @int_store")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

def internalcategory():
    cursor.execute("create or replace stage BHATBHATENI.STG.int_category file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);")
    print(f"internal named stage category created succesfully")
    try:
        cursor.execute(r"PUT 'file:///C:/Users/suyog.pandey/Desktop/ETL/extracted files/category.csv' @BHATBHATENI.STG.int_category/category.csv overwrite=true")
        print(f"category.csv copied to @int_category")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

def internalsubcategory():
    cursor.execute("create or replace stage BHATBHATENI.STG.int_subcategory file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);")
    print(f"internal named stage subcategory created succesfully")
    try:
        cursor.execute(r"PUT 'file:///C:/Users/suyog.pandey/Desktop/ETL/extracted files/subcategory.csv' @BHATBHATENI.STG.int_subcategory/subcategory.csv overwrite=true")
        print(f"subcategory.csv copied to @int_cateogry")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

def internalproduct():
    cursor.execute("create or replace stage BHATBHATENI.STG.int_product file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);")
    print(f"internal named stage product created succesfully")
    try:
        cursor.execute(r"PUT 'file:///C:/Users/suyog.pandey/Desktop/ETL/extracted files/product.csv' @BHATBHATENI.STG.int_product/product.csv overwrite=true")
        print(f"product.csv copied to @int_product")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

def internalcustomer():
    cursor.execute("create or replace stage BHATBHATENI.STG.int_customer file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);")
    print(f"internal named stage customer created succesfully")
    try:
        cursor.execute(r"PUT 'file:///C:/Users/suyog.pandey/Desktop/ETL/extracted files/customer.csv' @BHATBHATENI.STG.int_customer/customer.csv overwrite=true")
        print(f"customer.csv copied to @int_customer")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

def internalsales():
    cursor.execute("create or replace stage BHATBHATENI.STG.int_sales file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);")
    print(f"internal named stage sales created succesfully")
    try:
        cursor.execute(r"PUT 'file:///C:/Users/suyog.pandey/Desktop/ETL/extracted files/sales.csv' @BHATBHATENI.STG.int_sales/sales.csv overwrite=true")
        print(f"sales.csv copied to @int_sales")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")
