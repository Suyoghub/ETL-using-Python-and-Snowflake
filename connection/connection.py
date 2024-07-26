import snowflake.connector

#Use your Snowfake user credentials to connect

conn = snowflake.connector.connect(
        user='',
        password='',
        account='fo32592.central-india.azure',
                database='BHATBHATENI',
    )

cursor = conn.cursor()
'''
cursor.execute("SELECT * FROM sales")
rows = cursor.fetchall()
for row in rows:
    print(row)
'''