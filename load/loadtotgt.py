import snowflake
import connection.connection as connect
import loadtotemp as tmp

# Use your Snowfake user credentials to connect

# conn = snowflake.connector.connect(
#     user='SuyogPandey',
#     password='@986068sP#',
#     account='fo32592.central-india.azure',
#     database='BHATBHATENI',
#     schema='TGT'
# )
# i used active_flag as varchar but better to use boolen

cursor = connect.cursor

def tgt_country():
    # tgt_table='country'
    # tmp_table='country'
    tgtquerys = f"""
        Merge into TGT.dwh_d_country_lu as t
        using TMP.tmp_d_country_lu as s
        on t.country_id=s.id
        when matched then
            update set
            t.country_id=s.id,t.country_desc=s.country_desc,t.active_flg= 'Y', t.updated_ts= CURRENT_TIMESTAMP()
        when not matched then
            insert (country_id,country_desc,active_flg,created_ts,updated_ts)
            values(s.id,s.country_desc,'Y',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())
    """

    try:
        tmp.tmp_country()
        cursor.execute(tgtquerys)
        print(f"data copied from TEMP to target country")

    except snowflake.connector.errors.ProgrammingError as e:
         print(f"error")

    # finally:
    #      cursor.close()


def tgt_region():
    tgtquerys = f"""
        Merge into BHATBHATENI.TGT.dwh_d_region_lu as t
        using BHATBHATENI.TMP.tmp_d_region_lu as s
        on t.region_id=s.id
        when matched then
            update set
            t.region_id=s.id, t.country_key=s.country_id, t.region_desc=s.region_desc,t.active_flg= 'Y', t.updated_ts= CURRENT_TIMESTAMP()
        when not matched then
            insert (region_id,country_key,region_desc,active_flg,created_ts,updated_ts)
            values(s.id,s.country_id,s.region_desc,'Y',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())

    """

    try:
        tmp.tmp_region()
        cursor.execute(tgtquerys)
        print(f"data copied from TEMP to target region")

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

    # finally:
    #     cursor.close()


def tgt_store():
    tgtquerys = f"""
        Merge into BHATBHATENI.TGT.dwh_d_store_lu as t
        using BHATBHATENI.TMP.tmp_d_store_lu as s
        on t.store_id=s.id
        when matched then
            update set
            t.store_id=s.id, t.region_key=s.region_id, t.store_desc=s.store_desc,t.active_flg= 'Y', t.updated_ts= CURRENT_TIMESTAMP()
        when not matched then
            insert (store_id,region_key,store_desc,active_flg,created_ts,updated_ts)
            values(s.id,s.region_id,s.store_desc,'Y',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())

    """

    try:
        tmp.tmp_store()
        cursor.execute(tgtquerys)
        print(f"data copied from TEMP to target store")

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

    # finally:
    #     cursor.close()


def tgt_category():
    tgtquerys = f"""
        Merge into BHATBHATENI.TGT.dwh_d_category_lu as t
        using BHATBHATENI.TMP.tmp_d_category_lu as s
        on t.category_id=s.id
        when matched then
            update set
            t.category_id=s.id,t.category_desc=s.category_desc,t.active_flg= 'Y', t.updated_ts= CURRENT_TIMESTAMP()
        when not matched then
            insert (category_id,category_desc,active_flg,created_ts,updated_ts)
            values(s.id,s.category_desc,'Y',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())

    """

    try:
        tmp.tmp_category()
        cursor.execute(tgtquerys)
        print(f"data copied from TEMP to target category")

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

    # finally:
    #     cursor.close()


def tgt_subcategory():
    tgtquerys = f"""
        Merge into BHATBHATENI.TGT.dwh_d_subcategory_lu as t
        using BHATBHATENI.TMP.tmp_d_subcategory_lu as s
        on t.sub_category_id=s.id
        when matched then
            update set
            t.sub_category_id=s.id, t.category_key=s.category_id, t.sub_category_desc=s.subcategory_desc,t.active_flg= 'Y', t.updated_ts= CURRENT_TIMESTAMP()
        when not matched then
            insert (sub_category_id,category_key,sub_category_desc,active_flg,created_ts,updated_ts)
            values(s.id,s.category_id,s.subcategory_desc,'Y',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())

    """

    try:
        tmp.tmp_subcategory()
        cursor.execute(tgtquerys)
        print(f"data copied from TEMP to target subcategory")

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

    # finally:
    #     cursor.close()


def tgt_product():
    tgtquerys = f"""
        Merge into BHATBHATENI.TGT.dwh_d_product_lu as t
        using BHATBHATENI.TMP.tmp_d_product_lu as s
        on t.product_id=s.id
        when matched then
             update set
             t.product_id=s.id, t.sub_category_key=s.subcategory_id, t.product_desc=s.product_desc,t.active_flag= 'Y',t.updated_ts= CURRENT_TIMESTAMP()
        when not matched then
             insert (product_id,sub_category_key,product_desc,active_flag,created_ts,updated_ts)
             values(s.id,s.subcategory_id,s.product_desc,'Y',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())

    """

    try:
        tmp.tmp_product()
        cursor.execute(tgtquerys)
        print(f"data copied from TEMP to target product")

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

    # finally:
    #      cursor.close()



def tgt_customer():
        tgtquerys = f"""
            Merge into BHATBHATENI.TGT.dwh_d_customer_lu as t
            using BHATBHATENI.TMP.tmp_d_customer_lu as s
            on t.customer_id=s.id
            when matched then
                update set
                t.customer_id=s.id, t.customer_first_name=s.customer_first_name, t.customer_middle_name=s.customer_middle_name,
                t.customer_last_name=s.customer_last_name,t.customer_address=s.customer_address,
                t.active_flag= 'Y', t.updated_ts= CURRENT_TIMESTAMP()
            when not matched then
                insert (customer_id,customer_first_name,customer_middle_name,customer_last_name,customer_address,active_flag,created_ts,updated_ts)
                values(s.id,customer_first_name,s.customer_middle_name,s.customer_last_name,s.customer_address,'Y',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())
        """
        try:
            tmp.tmp_customer()
            cursor.execute(tgtquerys)
            print(f"data copied from TEMP to target customer")

        except snowflake.connector.errors.ProgrammingError as e:
            print(f"error")

        # finally:
        #     cursor.close()

def tgt_sales():
    tgtquerys = f"""
        Merge into BHATBHATENI.TGT.dwh_d_sales_lu as t
        using BHATBHATENI.TMP.tmp_d_sales_lu as s
        on t.sales_id=s.id
        when matched then
            update set
            t.sales_id=s.id,
            t.store_id=s.store_id,
            t.product_id=s.product_id,
            t.customer_id=s.customer_id,
            t.transaction_time=s.transaction_time,
            t.quantity=s.quantity,
            t.amount=s.amount,
            t.discount=s.discount,
            t.active_flag= 'Y', t.updated_ts= CURRENT_TIMESTAMP()
        when not matched then
            insert (sales_id,store_id,product_id,customer_id,transaction_time,quantity,amount,discount,active_flag,created_ts,updated_ts)
            values(s.id,s.store_id,s.product_id,s.customer_id,s.transaction_time,s.quantity,s.amount,s.discount,'Y',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())

    """

    try:
        tmp.tmp_sales()
        cursor.execute(tgtquerys)
        print(f"data copied from TEMP to target sales")

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"error")

    # finally:
    #     cursor.close()

