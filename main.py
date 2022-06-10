import sys
import pandas as pd
import numpy as np
import time
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras
from sqlalchemy import create_engine, null
import psutil
import logging
from fuzzywuzzy import process, fuzz


def create_sample_dataframe(size):
    logging.info('Sample Dataframe creation Starting')
    df = pd.DataFrame(data=np.random.randint(99999, 99999999, size=(size, 14)),
                      columns=['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11', 'C12', 'C13', 'C14'])
    df['C15'] = pd.util.testing.rands_array(5, size)
    try:
        df.to_csv("huge_data.csv")
    except Exception as e:
        logging.error(e)
    logging.info('Sample Dataframe and csv created')


def read_from_csv_chunks(chunk):
    # read data in chunks of 10000 rows at a time. This will reduce time and ram.
    # The chunksize can be increased if there is more ram to improve performance time or decreased
    # if the ram restrictions still apply.
    logging.info('Import Starting')
    try:
        df = pd.read_csv('huge_data.csv', chunksize=chunk, encoding='utf-8')
        pd_df = pd.concat(df)
    except Exception as e:
        pd_df = null
        logging.error(e)
    logging.info('Sample Dataframe imported')
    return pd_df


def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
    line_n = traceback.tb_lineno
    # print the connect() error
    logging.error("\npsycopg2 ERROR:", err, "on line number:", line_n)
    logging.error("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    logging.error("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    logging.error("pgerror:", err.pgerror)
    logging.error("pgcode:", err.pgcode, "\n")


def create_connection(postgres_username, postgres_password, postgres_address, postgres_port, postgres_dbname):
    cnx = null
    postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'
                    .format(username=postgres_username,
                            password=postgres_password,
                            ipaddress=postgres_address,
                            port=postgres_port,
                            dbname=postgres_dbname))
    # Create the connection to postgres db
    try:
        logging.info('Connecting to the PostgreSQL...........')
        cnx = create_engine(postgres_str)
        logging.info("Connection successfully..................")
    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)
        cnx = null
    return cnx


def copy_to_db(pd_df, cnx, chunk):
    try:
        # using connection created, push the data in public.test_od in chunksizes of 10000.
        # This can be adjusted too similar to the import from csv to improve performance time
        # or to adjust for ram restrictions
        logging.info('Starting copying to DB')
        pd_df.to_sql('public.test_od', con=cnx, if_exists='replace', index=False, chunksize=chunk)
        logging.info('Copy to DB Done')
    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)


def task1():
    logging.basicConfig(filename='logs.log', encoding='utf-8', level=logging.INFO)
    create_sample_dataframe(10000)
    pd_df = read_from_csv_chunks(10000)
    postgres_address = 'localhost'
    postgres_port = '5432'
    postgres_username = 'postgres'
    postgres_password = 'kevin'
    postgres_dbname = 'postgres'
    cnx = create_connection(postgres_username, postgres_password, postgres_address, postgres_port, postgres_dbname)
    copy_to_db(pd_df, cnx, 10000)
    logging.debug("task 1 done")
    print("Data inserted to Postgres DB")


def remove_duplicates_inplace(list, similar_level=50):
    def check_simi(d):
        # get similarity based on fuzzy matching and get indexes based on match % >50%
        dupl_indexes = []
        for i in range(len(d) - 1):
            for j in range(i + 1, len(d)):
                if fuzz.token_sort_ratio(d[i], d[j]) >= similar_level:
                    dupl_indexes.append(j)

        return set(dupl_indexes)

    indexes = check_simi(list)
    # get new list based on index not in list of matching indexes
    new_list = [j for i, j in enumerate(list) if i not in indexes]
    return new_list


def task2():
    company_name_example = ['Saama', 'Equipment ONLY - Saama Technologies', 'Saama Technologies', 'SaamaTech, Inc',
                            'Takeda Pharmaceutical SA - Central Office', '*** DO NOT USE *** Takeda Pharmaceutical',
                            'Takeda Pharmaceutical, SA', 'Ship to AstraZeneca', 'AstraZeneca, gmbh Munich',
                            'AstraZeneca (use AstraZeneca, gmbh Munich acct 84719482-A)']
    # since our code gets the first company name and ignores the rest,
    # for this example, I sorted the list by length of company name to get the shortest company name
    company_name_sorted = sorted(company_name_example, key=len)
    # called function to actually return new list without fuzzy matching duplicates
    company_names = remove_duplicates_inplace(company_name_sorted)
    print(company_names)
    logging.debug("task 2 done")


def connect_to_db(postgres_username, postgres_password, postgres_address, postgres_port, postgres_dbname):
    try:
        conn = psycopg2.connect(
            database=postgres_dbname, user=postgres_username, password=postgres_password, host=postgres_address,
            port=postgres_port
        )
    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)
        conn = null
    return conn


def create_tables(conn):
    cursor = conn.cursor()

    # Dropping VISITS table if already exists.
    cursor.execute("DROP TABLE IF EXISTS VISITS")

    # Creating table as per requirement
    sql = '''CREATE TABLE VISITS(
        Customer_id CHAR(20),
        City_id_visited INT,
        Date_visited date
    )'''
    cursor.execute(sql)
    logging.info("VISITS table created successfully........")
    conn.commit()

    # Dropping CUSTOMER table if already exists.
    cursor.execute("DROP TABLE IF EXISTS CUSTOMER")

    # Creating table as per requirement
    sql = '''CREATE TABLE CUSTOMER(
        Customer_id CHAR(20),
        Customer_name CHAR(40),
        Gender CHAR(1),
        Age INT
    )'''
    cursor.execute(sql)
    logging.info("CUSTOMER table created successfully........")
    conn.commit()

    # Dropping CITY table if already exists.
    cursor.execute("DROP TABLE IF EXISTS CITY")

    # Creating table as per requirement
    sql = '''CREATE TABLE CITY(
        City_id INT,
        City_name CHAR(20),
        Expense INT
    )'''
    cursor.execute(sql)
    logging.info("CITY table created successfully........")
    conn.commit()


def insert_to_tables(conn):
    cursor = conn.cursor()
    # Inserting sample data into the tables
    sql = '''INSERT INTO VISITS (Customer_id,City_id_visited,Date_visited) VALUES
        (1001,2003,'1-Jan-03'),
        (1001,2004,'1-Jan-04'),
        (1002,2001,'1-Jan-01'),
        (1004,2003,'1-Jan-03')'''
    cursor.execute(sql)
    logging.info("Data inserted in VISITS")
    conn.commit()
    sql = '''INSERT INTO CUSTOMER (Customer_id,Customer_name,Gender,Age) VALUES
        (1001,'John','M',25),
        (1002,'Mark','M',40),
        (1003,'Martha','F',55),
        (1004,'Selena','F',34)'''
    cursor.execute(sql)
    logging.info("Data inserted in CUSTOMER")
    conn.commit()
    sql = '''INSERT INTO CITY (City_id,City_name,Expense) VALUES
        (2001,'Chicago',500),
        (2002,'Newyork',1000),
        (2003,'SFO',2000),
        (2004,'Florida',800)'''
    cursor.execute(sql)
    logging.info("Data inserted in CITY")
    conn.commit()


def sql_q1(cursor):
    # 1) Cities frequently visited?

    sql = '''select v.city_id_visited,
                trim(c.city_name) as city_name,
                count(distinct customer_id) as count 
                from visits v 
                join city c on c.city_id=v.city_id_visited 
                group by v.city_id_visited,c.city_name 
                having count(distinct customer_id)>1'''
    cursor.execute(sql)
    output1 = cursor.fetchall()
    print("\nOutput for q1")
    print("Cities with more than 1 visits:")
    for row in output1:
        print(row[1])


def sql_q2(cursor):
    # 2) Customers visited more than 1 city?

    sql = '''select v.customer_id,
                trim(c.customer_name) as customer_name,
                count(distinct city_id_visited) as count 
                from visits v 
                join customer c on c.customer_id=v.customer_id 
                group by v.customer_id,c.customer_name 
                having count(distinct city_id_visited)>1'''
    cursor.execute(sql)
    output1 = cursor.fetchall()
    print("\nOutput for q2")
    print("Customers that visited more than 1 city:")
    for row in output1:
        print(row[1])


def sql_q3(cursor):
    # 3) Cities visited breakdown by gender?

    sql = '''select city_id,trim(city_name) as city_name,gender,count(*) as count
    from visits v
    join customer cust on v.customer_id=cust.customer_id
    join city on city.city_id=v.city_id_visited
    group by city_id,city_name,gender'''
    cursor.execute(sql)
    output1 = cursor.fetchall()
    print("\nOutput for q3")
    print(output1)


def sql_q4(cursor):
    # 4) List the city names that are not visited by every customer
    # and order them by the expense budget in ascending order?

    sql = '''with master_list as 
    (select distinct customer_id, customer_name,
    city_id 
    from customer,city),
    cities_visited as (select cust.customer_id,STRING_AGG(city_id_visited::text,',') as cities_visited from customer cust 
    join visits v on v.customer_id=cust.customer_id
    join city c on v.city_id_visited=c.city_id
    group by cust.customer_id)
    select m.customer_name,city.city_name,city.expense from master_list m 
    left join cities_visited c on m.customer_id=c.customer_id 
    join city on city.city_id=m.city_id
    where c.cities_visited is null or c.cities_visited not like format('%%%s%%', m.city_id)
    order by m.customer_id,city.expense asc'''
    cursor.execute(sql)
    output1 = cursor.fetchall()
    print("\nOutput for q4")
    for row in output1:
        print("Customer ", row[0])
        print("City not visited: ", row[1])
        print("Expense: ", row[2])


def sql_q5(cursor):
    # 5) Visit/travel Percentage for every customer?

    sql = '''select distinct trim(cust.customer_name) as customer_name,(count(c.city_id) over (partition by cust.customer_id)*100/count(c.city_id) over ()) as travel_perc from customer cust
    left join visits v on v.customer_id=cust.customer_id
    left join city c on v.city_id_visited=c.city_id '''
    cursor.execute(sql)
    output = cursor.fetchall()
    print("\nOutput for q5")
    for row in output:
        print("Customer: ", row[0])
        print("Travel %: ", row[1])


def sql_q6(cursor):
    # 6) Total expense incurred by customers on their visits?

    sql = '''select trim(cust.customer_id) as customer_id,
    trim(cust.customer_name) as customer_name,
    case when sum(c.expense) is null then 0 else sum(c.expense) end as expense 
    from customer cust 
    left join visits v on v.customer_id=cust.customer_id 
    left join city c on v.city_id_visited=c.city_id 
    group by cust.customer_id,cust.customer_name'''
    cursor.execute(sql)
    output = cursor.fetchall()
    print("\nOutput for q6")
    for row in output:
        print("Customer: ", row[1])
        print("Expense: ", row[2])


def sql_q7(cursor):
    # 7) list the Customer details along with the city they first visited and the date of visit?

    sql = '''select trim(customer_id) as customer_id,trim(customer_name) as customer_name,gender,age,trim(city_name) as city_name,to_char(date_visited,'yyyy-mm-dd') as date_visited 
    from (select cust.*,trim(c.city_name) as city_name,rank() over (partition by cust.customer_id order by v.date_visited asc) as rnk,v.date_visited from customer cust 
    join visits v on v.customer_id=cust.customer_id
    join city c on v.city_id_visited=c.city_id)a where rnk=1
    '''
    print("\nOutput for q7")
    cursor.execute(sql)
    output = cursor.fetchall()
    print(output)


def sqltasks():
    postgres_address = 'localhost'
    postgres_port = '5432'
    postgres_username = 'postgres'
    postgres_password = 'kevin'
    postgres_dbname = 'postgres'
    conn = connect_to_db(postgres_username, postgres_password, postgres_address, postgres_port, postgres_dbname)
    create_tables(conn)
    insert_to_tables(conn)
    cursor = conn.cursor()
    sql_q1(cursor)
    sql_q2(cursor)
    sql_q3(cursor)
    sql_q4(cursor)
    sql_q5(cursor)
    sql_q6(cursor)
    sql_q7(cursor)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    task1()
    task2()
    sqltasks()
