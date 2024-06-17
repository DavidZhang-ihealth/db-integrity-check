import os
import mysql.connector
from pymongo import MongoClient

def get_table(table_name, column_name):
    try:
        connection = mysql.connector.connect(host='mysql-analytic.da.svc.cluster.local',
                                             user='',
                                             password='')

        cursor = connection.cursor()
        sql_select_query = "SELECT " + column_name + " FROM UnifiedCare." + table_name
        # set variable in query
        cursor.execute(sql_select_query)
        # fetch result
        record = cursor.fetchall()


        return record

    except mysql.connector.Error as error:
        print("Failed to get record from MySQL table: {}".format(error))

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def data_matches(mongo_data, sql_data):
    return mongo_data == sql_data

def extract_data_from_mongodb(mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    data = list(collection.find())
    client.close()
    print(data)


def null_check(table_name, column):
    # throws exception if any column entry returns null
    record = get_table(table_name, column)
    # TODO: extract column from column name
    for i, row in enumerate(record) :
        if row[0] == None: 
            print(f"null check failed at row {i}, expected a gender but returned None")
    

null_check("dim_patients", "gender")