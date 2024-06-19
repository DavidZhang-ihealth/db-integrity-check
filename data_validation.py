import os
import sys
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
        print(f"{'\033[91m'}Failed to get record from MySQL table: {error}")


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


def null_check(table_name, column_name):
    
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'

    column_names = column_name.split(',')
    entries_checked = 0
    containsNull = False
    num_of_nulls = 0
    record = get_table(table_name, column_name)
    for row in record:
        for index, item in enumerate(row[1: ], 1):
            entries_checked += 1
            if row[index] is None:
                print(f"Null check for '{column_names[index]}' failed at ID {row[0]}")
                num_of_nulls = num_of_nulls + 1
                containsNull = True
    
    print (f"""
=========================================================================
Total entries checked: {entries_checked} | {GREEN}{entries_checked - num_of_nulls} Passed{RESET} | {RED}{num_of_nulls} Failed{RESET} | Failure Rate : {RED}{num_of_nulls/entries_checked * 100}%{RESET}""")
    
    exit(containsNull)

# Program entrypoint - edit this line
null_check("dim_patients", "id,clinicNickname,gender")