# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 12:23:58 2024

@author: NB30006
"""

import mysql.connector
from mysql.connector import Error

def check_duplicates():
    try:
        connection = mysql.connector.connect(
            host='localhost',       
            user='root',    
            password='password',
            database='database' 
        )

        if connection.is_connected():
            print("Successfully connected to the database")
            cursor = connection.cursor()

            #Fetching all tables in the database
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print("Tables in the database:")
            for table in tables:
                print(table)
                
            print("Checking for duplicates for a given field name")
    
            table_name = input("Type the name of the table here: ")
            field_name = input("Type the name of the field here: ")
            cursor.execute(f"""SELECT {field_name} FROM {table_name} 
                           GROUP BY {field_name}
                           HAVING COUNT(1)>=2;""")
            records = cursor.fetchall() #The method fetches all (or all remaining) rows of a query result set and returns a list of tuples
            print("Duplicate values: ")
            if not records:
                print("There are no duplicates!")
            else:
                for record in records:
                    print(record)

            # Closing the cursor and connection
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

    except Error as e:
        print(f"Error: {e}")
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed due to an error")


def check_integrity():
    print("Checking for data integrity and accuracy")
    try:
        connection = mysql.connector.connect(
            host='localhost',       
            user='root',    
            password='password',
            database='database' 
        )
        if connection.is_connected():
            print("Successfully connected to the database")
            cursor = connection.cursor()

        main_table = input("Type name of the main table here: ")
        check_table = input("Specify the name of the table where we want to verify if each dw...id exists: ")
        
        
        query_main = f"""SELECT DISTINCT column_name
                    FROM information_schema.columns                      
                    WHERE table_name = "{main_table}";"""
                    
        cursor.execute(query_main)
        
        column_names_main = [row[0] for row in cursor.fetchall()]
        main_table_id = []
        for v in column_names_main:
            if v.lower().startswith("dw") and v.endswith("id"):
                main_table_id.append(v)
        print(main_table_id)
    
                
            
            
            
        query_check = f"""SELECT DISTINCT column_name
                          FROM information_schema.columns
                          WHERE table_name = "{check_table}";"""
               
        cursor.execute(query_check)
        
        column_names_check = [row[0] for row in cursor.fetchall()]
        check_table_id = []
        for m in column_names_check:
            check_table_id.append(m)
        print(check_table_id)
        
        
        
        
        
        dw_test = input("Type the name of the field you want to test: ")
        if dw_test in main_table_id and dw_test in check_table_id:
            query_check_integrity = f"""SELECT m.{dw_test}, COUNT(m.{dw_test}) AS n    #COUNT(1) = COUNT(c.dw_test)
                                        FROM {main_table} AS m
                                        LEFT OUTER JOIN {check_table} AS c
                                        ON m.{dw_test} = c.{dw_test}
                                        WHERE c.{dw_test} IS NULL
                                        GROUP BY m.{dw_test}
                                        HAVING n > 0;"""
            cursor.execute(query_check_integrity)
            records = cursor.fetchall()
            if not records:
                print("All is in order")
            else:
                for record in records:
                    print("There is an integrity issue in the following(s) records: ",record)
            
    except Error as e:
        print(f"Error: {e}")
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed due to an error")  
            
def check_gaps_and_overlaps():
    try:
        connection = mysql.connector.connect(
            host='localhost',       
            user='root',    
            password='password',
            database='database' 
        )

        if connection.is_connected():
            print("Successfully connected to the database")
            cursor = connection.cursor()
            
            
        main_table = input("Type the name of the table here: ")
        primary_key = input("Type the name of the field here: ")
        start_date = "start_dttm"
        end_date = "end_dttm"

        query_n = f"""SELECT * FROM (
                          SELECT {primary_key}, {start_date}, {end_date},
                          LEAD({start_date}, 1) OVER (PARTITION BY {primary_key} ORDER BY {start_date},{end_date}) AS next_start_date 
                          FROM {main_table} ) AS subquery
                      WHERE {end_date} > next_start_date  #If this happens, there is an overlap issue"""
        
        cursor.execute(query_n)
        overlaps = cursor.fetchall()
        if overlaps:
            print("There is at least one overlap issue")
            for overlap in overlaps:
                primary_key_value, start_dttm, end_dttm, next_start_dttm = overlap
                print(f"Primary Key: {primary_key_value}, "
                      f"Current Period: {start_dttm.strftime('%Y-%m-%d')} to {end_dttm.strftime('%Y-%m-%d')}, "
                      f"Next Start Date: {next_start_dttm.strftime('%Y-%m-%d')}")
        else:
            print("There are no overlaps!")
            
        
        query_o = f"""SELECT * FROM (
                          SELECT {primary_key}, {start_date}, {end_date},
                          LEAD({start_date}, 1) OVER (PARTITION BY {primary_key} ORDER BY {start_date},{end_date}) AS next_start_date 
                          FROM {main_table} ) AS subquery
                      WHERE DATE_ADD({end_date}, INTERVAL 1 DAY) < next_start_date  #If this happens, there is an gap issue"""

        cursor.execute(query_o)
        gaps = cursor.fetchall()
        if gaps:
            print("There is at least one gap issue")
            for gap in gaps:
                primary_key_value, start_dttm, end_dttm, next_start_dttm = gap
                print(f"Primary Key: {primary_key_value}, "
                      f"Current Period: {start_dttm.strftime('%Y-%m-%d')} to {end_dttm.strftime('%Y-%m-%d')}, "
                      f"Next Start Date: {next_start_dttm.strftime('%Y-%m-%d')}")
        else:
            print("There are no gaps!")
            
            
    except Error as e:
        print(f"Error: {e}")
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed due to an error")  

if __name__ == "__main__":
    possible_answers = [1, 2, 3]
    while True:
        try:
            answer = int(input("1 - Check for duplicates\n2 - Check for integrity\n3 - Check for gaps and overlaps\nChoose 1, 2 or 3:"))
            if answer not in possible_answers:
                print("That is not a valid answer! Please choose 1, 2, or 3.")
            else:
                if answer == 1:
                    check_duplicates()
                elif answer == 2:
                    check_integrity()
                elif answer == 3:
                    check_gaps_and_overlaps()
                break  # Exit the loop after a valid answer is provided
        except ValueError:
            print("Please enter a valid number!")

    

