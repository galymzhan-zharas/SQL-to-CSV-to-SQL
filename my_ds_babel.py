# !!!!!!!!!!!!!!!!!!!
# Please, leave your contact details (Telegram/Discord) in the summary section of the peer-review sheet so that I could 
# clarify my potential mistakes
# !!!!!!!!!!!!!!!!!!!

import sqlite3
import csv

def sql_to_csv(database, table_name):
    con = sqlite3.connect(database)
    cursor =  con.cursor()
    cursor.execute('SELECT * FROM "{}"'.format(table_name.replace('"', '""')))
    csv_row = ""
    for row in cursor.fetchall():
        row = tuple(map(str, row))
        csv_row+=','.join(row)
        csv_row+='\n'
    con.close()
    return repr(csv_row)

def type_is(datum): 
    if type(datum) == str: 
        return 'TEXT'
    elif type(datum) == int: 
        return 'INTEGER'
    elif type(datum) == float: 
        return 'REAL'

def isFloat(number): 
    try: 
        float(number)
        return True
    except ValueError:
        return False

def isInteger(number):
    try:
        int(number)
        return True
    except ValueError: 
        return False

def csv_to_sql(csv_content, database, table_name):
    csv_content = csv_file.read()
    rows = csv_content.split('\n')
    rows_and_columns = list() # list of tuples representing columns in rows 
    for row in rows: 
        columns = row.split(',') # elements in 'columns', ints/floats, are strings
        for entry in columns: # I want to convert 'string ints' to 'int ints'
            #check if I can convert it to int/float
            if isInteger(entry): 
                columns[columns.index(entry)] = int(entry)
            elif isFloat(entry):
                columns[columns.index(entry)] = float(entry)
        rows_and_columns.append(tuple(columns))


    column_names = rows_and_columns[0]
    sample_columns = rows_and_columns[1] # I need it in line 68 to determine the datatype of an element in SQL format
    column_length = len(column_names) # I need it to build my sql command strings: to determine the number of placeholders
    # I want to form 'CREATE TABLE IF NOT EXISTS "{}" ({} TEXT, {} REAL, ..., {})'
    sql_create_command = 'CREATE TABLE IF NOT EXISTS "{}" ('
    sql_insert_command = 'INSERT INTO "{}" VALUES ('

    for i in range(column_length):
        # column names as identifiers should be enclosed by double quotes
        data_type = type_is(sample_columns[i]) # int -> INTEGER, str -> TEXT, float -> REAL
        sql_create_command = sql_create_command + '"{}" ' + data_type + ','
        sql_insert_command = sql_insert_command + '?,'

    # remove the final commas
    sql_create_command = sql_create_command[:-1]
    sql_insert_command = sql_insert_command[:-1]
    sql_create_command+=')'
    sql_insert_command+=')'
    print("\nI will run these commands:")
    print(sql_create_command.format(table_name, *column_names))
    print(sql_insert_command.format(table_name))
    print()


    #connect to the database
    con = sqlite3.connect(database)
    cursor = con.cursor()

    #create the table
    cursor.execute(sql_create_command.format(table_name, *column_names))
    con.commit()

    #insert the row&columns (I've already added them)
    #cursor.executemany(sql_insert_command.format(table_name),rows_and_columns)
    #con.commit()

    #print the rows
    cursor.execute('SELECT * FROM {}'.format(table_name))

    for row in cursor.fetchall():
        print(row)

    con.close()


print("\nSQL to CSV\n")
print(sql_to_csv('all_fault_line.db', 'fault_lines'))
csv_file = open("list_volcano.csv")
print('\nCSV to SQL\n')
csv_to_sql(csv_file, 'list_volcano.db', 'volcanos')

csv_file.close()

# !!!!!!!!!!!!!!!!!!!
# Please, don't forget to include your contact details
# !!!!!!!!!!!!!!!!!!!