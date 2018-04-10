#!/usr/bin/python3
import pymysql.cursors
import re

# Formats date to sql format
def format_date(date):
    day = date[:2]
    month = date[3:6]
    year = date[7:11]

    month = month_abbreviation_to_num(month)

    return year + '-' + str(month) + '-' + day

# Changes the abbreviation of the month to the monthn number
def month_abbreviation_to_num(month):
    month_num = 0

    if(month == 'Jan'):
        month_num = 1
    elif(month == 'Feb'):
        month_num = 2
    elif(month == 'Mar'):
        month_num = 3
    elif(month == 'Apr'):
        month_num = 4
    elif(month == 'May'):
        month_num = 5
    elif(month == 'Jun'):
        month_num = 6
    elif(month == 'Jul'):
        month_num = 7
    elif(month == 'Aug'):
        month_num = 8
    elif(month == 'Sep'):
        month_num = 9
    elif(month == 'Oct'):
        month_num = 10
    elif(month == 'Nov'):
        month_num = 11
    else:
        month_num = 12
    
    return month_num

connection = pymysql.connect(host='HOSTNAME',
                             user='USERNAME',
                             password='PASSWORD',
                             db='DATABASENAME')

try:
    cursor_obj = connection.cursor()

    # Comment out if table already created
    create_table = 'CREATE TABLE log_data (id INTEGER PRIMARY KEY AUTO_INCREMENT, remote_host TEXT, user_id TEXT, date DATE, time TIME, timezone TEXT, request_type TEXT, status_code INTEGER, response_size INTEGER, referer_header TEXT, user_agent TEXT);'
    cursor_obj.execute(create_table)

    # Read in log file
    infile = './access.log.26'
    with open(infile) as f:
        f = f.readlines()

    # Loop through each line of log file
    for line in f:
        # Regex for grabbing each field
        m = re.findall('^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+.(\\S+\\s+\\S+).\\s+\\"(\\S+)\\s+(.+?)\\s+(HTTP[^"]+)\\"\\s+(\\S+)\\s+(\\S+)\\s+\\"([^"]*)\\"\\s+\\"(.*)\\"$', line)

        # Set variables to insert
        remote_host = m[0][0]
        user_id = m[0][1]
        date = format_date(m[0][3])
        time = m[0][3][12:20]
        timezone = m[0][3][21:26]
        request_type = m[0][4] + ' ' + m[0][5] + ' ' + m[0][6] # combines entire request type (regex separates it into 3 fields)
        status_code = m[0][7]
        response_size = m[0][8]
        referer_header = m[0][9]
        user_agent = m[0][10]

        # Insert values
        insert_values = '\"' + remote_host + '\",\"' + user_id + '\",\"' + date + '\",\"' + time + '\",\"' + timezone + '\",\"' + request_type + '\",' + status_code + ',' + response_size + ',\"' + referer_header + '\", \"' + user_agent + '\"' 
        insert_statement = 'INSERT INTO log_data (remote_host, user_id, date, time, timezone, request_type, status_code, response_size, referer_header, user_agent) VALUES ('+insert_values+');'
        cursor_obj.execute(insert_statement)

    # commit insert to database
    connection.commit()
finally:
    connection.close()
    print ('Data inserted to database')

