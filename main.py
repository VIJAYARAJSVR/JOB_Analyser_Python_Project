from datetime import datetime
import json
import os

import mysql.connector as mysql
from mysql.connector import Error

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


# dir_path = "/Volumes/Macintosh_HD2/Linkedin_JOB/"
dir_path = "/Users/web_dev/Desktop/Linkedin_JOB/"
db_name = 'JOB_Analyser_DB'
tbl_name = 'JOB_Analyser_App_jobdetail'

insert_query = '''INSERT INTO
 JOB_Analyser_App_jobdetail(company,designation,url,experience,type,salary,
 source,email,website,posted,applied,created,description,skill)
  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


def mysql_connect():
    try:
        return mysql.connect(
            host='localhost',
            user='usrvijay',
            password='Nokia@123',
            database=db_name
        )
    except Error as e:
        print(e)
        return


now = datetime.now()


# now2 = datetime(2023, 1, 10, 0, 0, 0, 0)
# now3 = datetime(year=2000, month=2, day=3, hour=5, minute=35, second=2).strftime("%H:%M %p")


def format_applied_date(applied_dt, applied_time):
    try:
        arrdt = applied_dt.split("/")
        arrtime = applied_time.split(":")
        return datetime(day=int(arrdt[0]), month=int(arrdt[1]), year=int(arrdt[2]), hour=int(arrtime[0]),
                        minute=int(arrtime[1]), second=int(arrtime[2]))
    except:
        return now


def add_new_record(cursor1, r_data):
    jb_detail = (
        r_data['Company'], r_data['Designation'], r_data['Url'], r_data['Experience'], r_data['JobType'],
        r_data['Salary'],
        r_data['JOBSource'],
        r_data['Email'], r_data['Website'], r_data['PostedDate'],
        format_applied_date(r_data['AppliedDate'], r_data['AppliedTime']), now,
        r_data['Description'], r_data['Skills'])
    # print(insert_query)
    # print(jb_detail)

    try:
        cursor1.execute(insert_query, jb_detail)
        db.commit()
        print("Saved in Database Successfully")
        return True
    except mysql.Error as err:
        print("MySql Exception: {}".format(err))
        return False
    except Exception as eee:
        print("Exception: {}".format(eee))
        return False


def read_job_json(cursor11):
    try:
        # print(dir_path)
        for file_name in [file for file in os.listdir(dir_path) if
                          (file.endswith('.json') and not (file.__contains__('done_')))]:
            path_with_file_name = dir_path + file_name
            print(path_with_file_name)
            try:
                with open(path_with_file_name) as json_file:
                    data = json.load(json_file)
                    print(data)
                    if add_new_record(cursor11, data):
                        new_file_name_with_path = dir_path + 'done_' + file_name
                        os.rename(path_with_file_name, new_file_name_with_path)

            except EnvironmentError:
                print('EnvironmentError Exception')
    except FileNotFoundError:
        print("Error: FileNotFoundError")
    except EOFError:
        print("Exception")
    except Exception:
        print('General Exception')


if __name__ == '__main__':
    db = mysql_connect()
    cursor = db.cursor()
    read_job_json(cursor)
    db.close()
