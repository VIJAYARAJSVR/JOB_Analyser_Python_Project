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
 source,email,website,posted,applied,created,description,description_html,skill)
  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

delete_query = ''' delete t1 FROM JOB_Analyser_App_jobdetail t1
INNER  JOIN JOB_Analyser_App_jobdetail t2
WHERE t1.id < t2.id AND
    t1.company = t2.company AND
    t1.designation = t2.designation AND
    t1.source = t2.source AND
    DATE(t1.applied) = DATE(t2.applied);
'''


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
    # mydesc = r_data['Description']
    # print(mydesc)
    # return

    jb_url = r_data['Url']
    jb_url = jb_url[:700]

    jb_detail = (
        r_data['Company'], r_data['Designation'], jb_url, r_data['Experience'], r_data['JobType'],
        r_data['Salary'],
        r_data['JOBSource'],
        r_data['Email'], r_data['Website'], r_data['PostedDate'],
        format_applied_date(r_data['AppliedDate'], r_data['AppliedTime']), now,
        r_data['Description'], r_data['Description_HTML'], r_data['Skills'])

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


def delete_duplicate_records(cursor12):
    try:
        cursor12.execute(delete_query)
        db.commit()
        print("Successfully Deleted Duplicate records in Database")
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
            # print(path_with_file_name)
            try:
                with open(path_with_file_name) as json_file:
                    data = json.load(json_file)
                    # print(data)
                    shall_i_proceed = add_new_record(cursor11, data)

                    if shall_i_proceed:
                        new_file_name_with_path = dir_path + 'done_' + file_name
                        os.rename(path_with_file_name, new_file_name_with_path)
                    else:
                        continue

            except EnvironmentError:
                print('EnvironmentError Exception')
                continue
    except FileNotFoundError:
        print("Error: FileNotFoundError")
    except EOFError:
        print("Exception")
    except Exception:
        print('General Exception')


if __name__ == '__main__':
    db = mysql_connect()
    cursor = db.cursor()
    delete_duplicate_records(cursor)
    read_job_json(cursor)
    db.close()

# delete t1 FROM JOB_Analyser_App_jobdetail t1
# INNER  JOIN JOB_Analyser_App_jobdetail t2
# WHERE
# 	t1.id < t2.id AND
#     t1.company = t2.company AND
#     t1.designation = t2.designation AND
#     t1.source = t2.source AND
#     DATE(t1.applied) = DATE(t2.applied);

# SELECT company, COUNT(company),
# designation,COUNT(designation),
# source,COUNT(source),
# DATE(applied) ,COUNT(applied) from JOB_Analyser_App_jobdetail
# GROUP BY
# company, designation,source, DATE(applied)
# HAVING
# COUNT(company) > 1
# AND COUNT(designation) > 1
# AND COUNT(source) > 1
# AND COUNT(DATE(applied));
