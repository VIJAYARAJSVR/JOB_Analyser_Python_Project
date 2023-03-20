from datetime import datetime
import json
import os
import re
import mysql.connector as mysql
from mysql.connector import Error

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


# dir_path = "/Volumes/Macintosh_HD2/Linkedin_JOB/"
dir_path = "/Users/web_dev/Desktop/Applied_JOB/"
db_name = 'JOB_Analyser_DB'
tbl_name = 'JOB_Analyser_App_jobdetail'

dir_path_for_Job_Later = "/Users/web_dev/Desktop/Apply_Later/"
tbl_name_for_Job_Later = 'JOB_Analyser_App_joblater'

insert_query_for_Job_Later = '''INSERT INTO
 JOB_Analyser_App_joblater(company,designation,webpage,captured,created)
  VALUES (%s,%s,%s,%s,%s)'''

insert_query = '''INSERT INTO
 JOB_Analyser_App_jobdetail(company,designation,url,experience,type,salary,
 source,email,website,posted,applied,created,description,description_html,skill,updated)
  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

delete_query = ''' delete t1 FROM JOB_Analyser_App_jobdetail t1
INNER  JOIN JOB_Analyser_App_jobdetail t2
WHERE t1.id < t2.id AND
    t1.company = t2.company AND
    t1.designation = t2.designation AND
    t1.source = t2.source AND
    DATE(t1.applied) = DATE(t2.applied);
'''

delete_JOB_Later_query = ''' delete t1 FROM JOB_Analyser_App_joblater t1
INNER  JOIN JOB_Analyser_App_joblater t2
WHERE t1.id < t2.id AND
    t1.company = t2.company AND
    t1.designation = t2.designation;
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


def format_custom_date(custom_dt, custom_time):
    try:
        arr_dt = custom_dt.split("/")
        arr_time = custom_time.split(":")
        return datetime(day=int(arr_dt[0]), month=int(arr_dt[1]), year=int(arr_dt[2]), hour=int(arr_time[0]),
                        minute=int(arr_time[1]), second=int(arr_time[2]))
    except:
        return now


def add_new_record_Job_later(cursor1, r_data):
    company = r_data['Company'].strip()
    designation = r_data['Designation'].strip()

    if (company == "") or (designation == ""):
        print("Company or Designation is empty")
        return False

    jb_for_later = (
        r_data['Company'], r_data['Designation'], r_data['WebPage'],
        format_custom_date(r_data['CaptureDate'], r_data['CaptureTime']), datetime.now()
    )

    try:
        cursor1.execute(insert_query_for_Job_Later, jb_for_later)
        db.commit()
        print("Saved Job Later in Database Successfully")
        return True
    except mysql.Error as err:
        print("MySql Exception: {}".format(err))
        return False
    except Exception as eee:
        print("Exception: {}".format(eee))
        return False


def add_new_record(cursor1, r_data):
    company = r_data['Company'].strip()
    designation = r_data['Designation'].strip()

    if (company == "") or (designation == ""):
        print("Company or Designation is empty")
        return False

    # is_company = re.search("\\sfound\\s|\sfound", company.lower())
    # is_designation = re.search("\\sfound\\s|\sfounds", designation.lower())
    #
    # if (not is_company) or (not is_designation):
    #     print("Company or Designation is not Found")
    #     return False

    jb_url = r_data['Url']
    jb_url = jb_url[:700]

    applied_dt = format_custom_date(r_data['AppliedDate'], r_data['AppliedTime'])
    updated_dt = applied_dt

    jb_detail = (
        r_data['Company'], r_data['Designation'], jb_url, r_data['Experience'], r_data['JobType'],
        r_data['Salary'],
        r_data['JOBSource'],
        r_data['Email'], r_data['Website'], r_data['PostedDate'],
        applied_dt, now,
        r_data['Description'], r_data['Description_HTML'], r_data['Skills'], updated_dt)

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


def delete_duplicate_JOB_Later_records(cursor13):
    try:
        cursor13.execute(delete_JOB_Later_query)
        db.commit()
        print("Successfully Deleted Duplicate records in Database")
        return True
    except mysql.Error as err:
        print("MySql Exception: {}".format(err))
        return False
    except Exception as eee:
        print("Exception: {}".format(eee))
        return False


def read_job_json_for_job_later(cursor11):
    try:
        # print(dir_path_for_Job_Later)
        for file_name in [file for file in os.listdir(dir_path_for_Job_Later) if
                          (file.endswith('.json') and not (file.__contains__('done_')))]:
            path_with_file_name = dir_path_for_Job_Later + file_name
            # print(path_with_file_name)
            try:
                with open(path_with_file_name) as json_file:
                    data = json.load(json_file)
                    # print(data)
                    shall_i_proceed = add_new_record_Job_later(cursor11, data)

                    if shall_i_proceed:
                        new_file_name_with_path = dir_path_for_Job_Later + 'done_' + file_name
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
    delete_duplicate_JOB_Later_records(cursor)
    read_job_json(cursor)
    read_job_json_for_job_later(cursor)
    db.close()

# delete t1 FROM JOB_Analyser_App_jobdetail t1
# INNER  JOIN JOB_Analyser_App_jobdetail t2
# WHERE
# 	t1.id < t2.id AND
#     t1.company = t2.company AND
#     t1.designation = t2.designation AND
#     t1.source = t2.source AND
#     DATE(t1.applied) = DATE(t2.applied);


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
