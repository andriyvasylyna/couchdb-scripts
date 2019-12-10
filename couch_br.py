import datetime
from requests import put, get
from requests.auth import HTTPBasicAuth
from urllib.parse import quote
import json
import argparse
import logging
import getpass
import os
import re
import sys

# all_dbs.json()
def filter(databases):
    yearmonth = '.*-' + "%i%i" % (datetime.date.today().year, datetime.date.today().month)
    for i in range(len(databases)):
        if re.match(r'.*-[0-9]{6}', databases[i]):
            if not re.match(yearmonth, databases[i]):
                databases.pop(i)
    return databases
    pass


# read login and password from file return dict
def get_auth(file):
    with open(file, 'r') as uf:
        for line in uf:
            if line.startswith("user:"):
                user_name = line[5:].rstrip()
            elif line.startswith("pass:"):
                password = line[5:].rstrip()
    if len(user_name) == 0 or len(password) == 0:
        raise Exception("Username or password is not set, check credentials file")
    return user_name, password


# Defining functions


'''
# return number of lines in file
def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1
'''


# return list of db files in folder
def get_files(work_dir):
    db_files = [f for f in os.listdir(work_dir) if os.path.isfile(os.path.join(work_dir, f))]
    return db_files


# read file content
def get_file_content(file_name):
    dict_content = {}
    with open(file_name) as f:
        content = f.readlines()
    for i in range(len(content)):
        dict_content[i] = json.loads(content[i])
    return dict_content

'''
# insert single document into couchdb
def insert_document(dburl, dbname, document, username, password):
    if not dburl.endswith('/'):
        dburl = dburl + '/'
    insert_url = dburl + dbname + "/" + requests.utils.requote_uri(document["_id"])
    result = requests.get(insert_url, auth=HTTPBasicAuth(username, password))
    if result.status_code == 404:
        res = requests.put(insert_url, json=document, auth=HTTPBasicAuth(username, password))
    elif result.status_code == 200:
        if '_rev' in result.json():
            document['_rev'] = result.json()['_rev']
            res = requests.put(insert_url, json=document, auth=HTTPBasicAuth(username, password))
        else:
            res = requests.put(insert_url, json=document, auth=HTTPBasicAuth(username, password))
    if res.status_code == 201 or res.status_code == 304:
        return True
    else:
        return False
'''


def insert_document(dburl, dbname, document, username, password):
    if not dburl.endswith('/'):
        dburl = dburl + '/'
    # insert_url = dburl + dbname + "/" + requests.utils.requote_uri(document["_id"]) + '?new_edits=false'
    # insert_url = dburl + dbname + "/" + document["_id"] + '?new_edits=false'
    insert_url = dburl + dbname + "/" + quote(document["_id"],safe='') + '?new_edits=false'
    res = put(insert_url, json=document, auth=HTTPBasicAuth(username, password))
    if res.status_code == 201 or res.status_code == 304:
        return True
    else:
        return False

# create database if its not exist
def create_db(dburl, dbname, username, password):
    if dburl.endswith('/'):
        dburl = dburl + dbname
    else:
        dburl = dburl + '/' + dbname
    response = put(dburl, auth=HTTPBasicAuth(username, password))
    if response.status_code == 201:
        return True
    elif response.status_code == 412:
        return True
    else:
        raise Exception("Fail to create database: {}, with reason: {} ".format(dbname, response.reason))


# backup to file single DB from couchdb
def backup_db(url, db_name, username, password, backup_dir="./", filename=None, revisions=True):
    all_docs = get(
        '{}{}/_all_docs?include_docs=true&attachments=true'.format(url, db_name),
        auth=HTTPBasicAuth(username, password))
    if all_docs.status_code == 200:
        all_docs = all_docs.json()['rows']
        if not backup_dir.endswith('/'):
            backup_dir = backup_dir + '/'
        if filename is None:
            filename = db_name
        with open(backup_dir + filename, 'w') as f:
            for i in range(len(all_docs)):
                # to store documents without revision
                '''
                if not revisions:
                    del all_docs[i]["doc"]["_rev"]
                '''
                f.write(json.dumps(all_docs[i]["doc"]) + '\n')
    else:
        raise Exception('Could not get documents from db: {}'.format(all_docs.reason))


def main():
    logger = logging.getLogger("Log format")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s  %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Read shell arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--backup', action='store_true', help='set to backup databases, can not be used with -r')
    parser.add_argument('-r', '--restore', action='store_true',
                        help='set to restore databases, can not be used with -b')
    parser.add_argument('-s', '--server', action='store', help='IP for couchdb server')
    parser.add_argument('-p', '--port', action='store', help='couchdb port, default 5984', default='5984')
    parser.add_argument('-d', '--dir', action='store',
                        help='for backup directory where to save backup files, '
                             'for restore directory with files to be uploaded to server')
    parser.add_argument('-a', '--auth', action='store', help='path to file with credentials for Couchdb')
    parser.add_argument('-u', '--user', action='store', help='username in Couchdb')
    parser.add_argument('-f', '--filter', action='store', default='No',
                        help='Filter or not filtering MODB, default no, to enable set to yes')

    args = parser.parse_args()
    cdate = datetime.date.today()
    cdate = cdate.strftime('%d-%m-%Y')

    if args.backup is not True and args.restore is not True:
        print("Usage error: Set -b or -r option")
        # logger.error("Usage error: -b and -r not set")
        sys.exit(1)
    elif args.backup and args.restore:
        logger.error("Usage error: both -r and -b set ")
        sys.exit(1)
    elif args.server is None:
        logger.error("Usage error: Server IP is not set,-s option is mandatory")
        sys.exit(1)
    elif args.dir is None:
        logger.error("Usage error: Working dir is not set,-d option is mandatory")
        sys.exit(1)
    elif args.auth is None and args.user is None:
        logger.error("Usage error: Auth file is not set, please set -a option or -u USER")
        sys.exit(1)
    elif args.user is not None:
        username = args.user
        password = getpass.getpass('Enter password: ')
    elif args.auth is not None:
        try:
            credentials_file = args.auth
            logger.info("Getting credentials from file %s" % credentials_file)
            credentials = get_auth(credentials_file)
            username = credentials[0]
            password = credentials[1]
            logger.info("Credentials read node")
        except Exception as e:
            logger.exception("Exception due credentials read " + e)
            sys.exit(1)

    # Global variables
    work_dir = args.dir
    if work_dir.endswith('/'):
        backup_dir = work_dir + cdate + "/"
    else:
        backup_dir = work_dir + "/" + cdate + "/"
    db_url = "http://{}:{}/"
    db_url = db_url.format(args.server, args.port)
    # Start body of script
    if args.backup:
        logger.info("Backup starting")
        logger.info("Database address: %s" % db_url)
        # create dir for backup
        try:
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
        except IOError:
            logger.exception("Cannot create directory: +%s" % backup_dir)
            sys.exit(1)

        # get list of db names
        try:
            all_dbs = get(db_url + "_all_dbs", auth=HTTPBasicAuth(username, password))
            # Filter old MODB
            if args.filter == 'yes':
                all_dbs = filter(all_dbs.json())
            else:
                all_dbs = all_dbs.json()
            logger.info("DBs to backup: %s" % all_dbs)
        except Exception as e:
            logger.exception("Exception in get db list" + e)
            sys.exit(1)
        # Backuping start
        try:
            for i in range(len(all_dbs)):
                logger.info("Backuping: %s" % all_dbs[i])
                backup_db(db_url, quote(all_dbs[i],safe=''), username, password, backup_dir)
        except:
            logger.exception("Backup of db %s failed. Exiting" % all_dbs[i])
            sys.exit(1)

        # logger.info(os.system("zabbix_sender -vv -z 10.1.14.234 -p 10051 -s
        # 'couchdb-backup.pbx.vas.sn' -k dbackup_couch -o 0"))
        logger.info("Backup done successfull")
        sys.exit(0)
    elif args.restore:
        logger.info("###############")
        logger.info("Start Restoring")
        logger.info("###############")
        try:
            files_list = get_files(work_dir)
            logger.info("DB's to restore: %s" % files_list)
        except Exception as e:
            logger.exception("Exception due getting list in working directory" + e)
            sys.exit(1)
        logger.info("Creating databases if they does not exist")
        try:
            for i in files_list:
                f = create_db(db_url, i, username, password)
                if f:
                    logger.info("Database %s created" % i)
        except Exception as e:
            logger.exception("Exception creating DB %s" % e)
        try:
            logger.info("#########################")
            logger.info("Start restoring databases")
            logger.info("#########################")
            if not work_dir.endswith('/'):
                work_dir = work_dir + "/"
            for i in files_list:
                file_content = get_file_content(work_dir + i)
                logger.info("Restoring: %s" % i)
                for y in file_content.values():
                    # comment out if you need to update existing documents in same DB
                    #del y["_rev"]
                    if not insert_document(db_url, i, y, username, password):
                        logger.error("Error - inserting document {} ".format(y))
                        sys.exit(1)
        except Exception as e:
            logger.exception("Exception due restoring DB {} , with error: {}".format(i, e))
            sys.exit(1)
        logger.info("DB restoring successfull ended")
        sys.exit(0)


# MAIN
if __name__ == '__main__':
    main()
