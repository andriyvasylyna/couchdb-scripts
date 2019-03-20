#!/usr/bin/env python
import requests
import datetime
from requests.auth import HTTPBasicAuth
import urllib
import json
import sys
import argparse
import logging
import getpass
import os
import re


# all_dbs.json()
def filter(databases):
    yearmonth = '.*-' + "%i%i" % (datetime.date.today().year, datetime.date.today().month)
    for i in reversed(range(len(databases))):
        if re.match(r'.*-[0-9]{6}',databases[i]):
            if not re.match(yearmonth, databases[i]):
                databases.pop(i)
    return databases

#read login and password from file return dict
def get_auth(file):
    with open(file, 'r') as uf:
        for line in uf:
            if (line.startswith("user:")):
                user_name = line[5:].rstrip()
            elif (line.startswith("pass:")):
                password = line[5:].rstrip()
    if(len(user_name) == 0 or len(password) == 0):
       logger.exception("Username or password is not set, check credentials file")
       sys.exit(1)
    uf.close
    return [user_name, password]
# Defining functions

# return number of lines in file
def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

# return list of db files in folder
def get_files(dir_name):
    db_files = [f for f in os.listdir(work_dir) if os.isfile(os.join(work_dir, f))]
    return db_files

#read file content
def get_file_content(fname):
    dict_content = {}
    with open(fname) as f:
        content = f.readlines()
    for i in range(len(content)):
        dict_content[i] = json.loads(content[i])
    return dict_content

# insert single document into couchdb
def insert_document(dburl,dbname,document,credentials):
    insert_url = dburl + dbname + "/"+urllib.quote_plus(document["_id"])
    res = requests.put(insert_url, data=json.dumps(document), auth=HTTPBasicAuth(credentials[0], credentials[1]))
    #print(json.dumps(document))
    return res

#create database if its not exist
def create_db(dburl,dbname,credentials):
    if dburl.endswith('/'):
        dburl = dburl + dbname
    else:
        dburl = dburl + '/' + dbname
    check_get = requests.get(dburl, auth=HTTPBasicAuth(credentials[0], credentials[1]))
    if dbname in check_get.content:
        return True
    else:
        r = requests.put(dburl, auth=HTTPBasicAuth(credentials[0], credentials[1]))
        if r.reason == 'Created':
            return True
        else:
            return False

# backup to file single DB from couchdb
def backup_db(url, db_name, username, password, dir="./", filename=None, revisions=True):
    all_docs = requests.get(url + db_name + '/_all_docs?include_docs=true&attachments=true',auth=HTTPBasicAuth(username, password))
    all_docs = all_docs.json()
    all_docs = all_docs['rows']
    if dir.endswith('/') == False:
        dir = dir+'/'
    if filename is None:
        filename = db_name
    file = open(dir + filename, 'w')
    for i in range(len(all_docs)):
       # to store documents without revision
       if revisions == False:
           del all_docs[i]["doc"]["_rev"]
       file.write(json.dumps(all_docs[i]["doc"]) + '\n')
    file.close()


# Logging settings
log_file = '/var/log/couchdb/backup.log'
logger = logging.getLogger("Log format")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file)
formatter = logging.Formatter('%(asctime)s - %(levelname)s  %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Read shell arguments
parser = argparse.ArgumentParser()
parser.add_argument('-b', '--backup',action='store_true', help='set to backup databases, can not be used with -r')
parser.add_argument('-r', '--restore', action='store_true', help='set to restore databases, can not be used with -b')
parser.add_argument('-s','--server', action='store',help='IP for couchdb server')
parser.add_argument('-p','--port', action='store',help='couchdb port, default 5984',default='5984')
parser.add_argument('-d', '--dir', action='store', help='for backup directory where to save backup files, for restore directory with files to be uploaded to server')
parser.add_argument('-a','--auth', action='store', help='path to file with credentials for Couchdb')
parser.add_argument('-u','--user', action='store', help='username in Couchdb')
parser.add_argument('-f','--filter', action='store', default='No', help='Filter or not filtering MODB, default no, to enable set to yes')


args = parser.parse_args()
cdate = datetime.date.today()
cdate = cdate.strftime('%d-%m-%Y')

if args.backup != True and args.restore != True:
    print("Usage error: Set -b or -r option")
    #logger.error("Usage error: -b and -r not set")
    sys.exit(1)
elif (args.backup  and args.restore ):
    print("Usage error: both -r and -b set ")
    #logger.error("Usage error: both -r and -b set ")
    sys.exit(1)
elif args.server is None:
    print("Usage error: Server IP is not set")
    #logger.error("Usage error: Server IP is not set,-s option is mandatory")
    sys.exit(1)
elif args.dir is None:
    print("Usage error: Working dir is not set")
    #logger.error("Usage error: Working dir is not set,-d option is mandatory")
    sys.exit(1)
elif (args.auth is None and args.user is None ):
    print("Usage error: Auth file is not set")
    #logger.error("Usage error: Auth file is not set,-a option is mandatory")
    sys.exit(1)
elif args.user is not None:
    password = getpass.getpass('Enter password: ')

#Global variables
work_dir = args.dir
if work_dir.endswith('/'):
    backup_dir = work_dir + cdate+"/"
else:
    backup_dir = work_dir +"/"+ cdate+"/"
db_url = "http://{}:{}/"
db_url = db_url.format(args.server,args.port)
#Start body of script
if args.backup == True:
    logger.info("Backup starting")
    logger.info("Database address: %s" % db_url)
    #create dir for backup
    try:
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
    except IOError:
        logger.exception("Cannot create directory: +%s" % backup_dir)
        sys.exit(1)
    #authentification
    try:
        if args.auth is None:
            logger.info("Getting credentials from shell, user =  %s" % args.user)
            credentials = [args.user, password]
        else:
            credentials_file = args.auth
            logger.info("Getting credentials from file %s" % credentials_file)
            credentials = get_auth(credentials_file)
            logger.info("Credentials read node")
    except:
        logger.exception("Exception due credentials read")
        sys.exit(1)
    #get list of db names
    try:
        all_dbs = requests.get(db_url+"_all_dbs",auth=HTTPBasicAuth(credentials[0], credentials[1]))
        # Filter old MODB
        all_dbs = all_dbs.json()
        if args.filter == 'yes':
            all_dbs = filter(all_dbs)
        logger.info("DBs to backup: %s" % all_dbs)
    except:
        logger.exception("Exception in get db list")
        sys.exit(1)
    # Backuping start
    try:
        for i in range(len(all_dbs)):
            logger.info("Backuping: %s" % all_dbs[i])
            backup_db(db_url, urllib.quote_plus(all_dbs[i]), credentials[0], credentials[1], backup_dir)
    except:
        logger.exception("Backup of db %s failed. Exiting" % all_dbs[i])
        sys.exit(1)

    os.system("/usr/bin/zabbix_sender -v -z 10.1.14.234 -p 10051 -s 'couchdb-backup.pbx.vas.sn' -k dbackup_couch -o 0 >> /var/log/couchdb/backup.log")
    
    logger.info("Backup done successfull")
    sys.exit(0)
elif args.restore == True:
    logger.info("Start Restoring")
    try:
        files_list = get_files(work_dir)
        logger.info('DBs to restore: %s' % files_list)
    except:
        logger.exception("Exception due getting list in working directory")
        sys.exit(1)
    #authentification
    try:
        if args.auth is None:
            logger.info("Getting credentials from shell, user =  %s" % args.user)
            credentials = [args.user, password]
        else:
            credentials_file = args.auth
            logger.info("Getting credentials from file %s" % credentials_file)
            credentials = get_auth(credentials_file)
            logger.info("Credentials read node")
    except:
        logger.exception("Exception reading credentials")
        sys.exit(1)

    #creating databases if they don't exist
    logger.info("Creating databases if they dont exist")
    try:
        for i in range(len(files_list)):
            f = create_db(db_url,files_list[i],credentials)
            if f != True:
                logger.error("Error due creating DB %s" % files_list[i])
                sys.exit(1)
    except:
        logger.exception("Exception creating DB %s" % Exception )
    try:
        logger.info("Start restoring databases")
        for i in range(len(files_list)):
            file_content = get_file_content(work_dir+files_list[i])
            logger.info("Restoring: %s" % files_list[i])
            for y in range(len(file_content)):
                #comment if you need to update existing documents in same DB
                del file_content[y]["_rev"]
                r = insert_document(db_url, files_list[i], file_content[y], credentials)
                if r.status_code != 201:
                    logger.error("Error - inserting document %s with message: %s" % (file_content[y]["_id"], r.text))
                #print(r.status_code)
                #print(r.text)
    except:
        logger.exception("Exception due restoring DB")
        sys.exit(1)
    logger.info("DB restoring successfull ended")
    sys.exit(0)
