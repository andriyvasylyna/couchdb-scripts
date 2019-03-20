#!/usr/bin/env python
from os import listdir
from os.path import isfile, join
import requests
import datetime
from requests.auth import HTTPBasicAuth
import os
import urllib
import json
import sys
import argparse
import logging
import getpass

#Function def

#Get dict of DBs from couch
def get_all_db(db_url,user,password):
    all_dbs = "{}_all_dbs"
    all_dbs = all_dbs.format(db_url)
    db_list = requests.get(all_dbs, auth=HTTPBasicAuth(user, password))
    return db_list.json()

#Get user and password from file
def get_auth(file):
    with open(file, 'r') as uf:
        for line in uf:
            if (line.startswith("user:")):
                user_name = line[5:].rstrip()
            elif (line.startswith("pass:")):
                password = line[5:].rstrip()
    if(len(user_name) == 1 or len(password) == 0):
       raise Exception("Username or password is not set, check credentials file")
       sys.exit(1)
    uf.close
    return [user_name, password]


#Replicate DB from source to target
def replicate_db(db_name,src_url,dst_url,credentials_source,credentials_target):
    #src_url = src_url+"/"+db_name
    db_name = urllib.quote_plus(db_name)
    headers = {"Content-Type": "application/json"}
    dst = "http://{}:{}@{}/{}"
    dst = dst.format(credentials_target[0], credentials_target[1], dst_url, db_name)
    request_dict = {"source": src_url+db_name, "target": dst}
    #print(json.dumps(request_dict))
    replicate_request = requests.post(src_url+"_replicate",data=json.dumps(request_dict),headers=headers, auth=HTTPBasicAuth(credentials_source[0], credentials_source[1]),timeout=(30,120))
    return replicate_request.json()

#create database if its not exist
def create_db(dburl,dbname,credentials):
    dbname_enc = urllib.quote_plus(dbname)
    if dburl.endswith('/'):
        dburl = dburl + dbname_enc
    else:
        dburl = dburl + '/' + dbname_enc
    check_get = requests.get(dburl, auth=HTTPBasicAuth(credentials[0], credentials[1]))
    #print(check_get.text)
    if dbname in check_get.content:
        return True
    else:
        r = requests.put(dburl, auth=HTTPBasicAuth(credentials[0], credentials[1]))
        if r.reason == 'Created':
            return True
        else:
            return False

#Delete DB from target DB if it does not exist it source DB
def delete_db(db_url,db_name,user,password):
    d = requests.delete(db_url+db_name,auth=HTTPBasicAuth(user, password))
    return d
# Logging settings
log_file =  '/opt/couch/log/backup.log'
logger = logging.getLogger("Log format")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file)
formatter = logging.Formatter('%(asctime)s - %(levelname)s  %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
# Read shell arguments
parser = argparse.ArgumentParser()
parser.add_argument('-s','--source', action='store',help='IP of source couchdb, in format IP:PORT')
parser.add_argument('-t','--target', action='store',help='IP of target couchdb, in format IP:PORT')
parser.add_argument('-as','--authsource', action='store', help='path to file with credentials for source Couchdb')
#parser.add_argument('-us','--usersource', action='store', help='username for source Couchdb')
#parser.add_argument('-ut','--usertarget', action='store', help='username for target Couchdb')
parser.add_argument('-d','--dbname',action='store',help='database name for replication only single database')
parser.add_argument('-at','--authtarget', action='store', help='path to file with credentials for target Couchdb')


args = parser.parse_args()

if args.source is None or args.target is None:
    print("Usage error: target and source IP should be set")
    sys.exit(1)
elif ((args.authsource is None or args.authtarget is None)):
    print("Usage error: not all credentials set ")
    sys.exit(1)
elif args.authsource is not None and args.authtarget is not None:
    logger.info("Getting credentials from file %s" % args.authsource)
    credentials_source = get_auth(args.authsource)
    logger.info("Credentials for source DB read node")
    logger.info("Getting credentials from file %s" % args.authtarget)
    credentials_target = get_auth(args.authtarget)
    logger.info("Credentials for target DB read node")

#Global variables
db_url = "http://{}/"
db_source = args.source
db_target_ip = args.target
db_source = db_url.format(db_source)
db_target = db_url.format(db_target_ip)

# Body
# Replicate all DBs

if args.dbname is None:
    logger.info("Replication started")
    logger.info("Replicating all DBs")
    logger.info("Get list of DBs")
    src_dbs = get_all_db(db_source,credentials_source[0],credentials_source[1])
    src_dbs.remove("_replicator")
    src_dbs.remove("_users")
    src_dbs.remove("_global_changes")
    logger.info("DBs to replicate: %s" % src_dbs)
    logger.info("Create DB on target if its not exist")
    try:
        for i in range(len(src_dbs)):
            r = create_db(db_target,src_dbs[i],credentials_target)
            if r != True:
                logger.error("Error during creation of %s" % src_dbs[i])
    except:
        logger.exception("Exception while creating DBs")

    logger.info("Creating DBs successful")
    logger.info("Start Replication")

    try:
        for i in range(len(src_dbs)):
            logger.info("Replicating: %s" % src_dbs[i])
            rep_req = replicate_db(src_dbs[i],db_source,db_target_ip,credentials_source,credentials_target)
            if 'error' in rep_req:
                log_err = "Replication error: {}, reason: {}"
                log_err = log_err.format(rep_req['error'],rep_req['reason'])
                logger.error(log_err)
            elif 'ok' in rep_req:
                if 'session_id' in rep_req and 'history' in rep_req:
                    log_srt = "Replication success: session_id: {}, start_time: {} ,end_time: {}, missing_checked: {}, missing_found: {}, docs_read: {}, docs_written: {}, doc_write_failures: {}"
                    log_srt = log_srt.format(rep_req['session_id'],rep_req['history'][0]['start_time'],rep_req['history'][0]['end_time'],rep_req['history'][0]['missing_checked'],rep_req['history'][0]['missing_found'],rep_req['history'][0]['docs_read'],rep_req['history'][0]['docs_written'],rep_req['history'][0]['doc_write_failures'])
                    logger.info(log_srt)
                else:
                    #print type(rep_req['no_changes'])
                    logger.info("Replication success no changes:{}".format(str(rep_req['no_changes'])))
                    #print(str(rep_req['no_changes']))
            else:
                logger.error("Unexpected result: %s" % str(rep_req))
    except:
        logger.exception("Exception due replicating DB ")
        sys.exit(1)
    logger.info("Replicating finish")

# Delete DB that is not in prod already
    try:
        logger.info("Deleting old DBs from backup")
        trg_dbs = get_all_db(db_target,credentials_target[0],credentials_target[1])
        #trg_dbs.remove("_replicator")
        #trg_dbs.remove("_replicator")
        #trg_dbs.remove("_users")
        #trg_dbs.remove("_global_changes")
        res_list = list(set(trg_dbs)-set(src_dbs))
        if len(res_list) > 0:
            logger.info("Delete: %s" % res_list)
            for i in range(len(res_list)):
               del_res = delete_db(db_target, res_list[i], credentials_target[0], credentials_target[1])
               #print type(del_res.status_code)
               #print type(res_list[i])
               logger.info("Delete: %s , result: %d" % (str(res_list[i]), del_res.status_code))
               if del_res.status_code != 200:
                   logger.error("Error deleting DB: %s, reslut %d" % (str(res_list[i]), del_res.status_code))
        else:
            logger.info("There no DB to delete")

        logger.info("End")
    except:
        logger.exception("Exception while deleting DB ")
        sys.exit(1)
    sys.exit(0)

#Single DB replicate
try:
    if args.dbname is not None:
        logger.info("Replicating: %s" % args.dbname)
        db_name = args.dbname
        r = create_db(db_target,db_name,credentials_target)
        if r != True:
            logger.error("Error during creation of %s" % src_dbs[i])
        else:
            logger.info("Create DB if it's not exist")
            replicate_request = replicate_db(db_name, db_source, db_target_ip, credentials_source[0], credentials_source[1])
            if 'error' in replicate_request:
                log_err = "Replication error: {}, reason: {}"
                log_err = log_err.format(replicate_request['error'],replicate_request['reason'])
                logger.error(log_err)
            elif 'ok' in replicate_request:
                if 'session_id' in replicate_request and 'history' in replicate_request:
                    log_srt = "Replication success: session_id: {}, start_time: {} ,end_time: {}, missing_checked: {}, missing_found: {}, docs_read: {}, docs_written: {}, doc_write_failures: {}"
                    log_srt = log_srt.format(replicate_request['session_id'],replicate_request['history'][0]['start_time'],replicate_request['history'][0]['end_time'],replicate_request['history'][0]['missing_checked'],replicate_request['history'][0]['missing_found'],replicate_request['history'][0]['docs_read'],replicate_request['history'][0]['docs_written'],replicate_request['history'][0]['doc_write_failures'])
                    logger.info(log_srt)
                else:
                    logger.info("Replication success no changes:{}".format(str(replicate_request['no_changes'])))
            else:
                logger.error("Unexpected result: %s" % str(replicate_request))
except:
    logger.exception("Exception due replicating DB ")
    sys.exit(1)
sys.exit(0)
