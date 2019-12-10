import couchdb
import argparse
import logging
import sys
import string
from faker import Faker
import random
from random import randint
import threading
import time
import multiprocessing
import urllib3
# Classes


class DocGenerator:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DocGenerator, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.fake = Faker('en_US')

    def create_doc(self):
        return {'id': randint(0, 100), 'bar': {'name': self.fake.name(), 'score': randint(0, 100),'address': self.fake.address(), 'text': self.fake.text()}}


class CouchdbServer:

    def __init__(self,server='localhost',port=5984):
        self.server = couchdb.Server('http://%s:%s/' % (server, port))
        self.counter = 0
        self.generator = DocGenerator()

    def random_string(self,string_length):
        """ Generate random string with lowercase and uppercase with fixed length"""
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(string_length))

    def create(self, dbname=None):
            if dbname is None:
                dbname = self.random_string(10)
            db = self.server.create(dbname)
            if db:
                return db
            else:
                return False



    def delete_database(self, database):
        self.server.delete(database)
        return True

    def insert_doc(self,database):
        doc_id, doc_rev = database.save(self.generator.create_doc())
        return database[doc_id]

    def delete_doc(self,database,doc_id):
        database.delete(doc_id)
        return True

    def run(self,timeout=10,db=None):
        if db is None:
            db = self.create_database()
        timeout_start = time.time()
        counter = 0
        while time.time() < timeout_start + timeout:
            # insert document
            doc = self.insert_doc(db)
            self.delete_doc(db, doc)
            counter += 1
        self.delete_database(db.name)
        return counter/timeout


# Arg parser
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--server',action='store', help='hostname or IP of target server, default to localhost',default='localhost')
parser.add_argument('-p', '--port',action='store', help='port of target server default to 5984',default='5984')
parser.add_argument('-u','--user', action='store', help='couchdb username')
parser.add_argument('-th','--threads',action='store',help='number of threads to start, default 10',default=10)
parser.add_argument('-t','--time',action='store',help='number of seconds to run, default 10',default=10)

# logging
logger = logging.getLogger("Log format")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s  %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# worker

def worker(server,timeout,que):
    que.put(server.run(timeout=timeout))



# main

if __name__ =='__main__':
    args = parser.parse_args()
    serv = CouchdbServer(args.server, args.port)
    q = multiprocessing.Queue()
    for i in range(int(args.threads)):
        t = multiprocessing.Process(target=worker,args=(serv,int(args.time),q))
        print('++++')
        t.start()
    sum = 0
    while True:
        r = q.get()
        sum += r
        print('##############################################')
        print(sum)
        time.sleep(1)



