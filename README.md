couch_br.py
Script for backuping couchdb into folder, each DB will be stored in different folder.
All data stored in JSON in human readable format.
Also can restore all data from stored files.

USAGE: couch_br.py [-h] [-b] [-r] [-s SERVER] [-p PORT] [-d DIR] [-a AUTH]
                   [-u USER] [-f FILTER]

optional arguments:
  -h, --help            show this help message and exit
  -b, --backup          set to backup databases, can not be used with -r
  -r, --restore         set to restore databases, can not be used with -b
  -s SERVER, --server SERVER
                        IP for couchdb server
  -p PORT, --port PORT  couchdb port, default 5984
  -d DIR, --dir DIR     for backup directory where to save backup files, for
                        restore directory with files to be uploaded to server
  -a AUTH, --auth AUTH  path to file with credentials for Couchdb
  -u USER, --user USER  username in Couchdb
  -f FILTER, --filter FILTER
                        Filter or not filtering MODB, default no, to enable
                        set to yes

couch_replication.py
Make single time replication of all DBs from one couchdb to another.

USAGE: couch_replication.py [-h] [-s SOURCE] [-t TARGET] [-as AUTHSOURCE]
                            [-d DBNAME] [-at AUTHTARGET]

optional arguments:
  -h, --help            show this help message and exit
  -s SOURCE, --source SOURCE
                        IP of source couchdb, in format IP:PORT
  -t TARGET, --target TARGET
                        IP of target couchdb, in format IP:PORT
  -as AUTHSOURCE, --authsource AUTHSOURCE
                        path to file with credentials for source Couchdb
  -d DBNAME, --dbname DBNAME
                        database name for replication only single database
  -at AUTHTARGET, --authtarget AUTHTARGET
                        path to file with credentials for target Couchdb
