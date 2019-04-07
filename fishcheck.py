import os
import os.path
import sys
import hashlib
import pandas as pd
import time

DATABASE_IO_TIME = 300
BUFFERSIZE = 6553600
CRED = '\033[101m'
CGREENBG  = '\33[32m'
CYELLOWBG = '\33[33m'
CEND = '\033[0m'

def check2(rootdir):
    build(rootdir,"check");

def build(rootdir, switch):
    print ("func build");
    print (rootdir + '.csv');
    if os.path.isdir(rootdir + '.csv'): 
        print(rootdir + '.csv' + ' is a directory, remove it and retry');
        exit(0);
    if os.path.exists(rootdir + '.csv'): 
        print("file existed");
        tmp = input("overwrite? y/n")
        if tmp != 'y':
            exit(0);

    database = pd.DataFrame(columns=['path','checksum','mtime'])
    timer = time.time()  
    for path,dirs,files in os.walk(rootdir):
        for file in files:
            hasher = hashlib.sha1()
            utffilename = os.path.join(path, file)
            u2 = utffilename
            #u2 = utffilename.encode('utf-8')
            statbuf = os.stat(u2)
            print("mtime: ", os.path.getmtime(u2));
            with open(u2, 'rb') as file_to_check:
                print("Building checksum: ", '{0}\r'.format(u2), end='');
                buf = file_to_check.read(BUFFERSIZE)
                while len(buf) > 0:
                    hasher.update(buf)
                    buf = file_to_check.read(BUFFERSIZE)
            database = database.append({'path': os.path.join(path,file), 'checksum': hasher.hexdigest(),'mtime':os.path.getmtime(u2)},ignore_index=True)
            if ((time.time() - timer) > DATABASE_IO_TIME):
                database.to_csv(rootdir + '.csv', encoding='utf-8')
                timer = time.time()
    database.to_csv(rootdir + '.csv', encoding='utf-8')

def check(rootdir):
    database = pd.read_csv(rootdir + '.csv')
    diffbase = pd.DataFrame(columns=['path','record_checksum','new_checksum'])
    for index, row in database.iterrows():
        hasher = hashlib.sha1()
        with open(row['path'], 'r') as file_to_check:
            buf = file_to_check.read(BUFFERSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = file_to_check.read(BUFFERSIZE)
        if hasher.hexdigest() != row['checksum']:
            diffbase = diffbase.append({'path': row['path'],'record_checksum': row['checksum'], 'new_checksum': hasher.hexdigest()},ignore_index=True)
            diffbase.to_csv(rootdir + '.fishdf', encoding='utf-8')
            print ("Different checksum found: ");
            print (CRED + row['path'] + CEND);
            print ("Original checksum: ",CGREENBG + row['checksum'] + CEND, "");
            print ("Record checksum: ",CYELLOWBG + hasher.hexdigest() + CEND,"\n");

if len(sys.argv) != 3:
    print("Usage: ", sys.argv[0]," [build][check]");
    exit(0);
if sys.argv[1] == 'build':
    build(sys.argv[2],"build");
    exit(0);
if sys.argv[1] == 'check':
    check(sys.argv[2]);
    exit(0);

arg = sys.argv[1]
print ("number of arguments: ",len(sys.argv));
