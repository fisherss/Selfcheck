import os
import os.path
import sys
import hashlib
import pandas as pd
import time
import difflib
from colorama import Fore, Back, Style, init

DATABASE_IO_TIME = 300
BUFFERSIZE = 6553600
CRED = '\033[101m'
CGREENBG  = '\33[32m'
CYELLOWBG = '\33[33m'
CEND = '\033[0m'

init()
def check3(rootdir):
    diff = pd.DataFrame(columns=['path','org_checksum','new_checksum','org_mtime','new_mtime'])
    org_pd = pd.read_csv(rootdir + '.csv')
    new_pd = pd.read_csv(rootdir + '.tmp')
    for index_org, org_row in org_pd.iterrows():
        for index_new, new_row in new_pd.iterrows():
            if (org_row['path'] == new_row['path']):
                if (org_row['checksum'] != new_row['checksum']) or (org_row['mtime'] != new_row['mtime']):
                    print (CRED + org_row['path'] + CEND);
                    print(org_row['checksum']);
                    print(new_row['checksum']);
                    print(org_row['mtime']);
                    print(new_row['mtime']);
                    diff = diff.append({'path': org_row['path'],'org_checksum': org_row['checksum'], 'new_checksum': new_row['checksum'],'org_mtime': org_row['mtime'], 'new_mtime': new_row['mtime']},ignore_index=True)

    diff.to_csv(rootdir + '.diff', encoding='utf-8')

def check2():
    with open('f.csv', 'r', encoding='utf-8') as hosts0:
        with open('f.tmp', 'r', encoding='utf-8') as hosts1:
            diff = difflib.unified_diff(
                hosts0.readlines(),
                hosts1.readlines(),
                fromfile='f.csv',
                tofile='f.tmp',
            )
            for line in diff:
                #sys.stdout.write(line)
                if line.startswith('+'):
                    print (CRED + line + CEND);
                elif line.startswith('-'):
                    print (CRED + line + CEND);
                elif line.startswith('^'):
                    print (CRED + line + CEND);


def build(rootdir, switch):
    print ("func build");
    print (rootdir + '.csv');
    #if (switch == "check"):
    #    rootdir = rootdir + '-tmp'
    if (switch == "check"):
        ext = '.tmp'
    else:
        ext = '.csv'
    if os.path.isdir(rootdir + ext): 
        print(rootdir + ext + ' is a directory, remove it and retry');
        exit(0);
    if os.path.exists(rootdir + ext): 
        print("file existed");
        tmp = input("overwrite? y/n")
        if tmp != 'y':
            exit(0);

    database = pd.DataFrame(columns=['path','checksum','mtime'])
    timer = time.time()  
    for path,dirs,files in os.walk(rootdir):
        for file in files:
            hasher = hashlib.sha1()
            filename = os.path.join(path, file)
            statbuf = os.stat(filename)
            print("mtime: ", os.path.getmtime(filename));
            with open(filename, 'rb') as file_to_check:
                print("Building checksum: ", '{0}\r'.format(filename), end='');
                buf = file_to_check.read(BUFFERSIZE)
                while len(buf) > 0:
                    hasher.update(buf)
                    buf = file_to_check.read(BUFFERSIZE)
            database = database.append({'path': os.path.join(path,file), 'checksum': hasher.hexdigest(),'mtime':os.path.getmtime(filename)},ignore_index=True)
            if ((time.time() - timer) > DATABASE_IO_TIME):
                database.to_csv(rootdir + ext, encoding='utf-8')
                timer = time.time()
    database.to_csv(rootdir + ext, encoding='utf-8')
    check3(rootdir)

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
    build(sys.argv[2], "check");
    exit(0);

arg = sys.argv[1]
print ("number of arguments: ",len(sys.argv));
