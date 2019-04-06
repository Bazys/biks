#!/usr/bin/env python
# coding: utf-8
# table for this script
# DROP table if exists `biks`;
# CREATE TABLE IF NOT EXISTS `biks` (
#     `bik` VARCHAR(9) NOT NULL,
#     `name` VARCHAR(256) NULL,
#     `reg_n` VARCHAR(16) NULL DEFAULT NULL,
#     `cntr_cd` VARCHAR(2) NULL,
#     `rgn` INT NULL,
#     `ind` VARCHAR(6) NULL,
#     `t_np` VARCHAR(64) NULL,
#     `n_np` VARCHAR(256) NULL,
#     `adr` VARCHAR(1024) NULL,
#     `date_in` DATE NULL,
#     `uid` VARCHAR(11) NULL DEFAULT NULL,
#     `account` VARCHAR(20) NULL,
#     PRIMARY KEY (`bik`),
#     INDEX `name` USING BTREE (`name` ASC)
# );

import urllib.request
import zipfile
import codecs
import xmltodict
import json
import os
from mysql.connector import connection
from mysql.connector import errorcode
import mysql.connector

MYSQL_USER = 'root'
MYSQL_DATABASE = 'energy'

try:
    local_filename, headers = urllib.request.urlretrieve(
        'http://www.cbr.ru/s/newbik')

except Exception as e:
    print(e)

zip_ref = zipfile.ZipFile(local_filename, 'r')
file_name = zip_ref.namelist()[0]
biks = xmltodict.parse(zip_ref.read(file_name))['ED807']['BICDirectoryEntry']
os.remove(local_filename)

add_bik = ("""
INSERT INTO biks (bik,name,reg_n,cntr_cd,rgn,ind,t_np,n_np,adr,date_in,uid,account)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
""")
try:
    cnx = connection.MySQLConnection(user=MYSQL_USER, password=os.environ['MYSQL_PWD'],
                                     host='127.0.0.1',
                                     database=MYSQL_DATABASE)
    cursor = cnx.cursor()
    cursor.execute("TRUNCATE biks;")
    for bik in biks:
        corr = None
        participant_info = bik.get('ParticipantInfo')
        accounts = bik.get('Accounts', False)
        if accounts:
            if type(accounts) is list:
                corr = accounts[0].get('@Account')
            else:
                corr = accounts.get('@Account')
        data_bik = (bik['@BIC'],
                    participant_info.get('@NameP'),
                    participant_info.get('@RegN', None),
                    participant_info.get('@CntrCd', 'RU'),
                    participant_info.get('@Rgn', None),
                    participant_info.get('@Ind', None),
                    participant_info.get('@Tnp', None),
                    participant_info.get('@Nnp', None),
                    participant_info.get('@Adr', None),
                    participant_info.get('@DateIn', None),
                    participant_info.get('@UID', None),
                    corr,
                    )
        cursor.execute(add_bik, data_bik)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cnx.commit()
    cursor.close()
    cnx.close()
