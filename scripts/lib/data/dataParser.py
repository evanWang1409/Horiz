# -*- coding: utf-8 -*-

import csv
import os, time, sys
import pandas as pd
import xml.etree.ElementTree as ET
import mysql, mysql.connector
import chardet

hsFilePath = '../../../data/csvFiles/activities_HS.xlsx'
cFilePath = '../../../data/csvFiles/activities_C.xlsx'
CatFilePath = '../attributes.cat'

def getAttr():
    with open(CatFilePath, 'r') as f:
        attrStr = f.readline()
        attr = attrStr.split(',')
        if('\n' in attr[-1]):
            attr[-1] = attr[-1].replace('\n', '')
        return attr

def readCSV(filePath):
    activities = []
    with open(filePath, 'r') as csvFile:
        csvReader = csv.reader(csvFile, delimiter = ',')
        for row in csvReader:
            print(row)
    return activities

def readXLSX(filePath):
    xFile = pd.ExcelFile(filePath)
    xDataFrame = pd.read_excel(xFile)
    columnToRemove = xDataFrame.columns.values[-3:]
    xDataFrame = xDataFrame.drop(columnToRemove, axis = 1)
    attrs = getAttr()
    activities = []
    for _, row in xDataFrame.iterrows():
        attrList = []
        activityName = row['Name']
        try:
            activityName = str(activityName)
        except:
            activityName = activityName.encode("utf-8")
        if(chardet.detect(activityName)['encoding'] == 'ascii'):
            activityName = activityName.encode('utf-8')
        if(chardet.detect(activityName)['encoding'] != 'utf-8'):
            activityName = activityName.decode(chardet.detect(activityName)['encoding']).encode('utf-8')
        attrNum = 0
        missingVal = row.isnull()
        for i in range(1, len(attrs)):
            if not missingVal[attrs[i]]:
                attrNum += 1
                attr = row[attrs[i]]
                try:
                    attr = str(attr)
                except:
                    attr = attr.encode("utf-8")
                if(chardet.detect(attr)['encoding'] == 'ascii'):
                    attr = attr.encode('utf-8')
                if(chardet.detect(attr)['encoding'] != 'utf-8'):
                    attr = attr.decode(chardet.detect(attr)['encoding']).encode('utf-8')
                attrList.append(attr)
            else:
                attr = 'NaN'
                try:
                    attr = str(attr)
                except:
                    attr = attr.encode("utf-8")
                if(chardet.detect(attr)['encoding'] == 'ascii'):
                    attr = attr.encode('utf-8')
                    #attr = attr.encode('utf-8')
                    #print(chardet.detect(attr)['encoding'])
                if(chardet.detect(attr)['encoding'] != 'utf-8'):
                    attr = attr.decode(chardet.detect(attr)['encoding']).encode('utf-8')
                attrList.append(attr)
        attrList.append(attrNum)
        activities.append((activityName, attrList))
    return attrs, activities
    

def toSql(filePath, tableName):
    print(sys.version)
    horizDB = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        port = "3306",
        charset="utf8"
        )
    dbCursor = horizDB.cursor()

    createDBQuery = 'CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET gbk COLLATE gbk_chinese_ci'.format('HorizDB')
    dbCursor.execute(createDBQuery)

    dbCursor.execute('USE {}'.format('HorizDB'))

    if('xlsx' in filePath):
        attrs, activities = readXLSX(filePath)
    elif('csv' in filePath):
        attrs, activities = readCSV(filePath)
    else:
        raise Exception('error: unrecognized file type')
    
    clearTableQuery = 'DROP TABLE IF EXISTS HSContest'
    dbCursor.execute(clearTableQuery)

    createTableQuery = 'CREATE TABLE IF NOT EXISTS {} (Name VARCHAR(255) PRIMARY KEY'.format(tableName)
    for i in range(1, len(attrs)):
        if(attrs[i] == 'Group'):
            attrs[i] = '`Group`'
        newAttrQuery = ', {attrName} VARCHAR(255)'.format(attrName = attrs[i])
        createTableQuery += newAttrQuery
    createTableQuery += ', Priority INT)'
    dbCursor.execute(createTableQuery)

    encodeQuery = 'ALTER TABLE HSContest CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci'
    dbCursor.execute(encodeQuery)

    for activity in activities:
        insertQuery = 'INSERT INTO {tableName} VALUES ({activityName}{valueStr})'
        valueStr = ", '{}'"*(len(activity[1]))
        valueStr = valueStr.format(*activity[1])
        insertQuery = insertQuery.format(
            tableName = tableName,
            activityName = "'{}'".format(activity[0]),
            valueStr = valueStr
        )
        try:
            dbCursor.execute(insertQuery)
            sys.stdout.flush()
        except:
            continue
    horizDB.commit()
    print('Finished')
    
if __name__ == "__main__":
    toSql(hsFilePath, 'HSContest')
    #toSql(cFilePath, 'CContest')

