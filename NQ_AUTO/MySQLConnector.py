# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 17:18:29 2018

@author: hadoop
"""

#import MySQLdb
import pymysql as MySQLdb
import time

class MySQLConnector(object):
    def openConnector(self,dbID):
        '''
            Try to connect to the MySQL Server until succeed
        '''
        if dbID == '12':
            Host = '192.168.1.12'
            User = 'u_weather'
            Passwd = 'u_weather'
            Db = 'weather'
            Port = 3306
        elif dbID == '13':
            Host = '192.168.1.13'
            Port = 3306
            User = 'zhangweijian'
            Passwd = 'zhangweijian'
            Db = '600ly_basic_data'
        Charset = 'utf8'

        # 记录是否链接数据库成功
        self.__connected = False
        while not self.__connected:
            try:
                self.__connToDataSource = MySQLdb.connect(host=Host,port=Port,user=User,password=Passwd,database=Db,charset=Charset)
                self.__connected = True
            except IndexError:
                print(u'尝试重新连接数据库中 ...')
                time.sleep(1)
        self.cursor = self.__connToDataSource.cursor()
        
    def closeConnector(self):
        if self.__connected:
            self.__connToDataSource.commit()
            self.cursor.close()
            self.__connToDataSource.close()
        else:
            print(u'尚未对数据库的连接，所以不能关闭连接')
            
if __name__=='__main__':
    conn = MySQLConnector()
    conn.openConnector()
    conn.cursor.execute("show tables")
    tables = conn.cursor.fetchall()
    print(tables)
