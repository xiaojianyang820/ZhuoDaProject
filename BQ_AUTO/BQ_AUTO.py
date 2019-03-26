#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 15:44:05 2018

@author: hadoop
"""

import pandas as pd
import numpy as np
from MySQLConnector import MySQLConnector
import datetime,time
import requests,json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import parseaddr, formataddr


# 格式化邮件地址
def formatAddr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))
def sendMail(body,currentTime):
    smtp_server = 'smtp.qq.com'
    from_mail = '980890611@qq.com'
    mail_pass = 'exmlevxkxrkwbcec'
    to_mail = ['xiaojianyang820@163.com','chenxugao@600ly.com','gankai@600ly.com']
    # 构造一个MIMEMultipart对象代表邮件本身
    msg = MIMEMultipart()
    # Header对中文进行转码
    msg['From'] = formatAddr('数据中心 <%s>' % from_mail)
    msg['To'] = ','.join(to_mail)
    msg['Subject'] = Header('<%s>:北区运行方案'%currentTime, 'utf-8').encode()
    # plain代表纯文本
    msg.attach(MIMEText(body, 'plain', 'utf-8'))
    try:
        s = smtplib.SMTP()
        s.connect(smtp_server, "25")
        s.login(from_mail, mail_pass)
        s.sendmail(from_mail, to_mail, msg.as_string())  # as_string()把MIMEText对象变成str
        s.quit()
    except smtplib.SMTPException as e:
        print("Error: %s" % e)
        
def postCommend(projectID,currentMultiplier,currentTemp,currentLoadFactor,currentLoadSet,
                targetGSTemp,targetTempDiff,strategyTime):

    url = 'http://wechat.600ly.com/600ly-manage-data/runBrief/add.json?projectId=%s&currentMultiplier=1&currentTemp=%.2f&currentLoadFactor=%.2f&currentLoadSet=%d&targetGSTemp=%.1f&targetTempDiff=%.1f&strategyTime=%s'%(projectID,currentTemp,currentLoadFactor,currentLoadSet,targetGSTemp,targetTempDiff,strategyTime)
    headers = {
            'Device-Identifier':'ce8523a41875db7e64941e930e035889',
            'Content-Type':'application/json',
            'App-Name':'xfn',
            'Device-Time':'1446293675184',
            'Hmac':'ab2e05f026567a779c49bec8f7c12751e1bfd34dc140aba72639c89d2d36edb9a7c770538576ab9eed9b843d079bb2582531cbe9ff33c052e80036e37841b472',
            'User-Id':'0',
            'System-Version':'6.0',
            'App-Version':'2.0-debug',
            'System-Name':'Web'
            }
    try:
        req = requests.post(url,headers=headers)
    except:
        pass
    


def main():
    LoadingMap = {
            4:2.7,
            5:2.8,
            6:2.8,
            7:2.8,
            8:2.7,
            9:2.6,
            10:2.6,
            11:2.6,
            12:2.6,
            13:2.6,
            14:2.6,
            15:2.6,
            16:2.7,
            17:2.8,
            18:2.8,
            19:2.8,
            20:2.8,
            21:2.7,
            22:2.6,
            23:2.6,
            0:2.5,
            1:2.5,
            2:2.5,
            3:2.5,
            }
    # 供回水温度映射表
    data = pd.read_excel('供回水温度映射表.xlsx',sheetname='北区供回水映射表')
    Supply2BackMap = {}
    for i,j in data[['供水温度','回水温度（修正）']].values:
        Supply2BackMap[i] = j
    # 获取当前的时间和温度
    currentTime = datetime.datetime.now()
    currentHour = currentTime.hour
    currentTime_ = datetime.datetime.strftime(currentTime,'%Y-%m-%d %H:%M:%S')
    expiredTime = currentTime + datetime.timedelta(0,60*30)
    expiredTime_ = datetime.datetime.strftime(expiredTime,'%Y-%m-%d %H:%M:%S')
    conn = MySQLConnector()
    conn.openConnector('13')
    SQL = 'SELECT temp FROM bd_weather_station_1 WHERE create_time>"%s" AND project_id="100005" ORDER BY create_time DESC LIMIT 50'%(currentTime-datetime.timedelta(0,60*20))
    conn.cursor.execute(SQL)
    tempData = [float(i[0]) for i in conn.cursor.fetchall()]
    tempData = np.mean(tempData)
    #########################################
    tempData -= 2
    #########################################
    SQL_2 = 'SELECT ZG_HS_T FROM bd_nq_mainstation ORDER BY create_time DESC LIMIT 5'
    conn.cursor.execute(SQL_2)
    HST = np.mean([float(i[0]) for i in conn.cursor.fetchall()])
    
    SQL_2 = 'SELECT ZG_GS_T FROM bd_nq_mainstation ORDER BY create_time DESC LIMIT 5'
    conn.cursor.execute(SQL_2)
    GST = np.mean([float(i[0]) for i in conn.cursor.fetchall()])
    # 获取锅炉能正常开启的台数
    SQL_3 = 'SELECT GL1_O_PR,GL2_O_PR,GL3_O_PR,GL4_O_PR,GL5_O_PR,GL6_O_PR,GL9_O_PR,GL10_O_PR,GL11_O_PR,GL12_O_PR,GL13_O_PR FROM bd_nq_mainstation_gl ORDER BY create_time DESC LIMIT 20'
    conn.cursor.execute(SQL_3)
    O_PR_Num_1 = np.array(conn.cursor.fetchall())
    O_PR_Num_1 = [1 if i >= 2 else 0 for i in np.mean(O_PR_Num_1,axis=0)]
    O_PR_Num = sum(O_PR_Num_1)
    
    SQL_Temp_Lux = "SELECT temp,lux FROM bd_weather_station_1 WHERE project_id='100005' AND create_time > '%s' ORDER BY create_time DESC LIMIT 50"
    SQL_Temp_Lux = SQL_Temp_Lux%(currentTime-datetime.timedelta(0,10*60))
    conn.cursor.execute(SQL_Temp_Lux)
    data_Temp_Lux = np.array([[float(i[0]),float(i[1])] 
                             for i in conn.cursor.fetchall()])
    CurrentLux = np.mean(data_Temp_Lux[:,1])
    
    conn.closeConnector()
    # 评估上一轮次的预估效果
    global LastWC
    if LastWC != 0:
        CS = (GST-HST)/LastWC
    else:
        CS = 1.0
    print('本轮次的乘数为:%.2f'%CS)
    '''
    if CS > 1.04 and CS < 1.5:
        with open('P.txt','r') as f:
            P = float(f.read())
        with open('P.txt','w') as f:
            f.write(str(P - 2))
    elif CS < 0.96 and CS > 0.8:
        with open('P.txt','r') as f:
            P = float(f.read())
        with open('P.txt','w') as f:
            f.write(str(P + 2))
    '''
    # 所需要的供回水温差为：
    Loading = LoadingMap[currentHour]
    if CurrentLux > 40000:
        Loading -= 0.4
    elif CurrentLux > 30000:
        Loading -= 0.3
    elif CurrentLux > 25000:
        Loading -= 0.2
    elif CurrentLux > 20000:
        Loading -= 0.15
    elif CurrentLux > 15000:
        Loading -= 0.1
    '''
    if tempData > 5:
        Loading -= 0.15
    elif tempData > 3:
        Loading -= 0.1
    elif tempData > 0:
        Loading -= 0.05
    '''
    with open('K.txt','r') as f:
        K = float(f.read())
        
    Loading = Loading * np.exp((-5-tempData)*K)
    S2B_Diff = Loading/10 * (18 - tempData)
    # 计算所需的供水温度：
    targetT = 0
    minDiff = 100
    for t in np.arange(26,59,0.5):
        b = Supply2BackMap[t]
        d = abs((t-b) - S2B_Diff)
        if d < minDiff:
            targetT = t
            minDiff = d
            
    '''
    targetTDiff = max(0.8,abs(targetT - GST)*0.4)
    if abs(targetT - GST) > targetTDiff:
        if targetT > GST:
            targetT = GST + targetTDiff
        else:
            targetT = GST - targetTDiff
    '''
    #
    
    
    #
    
    # 获取此刻需要提升的温度
    ########################
#    currentClock = datetime.time(currentTime.hour,currentTime.minute)
#    if currentClock >=datetime.time(5,30) and currentClock < datetime.time(8,30):
#        targetT = 33
#    elif currentClock >= datetime.time(16,30) and currentClock < datetime.time(20,30):
#        targetT = 33
#    else:
#        targetT -= 6
    ########################
    tempData_ = round(tempData)
    if tempData_ > 12:
        targetT = 23
    elif tempData_ >= 10:
        targetT = 24
    elif tempData_ >= 9:
        targetT = 25
    elif tempData_ >= 8:
        targetT = 26
    elif tempData_ >= 7:
        targetT = 27
    elif tempData_ >= 6:
        targetT = 28.5
    elif tempData_ >= 5:
        targetT = 30
    elif tempData_ >= 4:
        targetT = 31
    elif tempData_ >= 3:
        targetT = 32
    else:
        targetT = 32
        
    if currentTime.hour >=22 or currentTime.hour < 5:
        targetT -= 2
    
    if currentTime.hour >= 9 and currentTime.hour <=16:
        targetT -= 2
    
    
    
    
    
    targetT = max(28,targetT)
    upTemp = targetT - HST
    # 潜在负荷设定
    print('目前能开机的台数为：%d'%O_PR_Num)
    NUM = O_PR_Num
    with open('P.txt','r') as f:
        P = float(f.read())
    P_SZ = upTemp * P
    P_SZ_Sin = P_SZ/NUM
    P_SZ_Sin = min(int(P_SZ_Sin - 18.6),92)
    P_SZ_Sin = max(6,P_SZ_Sin)
    
    
    # 
    printState = '''
            %s:
               当前的环境温度 %.1f
               当前的光照强度为 %d
               当前时刻的负荷系数为 %.2f
               单机负荷率设定为 %d
               目标供水温度为 %.1f
               需要提升的温差为：%.2f
            '''%(currentTime_,tempData,CurrentLux,Loading/10,int(P_SZ_Sin),targetT,upTemp)
    print(printState)
    print('+++++++++++++++++++++++++++++++++++++++++++++++++++')
    #sendMail(printState,currentTime_)
    try:
        postCommend('100004',1,tempData,Loading/10,int(P_SZ_Sin),targetT,upTemp,currentTime_)
    except:
        print("WeChat Error")
        pass
    
    BoilerCode = {1:5,2:6,3:9,4:10,5:5,6:2,7:9,8:10,9:11,10:12,11:13}
    controlURL = 'http://47.94.209.71:80/600ly-ctrl/cmdCenter/sendBatchCmd.json'
    jsons = []
    for i in range(4):
        i = BoilerCode[i+1]
        data = {
                'itemName':'NQ\GL\GL%d_O_PR_K.PV'%i,
                'itemValue':'%d'%P_SZ_Sin,
                'projectId':'100004',
                'fromPlatform':'app',
                'downType':'O_PR_K',
                'expiryTime':'%s'%expiredTime_,
                'executionTime':'%s'%currentTime_
                }
        jsons.append(data)
    JSON = json.dumps(jsons)
    headers = {
            'Device-Identifier':'ce8523a41875db7e64941e930e035889',
            'Content-Type':'application/json',
            'App-Name':'xfn',
            'Device-Time':'1446293675184',
            'Hmac':'ab2e05f026567a779c49bec8f7c12751e1bfd34dc140aba72639c89d2d36edb9a7c770538576ab9eed9b843d079bb2582531cbe9ff33c052e80036e37841b472',
            'User-Id':'0',
            'System-Version':'6.0',
            'App-Version':'2.0-debug',
            'System-Name':'Web'
            }
    
    print(requests.post(controlURL,headers=headers,json=JSON))
    # 存储理论计算温差
    LastWC = upTemp
    # 插入数据库
    conn = MySQLConnector()
    conn.openConnector('12')
    SQL = "INSERT INTO CommondRecord(Distrinct,create_time,Setting,Prod) VALUES('%s','%s',%d,%.2f)"
    SQL = SQL%('北区',currentTime_,P_SZ_Sin,CS)
    conn.cursor.execute(SQL)
    conn.closeConnector()
if __name__ == "__main__":
    LastWC = 0
    
    while True:
        current = datetime.datetime.now()
        if current.minute >= 0 and current.minute < 8:
            main()
            time.sleep(60*20)
        elif current.minute >= 30 and current.minute < 39:
            main()
            time.sleep(60*20)
        else:
            time.sleep(20)
            