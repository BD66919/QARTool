#python3.6
#from ctypes import *
import os
import sys
import ftplib
import datetime
import time
from unrarByMonth import unrarByMonth
import configparser
import logging
from sqlConnect import SQL
from distributeToAGS import distributeToAGSStart
import re

class FTP:
    ftp = ftplib.FTP()
    def Connect(self, host, port):
        self.ftp.connect(host, port)
        self.ftp.encoding = 'utf-8'
 
    def Login(self, user, passwd):
        self.ftp.login(user, passwd)
        self.ftp.sendcmd('opts utf8 on')

    def getA350FolderName(self,ftpDir):
        self.ftp.cwd(ftpDir)
        ftpNames = self.ftp.nlst()
        for fileName in ftpNames:
            if(fileName[0:4] == "A350"):
                return fileName
        
    def DownLoadFile(self, ftpFilePath, localFilePath):  # 下载单个文件
        if(os.path.exists(localFilePath)):
            if(os.path.getsize(localFilePath)==self.ftp.size(ftpFilePath)):
                logging.info(localFilePath+"文件已存在，跳过")
                print(localFilePath+"文件已存在，跳过")
                return True
        localRecive = open(localFilePath, 'wb') #创建本地文件
        print(localFilePath+'开始下载')
        logging.info(localFilePath+'开始下载')
        self.ftp.retrbinary('RETR ' + ftpFilePath, localRecive.write)#接收服务器上文件并写入到本地文件内
        logging.info(localFilePath+"下载完毕")
        localRecive.close()
        if(os.path.getsize(localFilePath)!=self.ftp.size(ftpFilePath)):
            logging.warning(localFilePath+"下载文件大小不与服务器文件大小一致，重新下载")
            print(localFilePath+"下载文件大小不与服务器文件大小一致，重新下载")
            self.DownLoadFile(ftpFilePath,localFilePath)
        return True

    def DownLoadQARFolder(self, ftpDir, localDir,  inputBegin, inputEnd):
        global benginDate
        global endDate
        self.ftp.cwd(ftpDir) #定位操作文件夹
        firstFileNames = self.ftp.nlst() #列出文件夹下文件
        for firstFileName in firstFileNames:
            if(firstFileName.lower().find('qar')!=-1):#获取文件夹或者压缩包名字
                firstLocalPath = os.path.join(localDir, firstFileName) 
                try:
                    beginDate = firstFileName[8:16]
                    endDate = firstFileName[17:21]
                    if(int(beginDate[4:6])>int(endDate[0:2])): 
                        endDate = str(int(beginDate[0:4])+1)+endDate
                    else:
                        endDate = beginDate[0:4]+endDate
                except:
                    print(firstFileName+"QAR文件夹包命名不正确，不包含日期，跳过")
                    continue
                if(dateContain(inputBegin,inputEnd,beginDate,endDate) == False ):#文件夹和文件日期不对就跳过
                    continue

                try:
                    self.ftp.cwd(firstFileName)#进入首目录下满足条件的文件夹
                except:
                    self.DownLoadFile(firstFileName,firstLocalPath)#是压缩包时进行下载
                    continue
                if not os.path.exists(firstLocalPath):
                    os.makedirs(firstLocalPath) #本地创建文件夹
                secondFileNames = self.ftp.nlst()
                for secondFileName in secondFileNames:
                    secondLocalPath = os.path.join(firstLocalPath, secondFileName) 
                    if(secondFileName.lower().find('rar')!=-1):
                       self.DownLoadFile(secondFileName,secondLocalPath)#下载文件
                       continue
                    else:#进入飞机号文件夹
                        if not os.path.exists(secondLocalPath):
                            os.makedirs(secondLocalPath)#本地创建文件夹
                        self.ftp.cwd(secondFileName)
                        thirdFileNames = self.ftp.nlst()
                        for thirdFileName in thirdFileNames:
                            thirdLocalPath = os.path.join(secondLocalPath,thirdFileName)
                            
                            if(thirdFileName.lower().find("wgl") != -1):#获取wgl的开始和结束日期,满足时进行下载
                                beginDate = next(iter(re.findall(r"[2][0][0-9]{6,6}", thirdFileName)), None)
                                endDate = next(iter(re.findall(r"[2][0][0-9]{6,6}", thirdFileName)), None)
                                if(dateContain(inputBegin,inputEnd,beginDate,endDate) == False):
                                    continue
                                self.ftp.cwd(thirdFileName)
                                if not os.path.exists(thirdLocalPath):
                                    os.makedirs(thirdLocalPath) #本地创建文件夹
                                wglFileNames = self.ftp.nlst()
                                for wgl in wglFileNames:
                                    self.DownLoadFile(wgl,os.path.join(thirdLocalPath,wgl))
                                self.ftp.cwd("..")
                                continue
                                
                            if(thirdFileName.lower().find("rar") != -1):#获取2645等PC卡的开始和结束日期,满足时进行下载
                                beginDate = next(iter(re.findall(r"[2][0][0-9]{6,6}", thirdFileName)), None)
                                endDate = next(iter(re.findall(r"[2][0][0-9]{6,6}", thirdFileName)), None)
                                if(dateContain(inputBegin,inputEnd,beginDate,endDate) == True):
                                    self.DownLoadFile(thirdFileName,thirdLocalPath)
                            
                    self.ftp.cwd("..")
            self.ftp.cwd("..")   #返回
                    
    def DownLoadB787Folder(self, ftpDir, localDir,  inputBegin, inputEnd):
        global beginDate
        global endDate
        self.ftp.cwd(ftpDir) #定位操作文件夹
        ftpNames = self.ftp.nlst() #列出文件夹下文件
        for fileName in ftpNames:
            if(fileName[0:4].lower() != "b787" and fileName.lower().find("zip") == -1 and fileName.lower().find("raw") != -1): #不是B787文件夹或者QAR的rar压缩包就跳过
                continue
            if(fileName[0:4].lower() == "b787"):#是文件夹时，获取文件夹开始结束日期
                beginDate = fileName[5:13]
                endDate = fileName[14:22]
            else:
                beginDate = next(iter(re.findall(r"[2][0][0-9]{6,6}", fileName)), None)
                endDate = next(iter(re.findall(r"[2][0][0-9]{6,6}", fileName)), None)
            if(dateContain(inputBegin,inputEnd,beginDate,endDate) == False ):#文件夹和文件日期不对就跳过
                continue
            localPath = os.path.join(localDir, fileName) #定位文件路径
            
            try:
                self.ftp.cwd(fileName)
                self.ftp.cwd("..")
                if not os.path.exists(localPath):
                    os.makedirs(localPath) #本地创建文件夹
                self.DownLoadB787Folder(fileName,localPath,inputBegin,inputEnd) #循环进入下一个文件夹
            except:
                self.DownLoadFile(fileName,localPath)#下载文件
        self.ftp.cwd("..")#返回上一级

    def DownLoadA350Folder(self, ftpDir, localDir,  inputBegin, inputEnd):
        global year
        global month
        global day
        self.ftp.cwd(ftpDir) #定位操作文件夹
        ftpNames = self.ftp.nlst() #列出文件夹下文件    
        for fileName in ftpNames:
            localPath = os.path.join(localDir, fileName) #定位文件路径
            if(fileName.lower().find("gz")!=-1):#是文件时判断日期进行下载
                beginDate = "20"+fileName[11:17]
                endDate = "20"+fileName[11:17]
                
                if(dateContain(inputBegin,inputEnd,beginDate,endDate)):
                    self.DownLoadFile(fileName,localPath)#下载文件
            
            #年模块
            else:
                year = fileName[0:4]
                YearFolderPath = os.path.join(localDir, fileName)
                
                if(int(inputBegin[0:4]) <= int(year) and int(year) <= int(inputEnd[0:4])) == False: #不在范围内跳过
                        continue
                    
                if not os.path.exists(YearFolderPath):
                    os.makedirs(YearFolderPath) #本地创建年文件夹
                
                #下载年文件夹下的数据
                print(fileName[:-1])
                self.ftp.cwd(fileName) #进入年文件夹
                ftpYearFolderNames = self.ftp.nlst() #列出年文件夹下文件
                for fileYearFolderFileName in ftpYearFolderNames:
                    #月模块
                    if(fileYearFolderFileName.find("gz")!=-1):#是文件时下载
                        beginDate = "20"+fileYearFolderFileName[11:17]
                        endDate = "20"+fileYearFolderFileName[11:17]
                        YearFilePath = os.path.join(YearFolderPath,fileYearFolderFileName)
                        if(dateContain(inputBegin,inputEnd,beginDate,endDate)):
                            self.DownLoadFile(fileYearFolderFileName,YearFilePath)#下载文件
                    else:#是月的文件夹
                        #(fileYearFolderFileName.find("月")!=-1):
                        month = fileYearFolderFileName[0:2]
                        MonthFolderPath = os.path.join(YearFolderPath,fileYearFolderFileName)
                        
                        if(inputBegin[0:5]==inputEnd[0:5]):#同一年内
                            if(int(inputBegin[5:7]) <= int(month) and int(month) <= int(inputEnd[5:7]))== False:
                                continue
                        if not os.path.exists(MonthFolderPath):
                            os.makedirs(MonthFolderPath) #本地创建文件夹
                        self.ftp.cwd(fileYearFolderFileName) #进入月文件夹
                        ftpMonthFolderNames = self.ftp.nlst() #列出月文件夹下文件

                        
                        for fileMonthFolderFileName in ftpMonthFolderNames:
                            MonthFilePath = os.path.join(MonthFolderPath,fileMonthFolderFileName)
                            #日模块
                            if(fileMonthFolderFileName.lower().find("gz")!=-1):#是文件时进行下载
                                self.DownLoadFile(fileMonthFolderFileName,MonthFilePath)
                            else:#进入日文件夹下进行下载
                                
                                DayFolderPath = os.path.join(MonthFolderPath,fileMonthFolderFileName)
                                if not os.path.exists(DayFolderPath):
                                    os.makedirs(DayFolderPath) #本地创建文件夹
                                    
                                self.ftp.cwd(fileMonthFolderFileName)
                                ftpDayFolderNames = self.ftp.nlst()
                                
                                for fileDayFolderFileName in ftpDayFolderNames:#下载日文件夹下的文件
                                    if(fileDayFolderFileName.find("gz")!=-1):#是文件时下载
                                        DayFilePath = os.path.join(DayFolderPath,fileDayFolderFileName)
                                        self.DownLoadFile(fileDayFolderFileName,DayFilePath)#下载文件
                                self.ftp.cwd("..")
                        self.ftp.cwd("..")  
                    
                self.ftp.cwd("..")#返回上一级
        self.ftp.cwd("..")#返回上一级
    def close(self):
        self.ftp.quit()#ftp关闭

def dateContain(inputBegin,inputEnd,beginDate,endDate):
    beginDate = datetime.datetime.strftime(datetime.datetime.strptime(beginDate, '%Y%m%d'),'%Y-%m-%d')
    endDate = datetime.datetime.strftime(datetime.datetime.strptime(endDate, '%Y%m%d'),'%Y-%m-%d')
    
    if (inputBegin >= beginDate and inputBegin <= endDate) or (beginDate >= inputBegin and beginDate <= inputEnd):
        return True
    else:
        return False
    
if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(os.getcwd()+'\\config.ini')
    dbFile = config.get('path','dbPath')
    
    sql = SQL(dbFile)
    sql.create()
    sql.createTimeTable()
    timeStructNow = time.localtime()
    Nowtime = time.strftime('%Y%m%d%H%M%S',timeStructNow)
    selectTime = sql.selectTimeTable()[0]
    sql.updateTimeById(selectTime[0],Nowtime)
    
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename='record.log', level=logging.DEBUG, format=LOG_FORMAT)

    downloadPath = config.get('path','downloadPath')
    
    ftpIp = config.get('ftp','ftpIp')
    ftpPort = config.get('ftp','ftpPort')
    ftpUser = config.get('ftp','ftpUser')
    ftpPassword = config.get('ftp','ftpPassword')
    
    '''
    ftpIp = config.get('ftpTest','ftpIp')
    ftpPort = config.get('ftpTest','ftpPort')
    ftpUser = config.get('ftpTest','ftpUser')
    ftpPassword = config.get('ftpTest','ftpPassword')
    '''
    inputBegin = input("开始日期:yyyy-mm-dd\n")
    inputEnd = input("结束日期:yyyy-mm-dd\n")
    logging.info("开始日期:"+inputBegin)
    logging.info("结束日期:"+inputEnd)
    try:
        datetime.datetime.strptime(inputBegin, '%Y-%m-%d')
    except ValueError:
        logging.info("开始日期格式不正确")
        print("开始日期格式不正确")
        os._exit(0)
    try:
        datetime.datetime.strptime(inputEnd, '%Y-%m-%d')
    except ValueError:
        logging.info("开始日期格式不正确")
        print("结束日期格式不正确")
        os._exit(0)
    if inputBegin > inputEnd:
        logging.info("开始日期格式不正确")
        print("开始和结束日期不规范")
        os._exit(0)
    
    
    ftp = FTP()
    ftp.Connect(ftpIp,int(ftpPort))
    ftp.Login(ftpUser, ftpPassword)

    A350Name = ftp.getA350FolderName('/')

    logging.info("已连接至"+ftpIp+"FTP服务器")
    print("已连接至"+ftpIp+"FTP服务器")
    
    if not os.path.exists(downloadPath+'\\B787'):#创建787文件夹
            os.makedirs(downloadPath+'\\B787')
            
    A350Path =  downloadPath+'\\'+A350Name
    if not os.path.exists(A350Path):#创建A350文件夹
            os.makedirs(A350Path)
    logging.info("WQAR文件夹开始下载")
    print("WQAR文件夹开始下载")

    ftp.DownLoadQARFolder('/',downloadPath,inputBegin ,inputEnd)  # 将QAR文件夹下载到本地目录

    logging.info("B787文件夹开始下载")
    print("B787文件夹开始下载")
    ftp.DownLoadB787Folder('/B787/',downloadPath+'\\B787',inputBegin ,inputEnd)#将787文件夹下载到本地目录

    logging.info("A350文件夹开始下载")
    print("A350文件夹开始下载")
    ftp.DownLoadA350Folder(ftp.getA350FolderName("/"),A350Path,inputBegin,inputEnd)
        
    ftp.close()
    logging.info("所有文件下载完毕!开始解压")
    print("所有文件下载完毕!开始解压")
    unrarByMonth(inputBegin,inputEnd)
    logging.info("解压完毕")
    print("解压完毕")

    print("开始分发")
    distributeToAGSStart()
