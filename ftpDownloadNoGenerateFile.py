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
import xlrd
from unrar import rarfile
import zipfile
import os
import shutil

class FTP:
    ftp = ftplib.FTP()
    def Connect(self, host, port):
        self.ftp.connect(host, port)
        self.ftp.encoding = 'utf-8'
 
    def Login(self, user, passwd):
        self.ftp.login(user, passwd)
        self.ftp.sendcmd('opts utf8 on')

    def nlst(self):
        return self.ftp.nlst()

    def cwd(self,path):
        return self.ftp.cwd(path)
    
    def close(self):
        self.ftp.quit()#ftp关闭

        
    def DownLoadFile(self, ftpFilePath, localFilePath):  # 下载单个文件
        if(os.path.exists(localFilePath)):
            if(os.path.getsize(localFilePath)==self.ftp.size(ftpFilePath)):
                logging.info(localFilePath+"文件已存在，跳过")
                print(localFilePath+"文件已存在，跳过")
                return True

        localRecive = open(localFilePath, 'wb') #创建本地文件
        self.ftp.retrbinary('RETR ' + ftpFilePath, localRecive.write)#接收服务器上文件并写入到本地文件内
        print(localFilePath+"下载完毕")
        logging.info(localFilePath+"下载完毕")
        localRecive.close()
        if(os.path.getsize(localFilePath)!=self.ftp.size(ftpFilePath)):
            logging.warning(localFilePath+"下载文件大小不与服务器文件大小一致，重新下载")
            print(localFilePath+"下载文件大小不与服务器文件大小一致，重新下载")
            self.DownLoadFile(ftpFilePath,localFilePath)
        return True

    
if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(os.getcwd()+'\\config.ini')

    
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename='record.log', level=logging.DEBUG, format=LOG_FORMAT)


    downloadFolder = config.get('path',"downloadFolder")
    temporary = config.get('path','temporary')
    ftpIp = config.get('ftp','ftpIp')
    ftpPort = config.get('ftp','ftpPort')
    ftpUser = config.get('ftp','ftpUser')
    #ftpPassword = config.get('ftp','ftpPassword')
    ftpPassword = 'Xtadliaw@%52923'

    AGSAnalyzeFolder = config.get("path","AGSAnalyzeFolder")
    
    if not os.path.exists(downloadFolder):#创建下载文件夹
            os.makedirs(downloadFolder)
            
    
    if not os.path.exists(temporary):#创建解压临时文件夹
            os.makedirs(temporary)
    
    wb = xlrd.open_workbook("未生产H5航班.xls")
    sheet1 = wb.sheet_by_index(0)
    col = sheet1.ncols
    row = sheet1.nrows-1
    number = sheet1.nrows-1
    status = config.get("path","COMMERCIAL")
    if(status == '1'):
            print("当前模式只下载状态为COMMERCIAL原始文件，可在config中修改COMMERCIAL进行更改")

    for i in range(0,col):
        if sheet1.cell(0,i).value == "FILE_FTP_PATH":
            FILE_FTP_PATH_NUMBER = i
        elif sheet1.cell(0,i).value == "PATH_INZIP":
            PATH_INZIP_NUMBER = i
        elif sheet1.cell(0,i).value == "FLIGHT_TYPE":
            FLIGHT_TYPE_NUMBER = i
    FILE_FTP_PATH_LIST = []
    PATH_INZIP_LIST = []
    for i in range(1,row+1):
        if(status == '1'):
            if(sheet1.cell(i,FLIGHT_TYPE_NUMBER).value == "COMMERCIAL"):
                FILE_FTP_PATH_LIST.append(sheet1.cell(i,FILE_FTP_PATH_NUMBER).value.replace('/','\\'))
                PATH_INZIP_LIST.append(sheet1.cell(i,PATH_INZIP_NUMBER).value)
        else:
            FILE_FTP_PATH_LIST.append(sheet1.cell(i,FILE_FTP_PATH_NUMBER).value.replace('/','\\'))
            PATH_INZIP_LIST.append(sheet1.cell(i,PATH_INZIP_NUMBER).value)
        
    row = len(FILE_FTP_PATH_LIST)
    number = len(FILE_FTP_PATH_LIST)
    '''
    ftpIp = config.get('ftpTest','ftpIp')
    ftpPort = config.get('ftpTest','ftpPort')
    ftpUser = config.get('ftpTest','ftpUser')
    ftpPassword = config.get('ftpTest','ftpPassword')
    '''
    
    ftp = FTP()
    ftp.Connect(ftpIp,int(ftpPort))
    ftp.Login(ftpUser, ftpPassword)


    logging.info("已连接至"+ftpIp+"FTP服务器")
    print("已连接至"+ftpIp+"FTP服务器")

    for i in range(0,len(FILE_FTP_PATH_LIST)):
       
        fileName = FILE_FTP_PATH_LIST[i][FILE_FTP_PATH_LIST[i].rfind('\\')+1:]
        
        try:
            if(fileName.endswith(".rar")):
                fileName = FILE_FTP_PATH_LIST[i][FILE_FTP_PATH_LIST[i].find('\\')+1:FILE_FTP_PATH_LIST[i].rfind('\\')] + "-"+fileName
                ftp.DownLoadFile(FILE_FTP_PATH_LIST[i],downloadFolder+"\\"+fileName)
                
            if(fileName.endswith(".wgl")):
                if not os.path.exists(downloadFolder+"\\"+fileName):
                    os.makedirs(downloadFolder+"\\"+fileName) 
                localFileName = fileName

                fileFtpPath = FILE_FTP_PATH_LIST[i][FILE_FTP_PATH_LIST[i].find('\\')+1:FILE_FTP_PATH_LIST[i].rfind('\\')] + "\\"+fileName
                ftp.cwd(fileFtpPath)
                wglFileNames = ftp.nlst()
                for wgl in wglFileNames:
                    ftp.DownLoadFile(r''+"\\"+os.path.join(fileFtpPath,wgl),r''+downloadFolder+"\\"+localFileName+"\\"+wgl)
                ftp.cwd("\\")
                
            else:
                ftp.DownLoadFile(FILE_FTP_PATH_LIST[i],downloadFolder+"\\"+fileName)

        except:
            print(FILE_FTP_PATH_LIST[i]+"下载失败,请手动尝试")
            logging.info(FILE_FTP_PATH_LIST[i]+"下载失败,请手动尝试")
        
    ftp.close()
    logging.info("所有文件下载完毕!存放在"+downloadFolder+"开始解压")

    
    print("所有文件下载完毕!存放在"+downloadFolder+"\n开始解压")
    
    index = 0
    
    for i in range(0,len(FILE_FTP_PATH_LIST)):
        if(FILE_FTP_PATH_LIST[i].endswith(".rar")==False):
            sub = index%4+1
            index = index+1
            fileName = FILE_FTP_PATH_LIST[i][FILE_FTP_PATH_LIST[i].rfind('\\')+1:]
            print(fileName)
            try:
                shutil.move(downloadFolder+"\\"+fileName, AGSAnalyzeFolder+str(sub))
                print(FILE_FTP_PATH_LIST[i]+"--"+PATH_INZIP_LIST[i]+"发送至AGS译码文件夹"+ AGSAnalyzeFolder+str(sub)+"下成功")
                number = number - 1
            except Exception as e:
                if(str(e).endswith("exists")):
                   print(FILE_FTP_PATH_LIST[i]+"--"+PATH_INZIP_LIST[i]+"已存在AGS译码文件夹"+ AGSAnalyzeFolder+str(sub)+"下")
                   number = number - 1
                   continue
            continue
        
        try:
            fileName = FILE_FTP_PATH_LIST[i][FILE_FTP_PATH_LIST[i].find('\\')+1:FILE_FTP_PATH_LIST[i].rfind('\\')] + "-"+ FILE_FTP_PATH_LIST[i][FILE_FTP_PATH_LIST[i].rfind('\\')+1:]
            rar = rarfile.RarFile(downloadFolder+"\\"+fileName)
        except Exception as e:
            print(e)
            pass
        result = 0
        for rarinfo in rar.infolist():
            filename = rarinfo.filename
            
            if(filename.find(PATH_INZIP_LIST[i])==0):
                rar.extract(filename,temporary)
                result = 1
                
        if(result == 0):
            print(FILE_FTP_PATH_LIST[i]+"--"+PATH_INZIP_LIST[i]+"未找到")
            continue
        
        sub = index%4 + 1
        index = index+1
        try:
            shutil.move(temporary+"\\"+PATH_INZIP_LIST[i], AGSAnalyzeFolder+str(sub))
            print(FILE_FTP_PATH_LIST[i]+"--"+PATH_INZIP_LIST[i]+"发送至AGS译码文件夹"+ AGSAnalyzeFolder+str(sub)+"下成功")
            number = number - 1
        except Exception as  e:
            if(str(e).endswith("exists")):
               print(FILE_FTP_PATH_LIST[i]+"--"+PATH_INZIP_LIST[i]+"已存在AGS译码文件夹"+ AGSAnalyzeFolder+str(sub)+"下")
               number = number - 1
               continue
            print(e)

    print(str(number)+"条未匹配数据重新译码失败"+",总共"+str(row)+"条数据")        
            
    logging.info("解压完毕")
    print("解压与发送完毕")

