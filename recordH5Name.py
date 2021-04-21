from threading import Thread
import time
from watchdog.observers import Observer
from watchdog.events import *
import shutil
import os
import logging
import configparser
from sqlConnect import SQL
import _thread

class MonitorThread(Thread):
    def __init__(self, name,targetPath):
        Thread.__init__(self)
        self.name = name
        self.targetPath = targetPath
        
    def run(self):  
        while True:
            monitor(self.name,self.targetPath)

def monitor(name,targetPath):
    observer = Observer()
    event_handler = FileEventHandler(name,targetPath)
    observer.schedule(event_handler,targetPath,False)
    observer.start()
    time.sleep(3)
    observer.stop()
    observer.join()
    
class FileEventHandler(FileSystemEventHandler):
    def __init__(self,threadName,targetPath):
        FileSystemEventHandler.__init__(self)
        self.threadName = threadName
        self.targetPath = targetPath

    def on_created(self, event):
        if event.is_directory:
            print("{0} ：新增{1}\n".format(self.threadName,event.src_path))
            
        else:
            try:
                _thread.start_new_thread(recordH5Name,(event.src_path,))
            except Exception as e:
                logging.warn(str(e))

    def on_deleted(self,event):
        if event.is_directory:
            print("{0} ：删除{1}\n".format(self.threadName,event.src_path))
        else:
            try:
                _thread.start_new_thread(updateStatus,(event.src_path,))
            except Exception as e:
                logging.warn(str(e))

def recordH5Name(filePath):#新增h5时没有对应的原始数据就增加一条，有对应数据的话就匹配上去
    time.sleep(1)
    if(filePath.find('.hdf5')!=-1):
        sql = SQL()
        sql.create()
        fileACNo = filePath[filePath.find('B-'):filePath.find('B-')+6]
        fileH5Name = filePath[filePath.rfind('\\')+1:]
        fileDate = fileH5Name[0:8]
        fileNo = filePath[filePath.rfind('-')+1:filePath.rfind('.')]
        print(fileNo)
        result = sql.selectByFileNo(fileNo)
        if(len(result) == 0):
            sql.insert('',fileACNo,fileDate,'','2','',fileH5Name,'',fileNo)
        else:
            if(len(result[0][8]) >=1):
                sql.updateById(result[0][0],result[0][1],result[0][2],result[0][3],result[0][4],'3',result[0][6],fileH5Name,result[0][8],result[0][9])
            else:
                sql.updateById(result[0][0],result[0][1],result[0][2],result[0][3],result[0][4],'2',result[0][6],fileH5Name,result[0][8],result[0][9])
            
def updateStatus(filePath):#删除h5时修改status
    sql = SQL()
    fileH5Name = filePath[filePath.rfind('\\')+1:]
    result = sql.selectByH5Name(fileH5Name)
    if(len(result)>0):
        status = int(result[0][5])
        status = status - 1
        sql.updateById(result[0][0],result[0][1],result[0][2],result[0][3],result[0][4],str(status),result[0][6],'',result[0][8],result[0][9])

if __name__ == '__main__':
    config = configparser.ConfigParser()    
    config.read(os.getcwd()+'\\config.ini')
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename='record.log', level=logging.DEBUG, format=LOG_FORMAT)
    
    generateHDF5Folder1=config.get('path','generateHDF5Folder1')
    generateHDF5Folder2=config.get('path','generateHDF5Folder2')
    generateHDF5Folder3=config.get('path','generateHDF5Folder3')
    generateHDF5Folder4=config.get('path','generateHDF5Folder4')

    thread1 = MonitorThread("线程1",generateHDF5Folder1)
    thread2 = MonitorThread("线程2",generateHDF5Folder2)
    thread3 = MonitorThread("线程3",generateHDF5Folder3)
    thread4 = MonitorThread("线程4",generateHDF5Folder4)

    print("线程1监控"+generateHDF5Folder1)
    print("线程2监控"+generateHDF5Folder2)
    print("线程3监控"+generateHDF5Folder3)
    print("线程4监控"+generateHDF5Folder4)
    print("程序运行中")

    time.sleep(5)
    
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

    

