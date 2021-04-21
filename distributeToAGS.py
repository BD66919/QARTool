from threading import Thread
import time
from watchdog.observers import Observer
from watchdog.events import *
import shutil
import os
import configparser
import logging
from recordH5File import recordH5Name
from analysisTxt import readFileNo

class MonitorThread(Thread):
    def __init__(self, name,number):
        Thread.__init__(self)
        config = configparser.ConfigParser()    
        config.read(os.getcwd()+'\\config.ini')
        self.name = name
        self.number = number
        
        self.AGSAnalyzeFolder = config.get('path','AGSAnalyzeFolder'+str(self.number))

        self.filePath = config.get('path','transPath'+str(self.number))
        self.generateHDF5Folder = config.get('path','generateHDF5Folder'+str(self.number))

        self.dbFile = config.get('path','dbPath')
        print("线程"+str(self.number)+"将源文件夹"+self.filePath+"发送至"+self.AGSAnalyzeFolder)
        
    
    def run(self):
        while True:
            try:
                monitor(self.name,self.filePath,self.AGSAnalyzeFolder)
            except IndexError:
                logging.info("源文件夹"+self.filePath+"已空,"+self.name+"终止\n")
                print("源文件夹"+self.filePath+"已空,"+self.name+"终止\n")
                #记录生成的hdf5文件名
                recordH5Name(self.generateHDF5Folder,self.dbFile)
                readFileNo(self.AGSAnalyzeFolder,self.dbFile)
                break
            except:
            	logging.warning("thread1脚本，monitor异常")
           
def monitor(name,filePath,targetPath):
    observer = Observer()
    event_handler = FileEventHandler(name,filePath,targetPath)
    observer.schedule(event_handler,targetPath,False)
    observer.start()
    time.sleep(3)
    observer.stop()
    observer.join()
    
class FileEventHandler(FileSystemEventHandler):
    def __init__(self,threadName,filePath,targetPath):
        FileSystemEventHandler.__init__(self)
        self.threadName = threadName
        self.filePath = filePath
        self.targetPath = targetPath
        targetFiles = os.listdir(targetPath)
        if(len(targetFiles)==4):
            os.chdir(self.filePath)
            files = os.listdir()
            shutil.move(self.filePath+'\\'+files[0],self.targetPath)

    def on_deleted(self, event):
        if event.is_directory:
            print("{0} ：删除{1}\n".format(self.threadName,event.src_path))
        else:
            print("{0} ：删除{1}\n".format(self.threadName,event.src_path))
            targetFiles = os.listdir(event.src_path[0:len(event.src_path)-len(event.src_path.rsplit('\\')[-1])])
            if(len(targetFiles)==4):
                os.chdir(self.filePath)
                files = os.listdir()
                try:
                    shutil.move(self.filePath+'\\'+files[0],self.targetPath)         
                except:
                    logging.warning("thread脚本，shutil.move异常")
                    pass
                
def distributeToAGSStart():
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename = 'record.log', level = logging.DEBUG, format = LOG_FORMAT)

    thread1 = MonitorThread("线程1",1)
    thread2 = MonitorThread("线程2",2)
    thread3 = MonitorThread("线程3",3)
    thread4 = MonitorThread("线程4",4)
    
    time.sleep(5)
    print("程序运行中")
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

