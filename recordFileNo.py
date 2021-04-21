from threading import Thread
import time
from watchdog.observers import Observer
from watchdog.events import *
import shutil
import os
import configparser
from analysisTxt import analysisTxt
import logging
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
        _thread.start_new_thread(readTxt,(event.src_path,))

def readTxt(path):
    time.sleep(5)
    print(path)
    looptimes = 1
    while looptimes < 5:
        try:
            result = analysisTxt(path+'\\ANAREP.TXT')
            if result == 1:
                looptimes = 5
            else:
                time.sleep(2)
                looptimes = looptimes + 1
        except Exception as e:
            looptimes = looptimes + 1
            time.sleep(2)
            logging.warn(str(e))
            continue

                
if __name__ == '__main__':
    config = configparser.ConfigParser()    
    config.read(os.getcwd()+'\\config.ini')
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename='record.log', level=logging.DEBUG, format=LOG_FORMAT)
    
    AGSAnalyzeFolder1=config.get('path','AGSAnalyzeFolder1')
    AGSAnalyzeFolder2=config.get('path','AGSAnalyzeFolder2')
    AGSAnalyzeFolder3=config.get('path','AGSAnalyzeFolder3')
    AGSAnalyzeFolder4=config.get('path','AGSAnalyzeFolder4')
 
    thread1 = MonitorThread("线程1",AGSAnalyzeFolder1+'\\AnalysisReport')
    thread2 = MonitorThread("线程2",AGSAnalyzeFolder2+'\\AnalysisReport')
    thread3 = MonitorThread("线程3",AGSAnalyzeFolder3+'\\AnalysisReport')
    thread4 = MonitorThread("线程4",AGSAnalyzeFolder4+'\\AnalysisReport')
    
    print("线程1监控"+AGSAnalyzeFolder1+'\\AnalysisReport')
    print("线程2监控"+AGSAnalyzeFolder2+'\\AnalysisReport')
    print("线程3监控"+AGSAnalyzeFolder3+'\\AnalysisReport')
    print("线程4监控"+AGSAnalyzeFolder4+'\\AnalysisReport')
    print("程序运行中")
    time.sleep(5)
    
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()