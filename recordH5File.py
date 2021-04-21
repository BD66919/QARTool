import os
import time
import logging
from sqlConnect import SQL

def recordH5Name(targetPath,dbFile):#新增h5时没有对应的原始数据就增加一条，有对应数据的话就匹配上去

    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename = 'record.log', level = logging.DEBUG, format = LOG_FORMAT)
    print(targetPath+"开始记录hdf5名")
    logging.info(targetPath+"开始记录hdf5名")
    try:
        filePath = os.listdir(targetPath)
        sql = SQL(dbFile)
        startTime = sql.selectTimeTable()[0][1]
        for file in filePath:
            file = targetPath+'\\'+file
            ctime = os.path.getctime(file)
            timeStructc = time.localtime(ctime)
            ctime = time.strftime('%Y%m%d%H%M%S',timeStructc)
            if(ctime>startTime):
                
                fileACNo = file[file.find('B-'):file.find('B-')+6]
                fileH5Name = file[file.rfind('\\')+1:]
                fileDate = fileH5Name[0:8]
                fileNo = file[file.rfind('-')+1:file.rfind('.')]
                print(fileNo)
                result = sql.selectByFileNo(fileNo)
                if(len(result) == 0):
                    sql.insert('',fileACNo,fileDate,'','2','',fileH5Name,'',fileNo)
                else:
                    if(len(result[0][8]) >=1):
                        sql.updateById(result[0][0],result[0][1],result[0][2],result[0][3],result[0][4],'3',result[0][6],fileH5Name,result[0][8],result[0][9])
                    else:
                        sql.updateById(result[0][0],result[0][1],result[0][2],result[0][3],result[0][4],'2',result[0][6],fileH5Name,result[0][8],result[0][9])
    
    except Exception as e:
        logging.warning(str(e))
    print(targetPath+"记录hdf5名结束")
    logging.info(targetPath+"记录hdf5名结束")

