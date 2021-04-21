import datetime
from sqlConnect import SQL
import os
import logging

def analysisTxt(path,dbFile):
    sqlite = SQL(dbFile)
    targetTxt = path

    analysisName = ''
    fileNo = ''
    acNo = ''
    is_pccard = False
    with open(targetTxt, 'r') as file_to_read:
        while True:
            lines = file_to_read.readline()
            if not lines:
                break
                pass
            lineData = lines.split()
            lastStr = lineData[len(lineData)-1]
            if lastStr.find('\\') >= 0 and lastStr.find(':') >= 0:
                for col in lineData:
                    if col.find('B-') >= 0:
                        acNo = col
                        break
                if lastStr.endswith('\\'):
                    analysisName = ''
                    is_pccard = True
                else:
                    analysisName = lastStr[lastStr.rfind('\\')+1:]
            elif lastStr != '0' and lastStr.isnumeric():
                fileNo = lastStr
                if is_pccard:
                    for col in lineData:
                        try:
                            datetime.datetime.strptime(col, '%d/%m/%Y')
                            date = datetime.datetime.strftime(datetime.datetime.strptime(col, '%d/%m/%Y'),'%Y%m%d')
                            findFileNo = sqlite.selectByFileNo(fileNo)
                            if len(findFileNo) == 0:
                                sqlite.insert('PCcard',acNo,date,'','1','','','',fileNo)
                            break
                        except:
                            pass
                else:
                    acType = ''
                    if analysisName.find('.wgl') >= 0:
                        acType = 'WQAR'
                    elif analysisName.find('.gz') >= 0:
                        acType = 'A350'
                    else:
                        acType = 'B787'
                    findAnalysisName = sqlite.selectByAnalysisName(analysisName)
                    if len(findAnalysisName) == 0:
                        findFileNo = sqlite.selectByFileNo(fileNo)
                        if len(findFileNo) == 0:
                            sqlite.insert(acType,acNo,'','','2','','',analysisName,fileNo)
                        else:
                            pass
                    else:
                        findFileNo = sqlite.selectByFileNo(fileNo)
                        if len(findFileNo) == 0:
                            status = 2
                            if len(findAnalysisName[0][7]) > 0:
                                status = status + 1
                                fileNo = findAnalysisName[0][7][findAnalysisName[0][7].rfind('-')+1:findAnalysisName[0][7].rfind('.')]
                            sqlite.updateById(findAnalysisName[0][0],findAnalysisName[0][1],findAnalysisName[0][2],findAnalysisName[0][3],findAnalysisName[0][4],str(status),findAnalysisName[0][6],findAnalysisName[0][7],findAnalysisName[0][8],fileNo)
                        else:
                            if findAnalysisName[0][0] != findFileNo[0][0]:
                                status = 0
                                if len(findAnalysisName[0][7]) == 0:
                                    if len(findFileNo[0][7]) > 0:
                                        status = status + 1
                                    if len(findAnalysisName[0][8]) > 0:
                                        status = status + 1
                                    if len(fileNo) > 0:
                                        status = status + 1
                                    sqlite.updateById(findAnalysisName[0][0],findAnalysisName[0][1],findAnalysisName[0][2],findAnalysisName[0][3],findAnalysisName[0][4],str(status),findAnalysisName[0][6],findFileNo[0][7],findAnalysisName[0][8],fileNo)
                                    sqlite.deleteById(findFileNo[0][0])
                                else:
                                    if findAnalysisName[0][7] != findFileNo[0][7]:
                                        sqlite.updateById(findFileNo[0][0],findAnalysisName[0][1],findAnalysisName[0][2],findAnalysisName[0][3],findAnalysisName[0][4],'3',findAnalysisName[0][6],findFileNo[0][7],findAnalysisName[0][8],findFileNo[0][9])
                                    else:
                                        sqlite.deleteById(findFileNo[0][0])

def readFileNo(folderPath,dbFile):
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename='record.log', level=logging.DEBUG, format=LOG_FORMAT)
    print('解析AnalysisReport')
    logging.info('解析AnalysisReport')
    try:
        folderPath = folderPath+'\\AnalysisReport'
        sqlite = SQL(dbFile)
        timesStr = sqlite.selectTimeTable()[0][1]
        timesStr = timesStr + '000'
        folders = os.listdir(folderPath)
        for folder in folders:
            if folder > timesStr:
                print('读取:'+folder)
                analysisTxt(folderPath+'\\'+folder+'\\ANAREP.TXT',dbFile)
    except Exception as e:
        logging.warn(str(e))
    print('AnalysisReport解析完毕')
    logging.info('AnalysisReport解析完毕')


