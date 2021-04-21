from unrar import rarfile
import zipfile
import os
import shutil
import datetime
from pathlib import Path
import logging
import configparser
from sqlConnect import SQL

config = configparser.ConfigParser()
config.read(os.getcwd()+'\\config.ini')
subFolder = 0
folders = [config.get('path','transPath1'),config.get('path','transPath2'),config.get('path','transPath3'),config.get('path','transPath4'),config.get('path','transPathIncorrectFile'),config.get('path','transPathOutOfDateRange')]
temporary = os.getcwd()+'\\rarTemporary'
dbFile = config.get('path','dbPath')
sqlite =SQL(dbFile)

def dateContain(inputBegin,inputEnd,beginDate,endDate):
    beginDate = datetime.datetime.strftime(datetime.datetime.strptime(beginDate, '%Y%m%d'),'%Y-%m-%d')
    endDate = datetime.datetime.strftime(datetime.datetime.strptime(endDate, '%Y%m%d'),'%Y-%m-%d')
    if (inputBegin >= beginDate and inputBegin <= endDate) or (beginDate >= inputBegin and beginDate <= inputEnd):
        return True
    else:
        return False

def func787(beginDate,endDate):
    print('开始处理B787文件')
    logging.info('开始处理B787文件')
    global subFolder
    global folders
    path787 = os.getcwd()+'\\B787'
    folder787s = os.listdir(path787)
    for folder787 in folder787s:
        folderBeginDate = folder787[5:13]
        folderEndDate = folder787[14:22]
        if dateContain(beginDate,endDate,folderBeginDate,folderEndDate):
            files = os.listdir(path787+'\\'+folder787)
            for file in files:
                targetPath = Path()
                if file.find('.raw') >= 0:
                    fileDate = file[file.rfind('-')-8:file.rfind('-')]
                    fileDate = fileDate[0:4]+'-'+fileDate[4:6]+'-'+fileDate[6:8]
                    if fileDate < beginDate or fileDate > endDate:
                        print(file+'文件不在选择的日期范围内！')
                        logging.warning(file+'文件不在选择的日期范围内！')
                        targetPath = Path(folders[5])
                    else:
                        targetPath = Path(folders[subFolder])
                        subFolder = (subFolder+1)%4
                        localPath = 'B787\\'+folder787+'\\'+file
                        analysisName = file
                        analysisNameDate = fileDate
                        acNo = file[0:6]
                        findAnalysisName = sqlite.selectByAnalysisName(analysisName)
                        if len(findAnalysisName) == 0:
                            sqlite.insert('B787',acNo,analysisNameDate,'','1',localPath,'',analysisName,'')
                elif file.find('.zip') >= 0:
                    fileDate = file[file.rfind('_')+1:file.rfind('_')+9]
                    fileDate = fileDate[0:4]+'-'+fileDate[4:6]+'-'+fileDate[6:8]
                    if fileDate < beginDate or fileDate > endDate:
                        print(file+'文件不在选择的日期范围内！')
                        logging.warning(file+'文件不在选择的日期范围内！')
                        targetPath = Path(folders[5])
                    else:
                        targetPath = Path(folders[subFolder])
                        subFolder = (subFolder+1)%4
                        localPath = 'B787\\'+folder787+'\\'+file
                        analysisName = ''
                        analysisNameDate = file[file.rfind('_')+1:file.rfind('_')+15]
                        acNo = 'B-'+file[4:8]
                        zipfile787 = zipfile.ZipFile(path787+'\\'+folder787+'\\'+file,'r')
                        namelists787 = zipfile787.namelist()
                        for namelist in namelists787:
                            if namelist.find('.zip') >= 0 or namelist.find('.raw') >= 0 or namelist.find('-CPL') >= 0:
                                analysisName = namelist
                                break
                        findAnalysisName = sqlite.selectByAnalysisName(analysisName)
                        if len(findAnalysisName) == 0:
                            sqlite.insert('B787',acNo,analysisNameDate[0:8],analysisNameDate,'1',localPath,'',analysisName,'')
                        zipfile787.close()
                else:
                    print(file+'文件格式不正确！')
                    logging.warning(file+'文件格式不正确！')
                    targetPath = Path(folders[4])
                if targetPath.exists() == False:
                    os.makedirs(targetPath)
                shutil.move(path787+'\\'+folder787+'\\'+file,str(targetPath)+'\\'+file)
    print('B787文件处理完毕')           
    logging.info('B787文件处理完毕')

def recursion350(beginDate,endDate,path):
    global subFolder
    global folders
    p = Path(path)
    if p.is_dir():
        pchilds = os.listdir(path)
        for pchild in pchilds:
            recursion350(beginDate,endDate,path+'\\'+pchild)
    else:
        targetPath = Path()
        if p.suffix == '.gz':
            fileDate = '20'+p.name[11:13]+'-'+p.name[13:15]+'-'+p.name[15:17]
            if fileDate < beginDate or fileDate > endDate:
                print(p.name+'文件不在选择的日期范围内！')
                logging.warning(p.name+'文件不在选择的日期范围内！')
                targetPath = Path(folders[5])
            else:
                targetPath = Path(folders[subFolder])
                subFolder = (subFolder+1)%4
                localPath = path[path.find('A350'):]
                analysisName = path[path.rfind('\\')+1:]
                analysisNameDate = '20'+analysisName[11:23]
                disk = path[0:path.find(':')+1]
                dirpath = path[0:path.rfind('\\')]
                targetdir = 'A350Tem\\'
                cmd = disk+' && cd '+dirpath+' && winrar x '+analysisName+' '+targetdir
                os.system(cmd)
                subFolder1 = os.listdir(dirpath+'\\'+targetdir)[0]
                acNo = os.listdir(dirpath+'\\'+targetdir+'\\'+subFolder1)[0][1:]
                shutil.rmtree(dirpath+'\\'+targetdir)
                findAnalysisName = sqlite.selectByAnalysisName(analysisName)
                if len(findAnalysisName) == 0:
                    sqlite.insert('A350',acNo,analysisNameDate[0:8],analysisNameDate,'1',localPath,'',analysisName,'')
        else:
            print(p.name+'文件格式不正确！')
            logging.warning(p.name+'文件格式不正确！')
            targetPath = Path(folders[4])
        if targetPath.exists() == False:
            os.makedirs(targetPath)
        shutil.move(path,str(targetPath)+'\\'+p.name)

def func350(beginDate,endDate):
    print('开始处理A350文件')
    logging.info('开始处理A350文件')
    path350 = ''
    allFiles = os.listdir(os.getcwd())
    for allFile in allFiles:
        if allFile[0:4] == 'A350':
            path350 = os.getcwd()+'\\'+allFile
            recursion350(beginDate,endDate,path350)
    print('A350文件处理完毕')
    logging.info('A350文件处理完毕')

def funcWQAR(beginDate,endDate):
    print('开始处理WQAR及PC卡文件')
    print('解压缩到临时文件夹')
    logging.info('开始处理WQAR及PC卡文件')
    logging.info('解压缩到临时文件夹')
    global subFolder
    global folders
    allFolders = os.listdir(os.getcwd())
    for allFolder in allFolders:
        if allFolder[0:3] == 'QAR':
            pathWQAR = os.getcwd()+'\\'+allFolder
            folderBeginDate = allFolder[8:16]
            folderEndDate = ''
            if allFolder[12:16] > allFolder[17:21]:
                folderEndDate = str(int(allFolder[8:12])+1)+allFolder[17:21]
            else:
                folderEndDate = allFolder[8:12]+allFolder[17:21]
            if allFolder.find('.rar') >= 0:
                if dateContain(beginDate,endDate,folderBeginDate,folderEndDate) == False:
                    os.remove(pathWQAR)
                    continue
                rar = rarfile.RarFile(pathWQAR)
                level1Len = len(allFolder)
                for rarinfo in rar.infolist():
                    filename = rarinfo.filename
                    if filename.endswith('.rar'):
                        print ('读取'+filename)
                        fileDate = filename[level1Len+2:level1Len+10]
                        if dateContain(beginDate,endDate,fileDate,fileDate):
                            rar.extract(rarinfo,temporary)
                            acNo = 'B-'+filename[level1Len-3:level1Len+1]
                            localPath = allFolder+'\\'+filename
                            checkClass = sqlite.selectByCheck('PCcard',acNo,fileDate,'')
                            logging.info(localPath+'写入数据库')
                            if len(checkClass) == 0:
                                sqlite.insert('PCcard',acNo,fileDate,'','1',localPath,'','','')
                            else:
                                sqlite.updateById(checkClass[0][0],checkClass[0][1],checkClass[0][2],checkClass[0][3],checkClass[0][4],checkClass[0][5],localPath,checkClass[0][7],checkClass[0][8],checkClass[0][9])
                    elif filename.find('.wgl') >= 0:
                        print ('读取'+filename)
                        fileDate = filename[level1Len+9:level1Len+17]
                        if dateContain(beginDate,endDate,fileDate,fileDate):
                            rar.extract(rarinfo,temporary)
                            if filename.endswith('.wgl'):
                                acNo = 'B-'+filename[level1Len-3:level1Len+1]
                                localPath = allFolder+'\\'+filename
                                analysisNameDate = filename[level1Len+9:level1Len+23]
                                analysisName = filename[filename.rfind('\\')+1:]
                                checkClass = sqlite.selectByCheck('WQAR',acNo,analysisNameDate[0:8],analysisNameDate)
                                logging.info(localPath+'写入数据库')
                                if len(checkClass) == 0:
                                    sqlite.insert('WQAR',acNo,analysisNameDate[0:8],analysisNameDate,'1',localPath,'',analysisName,'')
                                else:
                                    sqlite.updateById(checkClass[0][0],checkClass[0][1],checkClass[0][2],checkClass[0][3],checkClass[0][4],checkClass[0][5],localPath,checkClass[0][7],checkClass[0][8],checkClass[0][9])
                movefolder = temporary+'\\'+allFolder[0:allFolder.find('.')]
                movedirs = os.listdir(movefolder)
                for movedir in movedirs:
                    shutil.move(movefolder+'\\'+movedir,temporary+'\\'+movedir)
                shutil.rmtree(movefolder)
            else:
                if dateContain(beginDate,endDate,folderBeginDate,folderEndDate) == False:
                    shutil.rmtree(pathWQAR)
                    continue
                rarFiles = os.listdir(pathWQAR)
                for rarFile in rarFiles:
                    if rarFile.find('.rar') >= 0:
                        if len(rarFile) != 8:
                            try:
                                os.makedirs(folders[4])
                            except:
                                pass
                            shutil.move(pathWQAR+'\\'+rarFile,folders[4]+'\\'+rarFile)
                            continue
                        try:
                            print (rarFile+'解压中...')
                            rar = rarfile.RarFile(pathWQAR+'\\'+rarFile)
                            for rarinfo in rar.infolist():
                                filename = rarinfo.filename
                                rarfilepath = Path(filename)
                                if rarfilepath.suffix == '.rar':
                                    print ('读取'+filename)
                                    fileDate = filename[5:13]
                                    if dateContain(beginDate,endDate,fileDate,fileDate):
                                        rar.extract(rarinfo,temporary)
                                        acNo = 'B-'+rarFile[0:rarFile.find('.')]
                                        localPath = allFolder+'\\'+rarFile+'\\'+filename
                                        checkClass = sqlite.selectByCheck('PCcard',acNo,fileDate,'')
                                        logging.info(localPath+'写入数据库')
                                        if len(checkClass) == 0:
                                            sqlite.insert('PCcard',acNo,fileDate,'','1',localPath,'','','')
                                        else:
                                            sqlite.updateById(checkClass[0][0],checkClass[0][1],checkClass[0][2],checkClass[0][3],checkClass[0][4],checkClass[0][5],localPath,checkClass[0][7],checkClass[0][8],checkClass[0][9])
                                elif rarfilepath.suffix == '.wgl':
                                    print ('读取'+filename)
                                    fileDate = filename[12:20]
                                    if dateContain(beginDate,endDate,fileDate,fileDate):
                                        rar.extract(rarinfo,temporary)
                                        acNo = 'B-'+rarFile[0:rarFile.find('.')]
                                        localPath = allFolder+'\\'+rarFile+'\\'+filename
                                        analysisNameDate = filename[12:26]
                                        analysisName = filename[filename.rfind('\\')+1:]
                                        checkClass = sqlite.selectByCheck('WQAR',acNo,analysisNameDate[0:8],analysisNameDate)
                                        logging.info(localPath+'写入数据库')
                                        if len(checkClass) == 0:
                                            sqlite.insert('WQAR',acNo,analysisNameDate[0:8],analysisNameDate,'1',localPath,'',analysisName,'')
                                        else:
                                            sqlite.updateById(checkClass[0][0],checkClass[0][1],checkClass[0][2],checkClass[0][3],checkClass[0][4],checkClass[0][5],localPath,checkClass[0][7],checkClass[0][8],checkClass[0][9])
                                elif filename.find('.wgl') >= 0:
                                    fileDate = filename[12:20]
                                    if dateContain(beginDate,endDate,fileDate,fileDate):
                                        rar.extract(rarinfo,temporary)
                        except:
                            print(pathWQAR+'\\'+rarFile+"文件已损坏")
                            log.warning(pathWQAR+'\\'+rarFile+"文件已损坏")
                    elif len(rarFile) == 4:
                        try:
                            os.makedirs(temporary+'\\'+rarFile)
                        except:
                            pass
                        rarFileSubFiles = os.listdir(pathWQAR+'\\'+rarFile)
                        for rarFileSubFile in rarFileSubFiles:
                            if rarFileSubFile.find('.rar') >= 0:
                                fileDate = rarFileSubFile[0:8]
                                if dateContain(beginDate,endDate,fileDate,fileDate):
                                    acNo = 'B-'+rarFile
                                    localPath = allFolder+'\\'+rarFile+'\\'+rarFileSubFile
                                    checkClass = sqlite.selectByCheck('PCcard',acNo,fileDate,'')
                                    logging.info(localPath+'写入数据库')
                                    if len(checkClass) == 0:
                                        sqlite.insert('PCcard',acNo,fileDate,'','1',localPath,'','','')
                                    else:
                                        sqlite.updateById(checkClass[0][0],checkClass[0][1],checkClass[0][2],checkClass[0][3],checkClass[0][4],checkClass[0][5],localPath,checkClass[0][7],checkClass[0][8],checkClass[0][9])
                            elif rarFileSubFile.find('.wgl') >= 0:
                                fileDate = rarFileSubFile[7:15]
                                if dateContain(beginDate,endDate,fileDate,fileDate):
                                    acNo = 'B-'+rarFile
                                    localPath = allFolder+'\\'+rarFile+'\\'+rarFileSubFile
                                    analysisNameDate = rarFileSubFile[7:21]
                                    analysisName = rarFileSubFile
                                    checkClass = sqlite.selectByCheck('WQAR',acNo,analysisNameDate[0:8],analysisNameDate)
                                    logging.info(localPath+'写入数据库')
                                    if len(checkClass) == 0:
                                        sqlite.insert('WQAR',acNo,analysisNameDate[0:8],analysisNameDate,'1',localPath,'',analysisName,'')
                                    else:
                                        sqlite.updateById(checkClass[0][0],checkClass[0][1],checkClass[0][2],checkClass[0][3],checkClass[0][4],checkClass[0][5],localPath,checkClass[0][7],checkClass[0][8],checkClass[0][9])
                            existFile = Path(temporary+'\\'+rarFile+'\\'+rarFileSubFile)
                            if existFile.exists():
                                if existFile.is_dir():
                                    shutil.rmtree(pathWQAR+'\\'+rarFile+'\\'+rarFileSubFile)
                                else:
                                    os.remove(pathWQAR+'\\'+rarFile+'\\'+rarFileSubFile)
                            else:
                                shutil.move(pathWQAR+'\\'+rarFile+'\\'+rarFileSubFile,temporary+'\\'+rarFile+'\\'+rarFileSubFile)        
    print('解压缩完毕')
    print('进行分类中')
    logging.info('解压缩完毕')
    logging.info('进行分类中')
    temporaryFolders = os.listdir(temporary)
    for temporaryFolder in temporaryFolders:
        temporaryFiles = os.listdir(temporary+'\\'+temporaryFolder)
        for temporaryFile in temporaryFiles:
            file = Path(temporary+'\\'+temporaryFolder+'\\'+temporaryFile)
            targetPath = Path()
            if file.suffix == '.wgl':
                fileDate = temporaryFile[7:11]+'-'+temporaryFile[11:13]+'-'+temporaryFile[13:15]
                if fileDate < beginDate or fileDate > endDate:
                    print(temporaryFile+'文件不在所选日期范围内！')
                    logging.warning(temporaryFile+'文件不在所选日期范围内！')
                    targetPath = Path(folders[5])
                else:
                    targetPath = Path(folders[subFolder])
                    subFolder = (subFolder+1)%4
            elif file.suffix == '.rar':
                fileDate = temporaryFile[0:4]+'-'+temporaryFile[4:6]+'-'+temporaryFile[6:8]
                if fileDate < beginDate or fileDate > endDate:
                    print(temporaryFile+'文件不在所选日期范围内！')
                    logging.warning(temporaryFile+'文件不在所选日期范围内！')
                    targetPath = Path(folders[5])
                else:
                    targetPath = Path(folders[subFolder])
                    subFolder = (subFolder+1)%4
            else:
                print(temporaryFile+'文件格式不正确！')
                logging.warning(temporaryFile+'文件格式不正确！')
                targetPath = Path(folders[4])
            if targetPath.exists() == False:
                os.makedirs(targetPath)
            targetFile = Path(str(targetPath)+'\\'+temporaryFile)
            if targetFile.exists():
                if targetFile.is_dir():
                    shutil.rmtree(temporary+'\\'+temporaryFolder+'\\'+temporaryFile)
                else:
                    os.remove(temporary+'\\'+temporaryFolder+'\\'+temporaryFile)
            else:
                shutil.move(temporary+'\\'+temporaryFolder+'\\'+temporaryFile,str(targetPath)+'\\'+temporaryFile)
    print('分类完毕')
    print('WQAR及PC卡文件处理完毕')
    logging.info('分类完毕')
    logging.info('WQAR及PC卡文件处理完毕')
    
def unrarByMonth(beginDate,endDate):
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename='record.log', level=logging.DEBUG, format=LOG_FORMAT)

    print('创建解压缩临时文件夹')
    logging.info('创建解压缩临时文件夹')
    try:
        os.makedirs(temporary)
    except:
        pass
    print('解压缩临时文件夹创建成功')
    logging.info('解压缩临时文件夹创建成功')

    pathOutOfDate = Path(folders[4])
    pathIncorect = Path(folders[5])
    if pathOutOfDate.exists():
        shutil.rmtree(pathOutOfDate)
    if pathIncorect.exists():
        shutil.rmtree(pathIncorect)
    
    func787(beginDate,endDate)
    func350(beginDate,endDate)
    funcWQAR(beginDate,endDate)
    print('删除解压缩临时文件夹')
    logging.info('删除解压缩临时文件夹')
    temporaryFolders = os.listdir(temporary)
    for temporaryFolder in temporaryFolders:
        os.rmdir(temporary+'\\'+temporaryFolder)
    os.rmdir(temporary)
    print('解压缩临时文件夹删除成功')
    logging.info('解压缩临时文件夹删除成功')

