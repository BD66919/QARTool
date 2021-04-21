import sqlite3
import configparser

class SQL:
    sql = sqlite3
    dbFile = ''

    def __init__(self,dbFile):
        self.dbFile = dbFile
    
    def create(self):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        try:
            cursor.execute('create table path(id INTEGER primary key AUTOINCREMENT,type varchar(20),ACNo varchar(20),date varchar(20),dateTime varchar(20),status varchar(20),localPath varchar(300),h5Name varchar(100),analysisName varchar(100),fileNo varchar(100))')
        except:
            return
        cursor.close()
        conn.close()
        
    def createTimeTable(self):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        try:
            cursor.executescript('''
                create table times(id INTEGER primary key AUTOINCREMENT,time varchar(40));
                insert into times values (null,'');
             ''')          
        except:
            return
        
        cursor.close()
        conn.close()
        
    def selectTimeTable(self):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('select * from times')
        values = cursor.fetchall()
        cursor.close()
        conn.close()
        return values

    def updateTimeById(self,id,time):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('update times set time = ? where id = ?',(time,id,))
        cursor.close()
        conn.commit()
        conn.close()

    def selectById(self,id):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('select * from path where id  = ?',(id,))
        values = cursor.fetchall()
        cursor.close()
        conn.close()
        return values

    def insert(self,type,ACNo,date,dateTime,status,localPath,h5Name,analysisName,fileNo):
        self.create()
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('insert into path values (?,?,?,?,?,?,?,?,?,?)', (None,type,ACNo,date,dateTime,status,localPath,h5Name,analysisName,fileNo))
        cursor.close()
        conn.commit()
        conn.close()

    def updateById(self,id,type,ACNo,date,dateTime,status,localPath,h5Name,analysisName,fileNo):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('update path set type = ? , ACNo = ? ,date = ? , dateTime = ? , status = ? , localPath = ? ,h5Name = ? , analysisName = ? , fileNo = ? where id = ?',(type,ACNo,date,dateTime,status,localPath,h5Name,analysisName,fileNo,id,))
        cursor.close()
        conn.commit()
        conn.close()

    def selectByAnalysisName(self,analysisName):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('select * from path where analysisName  = ?',(analysisName,))
        values = cursor.fetchall()
        cursor.close()
        conn.close()
        return values

    def selectByFileNo(self,fileNo):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('select * from path where fileNo  = ?',(fileNo,))
        values = cursor.fetchall()
        cursor.close()
        conn.close()
        return values
    
    def selectByH5Name(self,h5Name):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('select * from path where h5Name  = ?',(h5Name,))
        values = cursor.fetchall()
        cursor.close()
        conn.close()
        return values

    def selectByCheck(self,type,acNo,date,dateTime):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('select * from path where type  = ? and acNo  = ? and date  = ? and dateTime  = ?',(type,acNo,date,dateTime,))
        values = cursor.fetchall()
        cursor.close()
        conn.close()
        return values
    
    def deleteById(self,id):
        conn = self.sql.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('delete from path where id = ?',(id,))
        cursor.close()
        conn.commit()
        conn.close()


