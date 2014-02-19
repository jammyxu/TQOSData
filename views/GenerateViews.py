# -*- coding: utf-8 -*-
import MySQLdb
import AnalysisLogWriter
import datetime
import time

# create views from tqos base data. 
#------------------------------------------------------------------

def ConnectDB(databaseName,LogPath):
	try:
		db= MySQLdb.connect('10.24.248.41','jimmyxu','TESTmy5%',databaseName)
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to connect DB")
		return None

	return db

#  
def GameFinishRateView(inputDate,databaseName,LogPath):
	db=ConnectDB(databaseName,LogPath)

	if db==None :
		return 

	#prepare a cursor object using cursor method
	cursor=db.cursor()	


	sql="select Count(*) from fifaCompletLog,fifaBeginLog where fifaBeginLog.matchID=fifaCompletLog.matchID and fifaBeginLog.qqid=fifaCompletLog.qqid and fifaCompletLog.gameEndType=0 and fifaBeginLog.recordTime like '" + inputDate+ "%'"	
	try:
		cursor.execute(sql)
		results=cursor.fetchone()
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to fetch data from database")	

	StorageArr=[0,0]
	StorageArr[0]=int(results[0])

	sql="select Count(*) from fifaBeginLog where  recordTime like '" + inputDate + "%'"
	try:
		cursor.execute(sql)
		results=cursor.fetchone()
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to fetch data from database")

	StorageArr[1]=int(results[0])

	#save data to view table
	sql="insert into  viewGameFinishRate(statdate, CompleteNum, BeginNum) values ("+inputDate+","+str(StorageArr[0]) +","+str(StorageArr[1]) +")"
	try:
		cursor.execute(sql)
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to insert data to view")

	db.commit()
	db.close()


#
def GameCompletionTypeView(inputDate,databaseName,LogPath):
	db=ConnectDB(databaseName,LogPath)

	if db==None :
		return 

	#prepare a cursor object using cursor method
	cursor=db.cursor()		
	
	#fetch game finish count by finish type	
	#	0 : Leaving the game because the game ended successfully.
	#	1 : Leaving the game due to a disconnect
	#	2 : Leaving the game because we quit
	#	3 : Leaving the game because we were idling too long
	#	4 : Leaving the game because we were trolling too much
	#	5 : Leaving the game because we got desynched
	sql="select gameEndType,Count(*) from fifaCompletLog where recordTime like '" + inputDate + "%' group by gameEndType"
	try:
		cursor.execute(sql)
		results=cursor.fetchall()
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to fetch data from database")

	EndTypeNum=[0,0,0,0,0,0]

	i=0
	for r in results:
		if i==int(r[0]):
			EndTypeNum[i]=int(r[1])	

		i=i+1


	#save data to view table
	sql="insert into  viewGameCompletionType(statdate, successNum, disconnectNum, quitNum, idlingNum, trollNum, desynchedNum) values ("+inputDate+","+str(EndTypeNum[0]) +","+str(EndTypeNum[1]) +","+str(EndTypeNum[2]) +","+str(EndTypeNum[3]) +","+str(EndTypeNum[4]) +","+str(EndTypeNum[5]) +")"
	try:
		cursor.execute(sql)
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to insert data to view")

	db.commit()
	db.close()


#构建比赛loading时长分布视图
def GameLoadingTime(inputDate,databaseName,LogPath):
	db=ConnectDB(databaseName,LogPath)

	if db==None :
		return 

	#prepare a cursor object using cursor method
	cursor=db.cursor()		

	sql="select loadingTime from fifaLoadingLog where recordTime like '"+ inputDate + "%'"
	try:
		cursor.execute(sql)
		results=cursor.fetchall()
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to fetch data from database")

	StorageArr=[0,0,0,0,0,0]


	for r in results:
		if int(r[0])<=3000:
			StorageArr[0]+=1
		elif int(r[0])>3000 and int(r[0])<=6000:
			StorageArr[1]+=1
		elif int(r[0])>6000 and int(r[0])<10000:
			StorageArr[2]+=1
		elif int(r[0])>10000 and int(r[0])<=20000:
			StorageArr[3]+=1
		elif int(r[0])>20000 and int(r[0])<=60000:
			StorageArr[4]+=1
		elif int(r[0])>60000:
			StorageArr[5]+=1
			
	#save data to view table
	sql="insert into  viewLoadingTime(statdate,Range1,Range2,Range3,Range4,Range5,Range6) values ("+inputDate+","+str(StorageArr[0]) +","+str(StorageArr[1]) +","+str(StorageArr[2]) +","+str(StorageArr[3]) +","+str(StorageArr[4]) +","+str(StorageArr[5]) +")"
	try:
		cursor.execute(sql)
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to insert data to view")

	db.commit()
	db.close()

#构建比赛loading后连接类型分布视图
def LoadingConnectType(inputDate,databaseName,LogPath):
	db=ConnectDB(databaseName,LogPath)

	if db==None :
		return 

	#prepare a cursor object using cursor method
	cursor=db.cursor()		

	sql="select connectType from fifaLoadingLog where recordTime like '"+ inputDate + "%'"
	try:
		cursor.execute(sql)
		results=cursor.fetchall()
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to fetch data from database")

	StorageArr=[0,0,0,0]
	for r in results:
		if int(r[0])==0:
			StorageArr[0]+=1
		elif int(r[0])==1:
			StorageArr[1]+=1
		elif int(r[0])==2:
			StorageArr[2]+=1
		elif int(r[0])==3:
			StorageArr[3]+=1

	#save data to view table
	sql="insert into  viewLoadingConnectType(statdate,Range1,Range2,Range3,Range4) values ("+inputDate+","+str(StorageArr[0]) +","+str(StorageArr[1]) +","+str(StorageArr[2]) +","+str(StorageArr[3]) +")"
	try:
		cursor.execute(sql)
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to insert data to view")

	db.commit()
	db.close()

#
def DailyCrash(inputDate,databaseName,LogPath):
	db=ConnectDB(databaseName,LogPath)

	if db==None :
		return 

	#prepare a cursor object using cursor method
	cursor=db.cursor()		

	#crash count
	sql="select count(*) from fifaCrashLog where recordTime like '"+ inputDate + "%'"
	try:
		cursor.execute(sql)
		results=cursor.fetchone()
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to fetch data from database") 

	StorageArr=[0,0]
	StorageArr[0]=int(results[0])

	#match begin count
	sql="select Count(*) from fifaBeginLog where  recordTime like '" + inputDate + "%'"
	try:
		cursor.execute(sql)
		results=cursor.fetchone()
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to fetch data from database")

	StorageArr[1]=int(results[0])

	#save data to view table
	sql="insert into  viewGameCrashRate(statdate,CrashNum,BeginNum) values ("+inputDate+","+str(StorageArr[0]) +","+str(StorageArr[1]) +")"
	try:
		cursor.execute(sql)
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to insert data to view")

	db.commit()
	db.close()


def TranElapseTime(inputDate,databaseName,LogPath):
	db=ConnectDB(databaseName,LogPath)

	if db==None :
		return 

	#prepare a cursor object using cursor method
	cursor=db.cursor()		

	sql="select TranTime from fifaTranElapseLog where recordTime like '"+ inputDate + "%'"
	try:
		cursor.execute(sql)
		results=cursor.fetchall()
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to fetch data from database")

	StorageArr=[0,0,0,0,0,0]


	for r in results:
		if int(r[0])<=3000:
			StorageArr[0]+=1
		elif int(r[0])>3000 and int(r[0])<=6000:
			StorageArr[1]+=1
		elif int(r[0])>6000 and int(r[0])<10000:
			StorageArr[2]+=1
		elif int(r[0])>10000 and int(r[0])<=20000:
			StorageArr[3]+=1
		elif int(r[0])>20000 and int(r[0])<=60000:
			StorageArr[4]+=1
		elif int(r[0])>60000:
			StorageArr[5]+=1
			
	#save data to view table
	sql="insert into  viewTranElapseTime(statdate,Range1,Range2,Range3,Range4,Range5,Range6) values ("+inputDate+","+str(StorageArr[0]) +","+str(StorageArr[1]) +","+str(StorageArr[2]) +","+str(StorageArr[3]) +","+str(StorageArr[4]) +","+str(StorageArr[5]) +")"
	try:
		cursor.execute(sql)
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to insert data to view")

	db.commit()
	db.close()	


def TranReason(inputDate,databaseName,LogPath):
	db=ConnectDB(databaseName,LogPath)

	if db==None :
		return 

	#prepare a cursor object using cursor method
	cursor=db.cursor()		

	sql="select TransitionReason from fifaTranReqLog where recordTime like '"+ inputDate + "%'"
	try:
		cursor.execute(sql)
		results=cursor.fetchall()
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to fetch data from database")

	StorageArr=[0,0,0]	

	for r in results:
		if int(r[0])==5:
			StorageArr[0]+=1
		elif int(r[0])==6:
			StorageArr[1]+=1
		elif int(r[0])==7:
			StorageArr[2]+=1

	#save data to view table
	sql="insert into viewTranReason(statdate,Range1,Range2,Range3) values ("+inputDate+","+str(StorageArr[0]) +","+str(StorageArr[1]) +","+str(StorageArr[2]) +")"
	try:
		cursor.execute(sql)
	except:
		AnalysisLogWriter.WriteLog(LogPath,"    Fail to insert data to view")

	db.commit()
	db.close()	

#run all stripts
def AutoRun(inputDate,databaseName,LogPath):
	
	# GameCompletionTypeView(inputDate,databaseName,LogPath)
	# GameLoadingTime(inputDate,databaseName,LogPath)
	# LoadingConnectType(inputDate,databaseName,LogPath)
	# DailyCrash(inputDate,databaseName,LogPath)
	# TranElapseTime(inputDate,databaseName,LogPath)	
	# TranReason(inputDate,databaseName,LogPath)

	inputdatestr=time.strptime(inputDate,"%Y%m%d")
	lastday=datetime.date(inputdatestr.tm_year,inputdatestr.tm_mon,inputdatestr.tm_mday)-datetime.timedelta(days=1)
	GameFinishRateView(lastday.strftime("%Y%m%d"),databaseName,LogPath)

if __name__ == '__main__':
	#from 20131201 to 20140211
	#AutoRun("20131202","FIFA_TQOS_19139","/data/FIFAOL3/TQOSData/views/outfile")
	timestart=time.strptime("20131216","%Y%m%d")
	timeend=time.strptime("20140211","%Y%m%d")
	startday=datetime.date(timestart.tm_year,timestart.tm_mon,timestart.tm_mday)
	endday=datetime.date(timeend.tm_year,timeend.tm_mon,timeend.tm_mday)
	currentday =startday
	while currentday<=endday:
		AutoRun(currentday.strftime("%Y%m%d"),"FIFA_TQOS_19139","/data/FIFAOL3/TQOSData/views/outfile")
		currentday=currentday+datetime.timedelta(days=1)
	

