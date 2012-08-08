import MySQLdb
import os

class database:
	def __init__(self,host,port,usr,pwd,db=""):
		port = int(port)
		self.db = MySQLdb.connect(host = host, port = port, user = usr,passwd = pwd,db = db,compress = 1)
		
	def executeMany(self,query,args):
		try:		
			cursor = self.db.cursor()
			retVal = cursor.executemany(query,args)
			cursor.close()
			self.db.commit()
		except MySQLdb.Error,e:
			print 'Error:',e
			print query
			raise
			

	def executeInlot(self,query,args):
		r=self.executeMany(query,args)
		return r

	def execute_Non_Query(self,query):
		try:
			cursor = self.db.cursor()
			cursor.execute(query)
			cursor.close()
			self.insert_id=self.db.insert_id()
			self.db.commit()
			return 1
		except MySQLdb.Error,e:
			print 'Error in query: ',query
			print 'Message: ',e
			raise 

	
	def select_Query(self,query):
		res = []
		result = self.selectQuery(query)
		if result:
			res = [str(row[0]) for row in result]
		return res
	
	def selectQuery(self,query):
		try:
			res=()
			cursor = self.db.cursor()
			cursor.execute(query)
			res = cursor.fetchall()
			self.db.commit()
			return res
		except MySQLdb.Error, e:
			print "Error in Quer:",query
			print 'Message: ',e
			raise

