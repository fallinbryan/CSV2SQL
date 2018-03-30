# Converts a CSV file to Transactional SQL

import csv
import sys
import time
import datetime
import threading
import time

def withTwirly(f):
	def wrap(self):
		twirl = Twirly()
		twirl.start()
		f(self)
		twirl.stop()
	return wrap

class Twirly(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.runFlag = False
		self.startTime = 0
		self.stopTime = 0
		
		
	def run(self):
		self.startTime = time.clock()
		self.runFlag = True
		self.printTwirly()
		
	def stop(self):
		self.stopTime = time.clock()
		runTime = (self.stopTime - self.startTime) * 1000
		self.runFlag = False
		sys.stdout.write('...................Finished[%fms]\n'%runTime)
		sys.stdout.flush()
		
		
	def	printTwirly(self):
		while self.runFlag:
			sys.stdout.write('|')
			time.sleep(0.15)
			sys.stdout.write('\r/')
			time.sleep(0.15)
			sys.stdout.write('\r-')
			time.sleep(0.15)
			sys.stdout.write('\r\\')

class CSVConverter(object):
	#add more date format string to catch more date input variations
	dateFormatStrings = ['%m/%d/%Y','%Y-%m-%dT%H:%M:%S+00:00','%Y-%m-%d %H:%M']
	def __init__(self, filename,tableRoot,dropOption=None):
		if dropOption is not None:
			self.dropOption = dropOption
		else:
			self.dropOption = False
		self.tableRoot = tableRoot
		self.filename = filename
		self.tableName = filename.split('.')[0]
		self.csvtodictionaryList = []
		self.columnNames = []
		self.nameToType = {}
		
		print('Initializing vectors')
		self.initializeLists()
		
		
		self.sqlstring = ''
		self.maxStringLen = 0
		
		print('Normalizing Data')
		self.normalizeData()
		
		print('Converting Data to SQL')
		self.processCSV()
		
	@withTwirly
	def initializeLists(self):
		
		try:
			with open(self.filename,'r') as file:
				csvReader = csv.DictReader(file)
				for row in csvReader:
					self.csvtodictionaryList.append(row)
		
			
			for dict in self.csvtodictionaryList[0]:
				self.columnNames.append(dict)
		
		except FileNotFoundError:
			print('No such file found:',inputFileName)
			sys.exit(1)
			
		
			
	def processAllButLast(self,itr):
		itr = iter(itr)
		prev = itr.__next__()
		for item in itr:
			yield prev
			prev = item

	def isDateTime(self,d_str):
		rtrn = False
		for f in self.dateFormatStrings:
			try:
				date = datetime.datetime.strptime(d_str,f)
				rtrn = True
			except:
				pass
		return rtrn


	
	def getDateFormatString(self,d_string):
		for f in self.dateFormatStrings:
			try:
				date = datetime.datetime.strptime(d_string,f)
				rtrn = self.dateFormatStrings.index(f)
			except:
				pass
		return self.dateFormatStrings[rtrn]

	def normalizeDateTime(self,d_string):
		return str(datetime.datetime.strptime(d_string,self.getDateFormatString(d_string)))
		
	def extractDataType(self,i_str,maxLen):
		rtrn = ''
		#add more conditions to check for more types
		if i_str.isdigit():
			rtrn = 'INT'
		elif self.isDateTime(i_str):
			rtrn = 'DATETIME'
		else:
			rtrn = 'VARCHAR(%d)'% maxLen
			
		return rtrn	

	def getMax(self,listofDicts):
		max = 0
		for row in self.csvtodictionaryList:
			for name in self.columnNames:
				if max < len(row[name]):
					max = len(row[name])	
		return max+10

	@withTwirly
	def normalizeData(self):
			nameToType = self.nameToType
			var_names = []
			listOfDicts = self.csvtodictionaryList
			parallel_listOFTypes = []
			max = self.getMax(listOfDicts)
			for name in listOfDicts[0]:
				var_names.append(name)
				
			for name in var_names:
				nameToType[name] = self.extractDataType(listOfDicts[0][name],max)
			
			
			
			for dict in listOfDicts:
				for name in var_names:
					if nameToType[name] == 'DATETIME':
						try:
							dict[name] = self.normalizeDateTime(dict[name])
						except UnboundLocalError:
							print('Inconsitent Column Data')
							print('Defaulting Date to 12/31/2999')
							dict[name] = str(datetime.date.max)
					if nameToType[name] == 'INT':
						
						dict[name] = dict[name].replace(',','')
					elif 'VARCHAR' in nameToType[name]:
						dict[name] = dict[name].replace("'","''")
					else:
						pass
			
	def sanitize(self,i_string):
		r_string = i_string.replace(' ','_').replace('.','_').replace('-','_')
		r_string = r_string.replace('__','_')
		r_string = r_string.replace('___','_')
		r_string = r_string.replace('/','')
		r_string = r_string.replace('?','')
		r_string = r_string.replace('(','')
		r_string = r_string.replace(')','')
		r_string = r_string.replace('%','')
		if 'Name' in r_string:
			r_string = 'T_'+r_string
		return r_string

	@withTwirly
	def processCSV(self):
		if self.dropOption:
			self.sqlstring = 'DROP TABLE '+self.tableRoot+'.'+self.tableName+'\n'	
		self.sqlstring += 'CREATE TABLE '+self.tableRoot+'.'+self.tableName+'(\n'
		self.sqlstring += '\t'+self.tableName+'_ID uniqueidentifier PRIMARY KEY DEFAULT newid(),\n'

		for name in self.columnNames:
			self.sqlstring += '\t'+self.sanitize(name)+' '+self.nameToType[name]+' NOT NULL,\n'

		self.sqlstring += ')\nGO\n'

		for row in self.csvtodictionaryList:
			self.sqlstring += 'INSERT '+self.tableRoot+'.'+self.tableName+'(\n'
			for name in self.processAllButLast(self.columnNames):
				self.sqlstring += '\t'+self.sanitize(name)+',\n'

			self.sqlstring += '\t'+self.sanitize(self.columnNames[-1])+')\n VALUES (\n'
			
			
			for name in self.processAllButLast(self.columnNames):
				self.sqlstring += '\t\''+row[name]+'\',\n'
			
			self.sqlstring += '\t\''+row[self.columnNames[-1]].strip('\n')+'\'\n)\n'
	
	def getSql(self):
		return(self.sqlstring)
	
	
	def setDropTableDropOption(self,boolean):
		assert type(boolean) == bool
		self.dropOption = boolean

	
if __name__ == "__main__":	
	
	dropOption = False
	
	try:
		inputFileName = sys.argv[1]
		tableRoot = sys.argv[2]
	except IndexError:
		print('Invalid command ussage:  python csv2sql.py <filname.csv> <tableRoot> [-setDropOption]')
		sys.exit(1)
	try:
		dropOptionString = sys.argv[3]
		if dropOptionString != '-setDropOption':
			print('Invalid paramerter')
			sys.exit(1)
		
		dropOption = True
	except IndexError:
		pass

	print('CSV to SQL Conversion 1.0')
	print('Converting %s to Transact SQL'%inputFileName)
	startTime = time.clock()
		
	inputfileNameSplit = inputFileName.split('.')
	outputFileName = inputfileNameSplit[0] + '.sql'

	converter = CSVConverter(inputFileName,tableRoot,dropOption)
		
		
	outFile = open(outputFileName,'w')
	outFile.write(converter.getSql())
	outFile.close()
	stopTime = time.clock()
	timeDiff = (stopTime - startTime)*1000
	print('Process Complete in %fms. Saved SQL to %s' %(timeDiff,outputFileName))
	


