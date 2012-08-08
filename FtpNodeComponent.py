import getopt,sys,time
import os
from database import *
import cPickle,gzip,hashlib
from NodeComponent import NodeComponent
from gridftp.CommandHandler import CommandHandler
from ftplib import FTP

class FtpNodeComponent(NodeComponent):
    ftpRetries = 3
    ftpSleepTime = 10
    _capTablesToCopy = []                
    _fullAstTablesToCopy = []
    _debugAstTablesToCopy = []
    _fullCapTablesToCopy = []
    _debugCapTablesToCopy = []
    triggerQuery = ''
    uploadFileExtension = '.gru'
    downloadFileExtension = '.grd'
    
    def __init__(self,localCapDb,localAstDb,nodeServerDetails,settings):
        NodeComponent.__init__(self, settings)
        self.localCapDb = localCapDb
        self.localAstDb = localAstDb
        self.nodeServerDetails = nodeServerDetails
        self.ftpClient = None
    
    def _uploadFile(self,fileName):
        retriesLeft = self.ftpRetries
        while(retriesLeft):
            try:
                self.ftpClient = None
                ip,port,usr,pwd = self.nodeServerDetails
                self.ftpClient = FTP()
                self.ftpClient.connect(ip, port)
                self.ftpClient.login(usr,pwd)
                f = open(fileName, 'rb')
                self.ftpClient.storbinary('STOR ' + fileName, f)
                f.close()
                self.ftpClient.close()
                retriesLeft = 0
            except:
                retriesLeft = retriesLeft - 1
                time.sleep(self.ftpSleepTime)
                if not retriesLeft:
                    raise
        
    
    def _downloadFile(self,fileName):
        retriesLeft = self.ftpRetries
        while(retriesLeft):
            try:
                self.ftpClient = None
                ip,port,usr,pwd = self.nodeServerDetails
                self.ftpClient = FTP()
                self.ftpClient.connect(ip, port)
                self.ftpClient.login(usr,pwd)
                f = open(fileName, 'wb')
                self.ftpClient.retrbinary('RETR ' + fileName,f.write)
                f.close()
                self.ftpClient.close()
                retriesLeft = 0
            except:
                retriesLeft = retriesLeft - 1
                print 'Exception in ftp,retries left : ',retriesLeft
                time.sleep(self.ftpSleepTime)
                if not retriesLeft:
                    raise
    
    def _getQueries(self,table):
        selectQuery,deleteQuery,insertQuery = None,None,None
        tableName = table
        if type(table) is tuple:
            tableName = table[0]
            for query in table[1:]:
                if query.startswith('select') or query.startswith('update'):
                    selectQuery = query
                elif query.startswith('delete'):
                    deleteQuery = query
                elif query.startswith('insert'):
                    insertQuery = query
                elif query.startswith('ignore'):
                    deleteQuery = ''
        if selectQuery is None:
            selectQuery = 'select * from '+tableName+' where documentid in %d'
        if insertQuery is None:
            insertQuery = 'insert into '+tableName+' values %v'
        if deleteQuery is None:
            deleteQuery = 'delete from '+tableName+' where documentid in %d'
        return tableName,selectQuery,deleteQuery,insertQuery
    
    def _doNodeCleanUp(self,documents,tablesToCopy):
        revDocList = tablesToCopy[:]
        revDocList.reverse()
        delQueries = []
        for table in revDocList:
            deleteQuery = self._getQueries(table)[2]
            if deleteQuery:
                delQueries.append(deleteQuery)
        
        if delQueries:
            packet = ['DOCSNONSELECTQUERIES',documents,delQueries]
            upFileName,downFileName = self._getUniqueFileNames(documents,delQueries)
            self._sendPacket(packet, upFileName)

    def _getUniqueFileNames(self,documents,tableNames):
        sortedDocs = documents[:]
        sortedTables = tableNames[:]
        sortedDocs.sort()
        sortedTables.sort()
        sortedDocs.extend(sortedTables)
        textToHash = ','.join(sortedDocs)
        fileName = hashlib.md5(textToHash).hexdigest()
        return fileName+self.uploadFileExtension,fileName+self.downloadFileExtension
    
    def _copyDataToNode(self,documents,srcDb,tablesToCopy):
        insertQueriesData = []
        tableNames = []
        delQueries = []
        docString = '('+','.join(documents)+')'
        for table in tablesToCopy:
            tableName,selectQuery,deleteQuery,insertQuery = self._getQueries(table)
            selectQuery = selectQuery.replace('%d',docString)
            #get data
            data = srcDb.selectQuery(selectQuery)
            if data:
                tableNames.append(tableName)
                delQueries.append(deleteQuery)
                insertQueriesData.append((insertQuery,data))
        
        delQueries.reverse()
        packet = ['COPYDOCDATAANDTRIGGER',documents,delQueries,insertQueriesData,self._triggerQueries]
        upFileName,downFileName = self._getUniqueFileNames(documents,tableNames)
        self._sendPacket(packet, upFileName)
    
    def _copyDataFromNode(self,documents,destDb,tablesToCopy): 
        deleteQueries,insertQueries,tableData = self._getDataFromNode(documents,tablesToCopy)
        docString = '(' + ','.join(documents)+')'
        deleteQueries.reverse()
        for tableName,deleteQuery in deleteQueries:
            if deleteQuery:
                deleteQuery = deleteQuery.replace('%d',docString)
                destDb.execute_Non_Query(deleteQuery)
            
        for tableName,insertQuery in insertQueries:
            data = tableData[tableName]
            if data:
                insertQuery = insertQuery.replace('%d',docString)
                columnCount = len(data[0])
                vString = '('+'%s,'*(columnCount - 1)+'%s)'
                insertQuery = insertQuery.replace('%v',vString)
                destDb.executeInlot(insertQuery,data)

    def _getDataFromNode(self,documents,tablesToCopy):
        tableNames = []
        tableSelectQueries = []
        tableDeleteQueries = []
        tableInsertQueries = []
        for table in tablesToCopy:
            tableName,selectQuery,deleteQuery,insertQuery = self._getQueries(table)
            tableSelectQueries.append([tableName,selectQuery])
            tableDeleteQueries.append([tableName,deleteQuery])
            tableInsertQueries.append([tableName,insertQuery])
            tableNames.append(tableName)

        upFileName,downFileName =  self._getUniqueFileNames(documents,tableNames)
        packet = ['GETDOCDATA',documents,tableSelectQueries,downFileName]
        
        dataObj = self._sendPacketAndGetReply(packet, upFileName, downFileName)
        
        return tableDeleteQueries,tableInsertQueries,dataObj
    
    def _executeSelectOnNode(self,selectQuery):
        upFileName,downFileName =  self._getUniqueFileNames([],[selectQuery])
        packet = ['GENERICSELECTQUERY',selectQuery,downFileName]
        return self._sendPacketAndGetReply(packet, upFileName, downFileName)
    
    def _executeDocSelectOnNode(self,documents,selectQuery):
        upFileName,downFileName =  self._getUniqueFileNames(documents,[selectQuery])
        packet = ['DOCSELECTQUERY',documents,selectQuery,downFileName]
        return self._sendPacketAndGetReply(packet, upFileName, downFileName)
    
    def _sendPacket(self,packet,upFileName):
        ufp = gzip.open(upFileName,'wb')
        cPickle.dump(packet,ufp)
        ufp.close()
        self._uploadFile(upFileName)
        os.remove(upFileName)
    
    def _sendPacketAndGetReply(self,packet,upFileName,downFileName):
        ufp = gzip.open(upFileName,'wb')
        cPickle.dump(packet,ufp)
        ufp.close()
        
        self._uploadFile(upFileName)
        os.remove(upFileName)
        self._downloadFile(downFileName)
        
        dfp = gzip.open(downFileName,'rb')
        dataObj = cPickle.load(dfp)
        dfp.close()
        os.remove(downFileName)
        return dataObj
    
    def copyLocalCapToNodeCap(self,documents):
        if self._capTablesToCopy:
            self._copyDataToNode(documents, self.localCapDb,self._capTablesToCopy)

    def copyFullNodeAstToLocalAst(self,documents):
        if self._fullAstTablesToCopy:
            self._copyDataFromNode(documents, self.localAstDb,self._fullAstTablesToCopy)
    
    def copyDebugNodeAstToLocalAst(self,documents):
        if self._debugAstTablesToCopy:
            self._copyDataFromNode(documents, self.localAstDb,self._debugAstTablesToCopy)
        
    def copyFullNodeCapToLocalCap(self, documents):
        if self._fullCapTablesToCopy:
            self._copyDataFromNode(documents,self.localCapDb,self._fullCapTablesToCopy)
        
    def copyDebugNodeCapToLocalCap(self, documents):
        if self._debugCapTablesToCopy:
            self._copyDataFromNode(documents,self.localCapDb,self._debugCapTablesToCopy)
    
    def cleanNodeDb(self,documents):
        if self._tablesToClear or self._capTablesToCopy or self._fullAstTablesToCopy:
            self._doNodeCleanUp(documents,self._tablesToClear + self._capTablesToCopy + self._fullAstTablesToCopy)
    
    def getNodeState(self,scheduledDocs):
        initDocsQueue,processingDocsQueue,processedDocsQueue = [],[],[]
        if scheduledDocs:
            processingDocsQueue = self._executeDocSelectOnNode(scheduledDocs,self._nodeStateQuery)
            processingDocsQueue = [str(doc[0]) for doc in processingDocsQueue]
            initDocsQueue = [docId for docId in scheduledDocs if docId not in processingDocsQueue]
        return initDocsQueue,processingDocsQueue,processedDocsQueue
    
    def getDocsStatus(self,documents):
        result = self._executeDocSelectOnNode(documents,  self._docsStatusQuery)
        docsStatus = [(str(docId),docStatus) for docId,docStatus in result]
        return docsStatus

if __name__ == "__main__":
    documents = ['236615','236614']
    docString = '('+','.join(documents)+')'
    testQuery = 'select * from cap2.document where documentid in %d'
    db = database('240','240')
    NodeServerDetails = ['192.168.3.28','21','shadab','pwd']
    nftp = FtpNodeComponent('240','240',NodeServerDetails,None)
    dataFromDb = db.selectQuery(testQuery.replace('%d',docString))
    dataFromFtp = nftp._executeDocSelectOnNode(documents,testQuery)
    assert (dataFromDb == dataFromFtp)
        

