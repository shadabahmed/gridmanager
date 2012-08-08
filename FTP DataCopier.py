import getopt,sys,time
import os
from database import *

class DataCopier:
    
    _capTablesToCopy = []                
    _fullAstTablesToCopy = []
    _debugAstTablesToCopy = []
    _fullCapTablesToCopy = []
    _debugCapTablesToCopy = []
    
    def __init__(self,localCapDb,localAstDb,nodeDb):
        self.localCapDb = localCapDb 
        self.localAstDb = localAstDb
        self.nodeDb = nodeDb
    
    def _getQueries(self,table,documents):
        docString = '('+','.join(documents)+')'
        selectQuery,deleteQuery,insertQuery = None,None,None
        tableName = table
        if type(table) is tuple:
            tableName = table[0]
            for query in table[1:]:
                queryString = query.replace('%d',docString)
                if query.startswith('select') or query.startswith('update'):
                    selectQuery = queryString 
                elif query.startswith('delete'):
                    deleteQuery = queryString
                elif query.startswith('insert'):
                    insertQuery = queryString
                elif query.startswith('ignore'):
                    deleteQuery = ''
        if selectQuery is None:
            selectQuery = 'select * from '+tableName+' where documentid in '+docString
        if insertQuery is None:
            insertQuery = 'insert into '+tableName+' values %v'
        if deleteQuery is None:
            deleteQuery = 'delete from '+tableName+' where documentid in '+docString
        return selectQuery,deleteQuery,insertQuery
        
        
    def _doCleanUp(self,documents,destDb,tablesToCopy):
        revDocList = tablesToCopy[:]
        revDocList.reverse()
        for table in revDocList:
            deleteQuery = self._getQueries(table)[1]
            if deleteQuery:
                destDb.execute_Non_Query(deleteQuery)
        
    def _copyDataForDocuments(self,documents,srcDb,destDb,tablesToCopy):
        for table in tablesToCopy:
            selectQuery,deleteQuery,insertQuery = self._getQueries(table,documents)
            
            #get data
            data = srcDb.selectQuery(selectQuery)
            
            if data is not None and len(data) > 0:
                columnCount = len(data[0])
                vString = '('+'%s,'*(columnCount - 1)+'%s)'
                insertQuery = insertQuery.replace('%v',vString)
                destDb.executeInlot(insertQuery,data)
    
    def _getQueriesCompressed(self,table):
        selectQuery,deleteQuery,insertQuery = None,None,None
        tableName = table
        if type(table) is tuple:
            tableName = table[0]
            for query in table[1:]:
                if query.startswith('select') or query.startswith('update'):
                    selectQuery = queryString 
                elif query.startswith('delete'):
                    deleteQuery = queryString
                elif query.startswith('insert'):
                    insertQuery = queryString
                elif query.startswith('ignore'):
                    deleteQuery = ''
        if selectQuery is None:
            selectQuery = 'select * from '+tableName+' where documentid in %d'
        if insertQuery is None:
            insertQuery = 'insert into '+tableName+' values %v'
        if deleteQuery is None:
            deleteQuery = 'delete from '+tableName+' where documentid in %d'
        return selectQuery,deleteQuery,insertQuery
    
    DELPKT = 0xAA
    INSERTPKT = 0xA1
    SELECTPKT = 0xA2
    
    def _doCleanUpCompressed(self,documents,destDb,tablesToCopy):
        revDocList = tablesToCopy[:]
        revDocList.reverse()
        deleteQueries = []
        
        for table in revDocList:
            deleteQuery = self._getQueries(table)[1]
            if deleteQuery:
                deleteQueries.append(deleteQuery)
        
        packet = [DELPKT,deleteQueries,documents]
        
        
     
    def _copyDataForDocumentsCompressed(self,documents,srcDb,destDb,tablesToCopy):
        compressedData = []
        
        for table in tablesToCopy:
            selectQuery,deleteQuery,insertQuery = self._getQueries(table)
            
            #get data
            data = srcDb.selectQuery(selectQuery)
            if data:
                compressedData.append((insertQuery,data))
        
        for insertQuery,data in compressedData:
            columnCount = len(data[0])
            vString = '('+'%s,'*(columnCount - 1)+'%s)'
            insertQuery = insertQuery.replace('%v',vString)
            destDb.executeInlot(insertQuery,data)
    
    def copyLocalCapToNodeCap(self,documents):
        self._doCleanUp(documents, self.nodeDb, self._capTablesToCopy)
        self._copyDataForDocuments(documents,self.localCapDb,self.nodeDb,self._capTablesToCopy)

    def copyFullNodeAstToLocalAst(self,documents):
        self._doCleanUp(documents, self.localAstDb, self._fullAstTablesToCopy)
        self._copyDataForDocuments(documents,self.nodeDb,self.localAstDb,self._fullAstTablesToCopy)
    
    def copyDebugNodeAstToLocalAst(self,documents):
        self._doCleanUp(documents, self.localAstDb, self._debugAstTablesToCopy)
        self._copyDataForDocuments(documents,self.nodeDb,self.localAstDb,self._debugAstTablesToCopy)
        
    def copyFullNodeCapToLocalCap(self, documents):
        self._doCleanUp(documents, self.localCapDb, self._fullCapTablesToCopy)
        self._copyDataForDocuments(documents,self.nodeDb,self.localCapDb,self._fullCapTablesToCopy)
        
    def copyDebugNodeCapToLocalCap(self, documents):
        self._doCleanUp(documents, self.localCapDb, self._debugCapTablesToCopy)
        self._copyDataForDocuments(documents,self.nodeDb,self.localCapDb,self._debugCapTablesToCopy)
    
    def cleanNodeDb(self,documents):
        self._doCleanUp(documents, self.nodeDb, self._fullAstTablesToCopy)
        self._doCleanUp(documents, self.nodeDb, self._capTablesToCopy)
        self._doCleanUp(documents, self.nodeDb, self._tablesToClear)
