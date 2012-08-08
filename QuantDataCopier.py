import getopt,sys,time
import os
from database import *
from DataCopier import *

QuantSettings = None
class QuantDataCopier(DataCopier):
    
    def __init__(self,localCapDb,localAstDb,nodeDb,QuantSettings):
        DataCopier.__init__(self, localCapDb, localAstDb, nodeDb)
                
        self._capTablesToCopy = QuantSettings.capTablesToCopy
        
        self._fullCapTablesToCopy = QuantSettings.fullCapTablesToCopy
        self._debugCapTablesToCopy = QuantSettings.debugCapTablesToCopy
        
        self._debugAstTablesToCopy = QuantSettings.debugAstTablesToCopy
        self._fullAstTablesToCopy = QuantSettings.fullAstTablesToCopy
        
        self._tablesToTransform = QuantSettings.tablesToTransform
        self._tableSentenceQueries = QuantSettings.tableSentenceQueries
        
        self._tablesToClear = QuantSettings.tablesToClear
        
    def getTSMasterSlaveMapping(self,documents):
        docString = '('+','.join(documents)+')'
        query = 'select slavetablesentenceid,tablesentenceid from astprod.tablesentence \
                where documentid in '+docString
        result = self.localAstDb.selectQuery(query)
        return dict(result)
    
    def transformTableSentenceReferences(self,documents):
        if not self._tablesToTransform:
            return False
        
        for table in self._tablesToTransform:
            deleteQuery = self._getQueries(table,documents)[1]
            #delete local data
            self.localAstDb.execute_Non_Query(deleteQuery)
        
        tblSelectQuery,tblDeleteQuery,tblInsertQuery = self._getQueries(self._tableSentenceQueries, documents)
        
        self.localAstDb.execute_Non_Query(tblDeleteQuery)
        
        tblSentences = self.nodeDb.selectQuery(tblSelectQuery)
        
        if tblSentences:
            columnCount = len(tblSentences[0])
            vString = '('+'%s,'*(columnCount - 1)+'%s)'
            tblInsertQuery = tblInsertQuery.replace('%v',vString)
            self.localAstDb.executeInlot(tblInsertQuery,tblSentences)
            
        msMap = self.getTSMasterSlaveMapping(documents)
        
        for table in self._tablesToTransform:
            selectQuery,deleteQuery,insertQuery = self._getQueries(table,documents)
            #get data
            oldData = self.nodeDb.selectQuery(selectQuery)
            data = []
            for entry in oldData:
                slaveTSId = entry[0]
                record = list(entry)
                record[0] = msMap[slaveTSId]
                data.append(record)
                
            if data:
                columnCount = len(data[0])
                vString = '('+'%s,'*(columnCount - 1)+'%s)'
                insertQuery = insertQuery.replace('%v',vString)
                self.localAstDb.executeInlot(insertQuery,data)
            
            #delete Node data
            self.nodeDb.execute_Non_Query(deleteQuery)
        self.nodeDb.execute_Non_Query(tblDeleteQuery)
        return True
    
    def updateAutoProcessState(self,documents):
        docString = '('+','.join(documents)+')'
        selectQuery = 'select documentid,TableParsing,PeriodIdentification,\
                 ParameterIdentification,PeriodTranslation,AttributeExtraction,TableOldDataDeletion \
                 from cap2.autoprocessstate where documentid in ' + docString
        apData = self.nodeDb.selectQuery(selectQuery)
        for apRow in apData:
            docId = apRow[0]
            data = apRow[1:]
            updateQuery = 'update cap2.autoprocessstate set TableParsing = "%s" , PeriodIdentification = "%s" ,\
                     ParameterIdentification = "%s" ,PeriodTranslation = "%s" ,AttributeExtraction = "%s" ,TableOldDataDeletion ="%s" where \
                      documentid = '+str(docId)
            updateQuery = updateQuery % data
            self.localCapDb.execute_Non_Query(updateQuery)
    
    def copyFullNodeCapToLocalCap(self,documents):
        self.updateAutoProcessState(documents)
        DataCopier.copyFullNodeCapToLocalCap(self, documents)
    
    def copyDebugNodeCapToLocalCap(self,documents):
        self.updateAutoProcessState(documents)
        DataCopier.copyDebugNodeCapToLocalCap(self, documents)