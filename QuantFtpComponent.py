import getopt,sys,time
import os
from database import *
from FtpNodeComponent import *

QuantSettings = None
class QuantFtpComponent(FtpNodeComponent):
    
    def __init__(self,localCapDb,localAstDb,nodeServerDetails,QuantSettings):
        FtpNodeComponent.__init__(self, localCapDb, localAstDb, nodeServerDetails,QuantSettings)
        
        self._tablesToTransform = QuantSettings.tablesToTransform
        self._tableSentenceQueries = QuantSettings.tableSentenceQueries
        self._autoProcessStateQueries = QuantSettings.autoProcessStateQueries
        self._tablesToClear =  [self._tableSentenceQueries] + self._tablesToTransform + self._tablesToClear
                
    def getTSMasterSlaveMapping(self,documents):
        docString = '('+','.join(documents)+')'
        query = 'select slavetablesentenceid,tablesentenceid from astprod.tablesentence \
                where documentid in '+docString
        result = self.localAstDb.selectQuery(query)
        return dict(result)
    
    def transformTableSentenceReferences(self,documents):
        if not self._tablesToTransform:
            return False
        
        tablesToFetch = [self._tableSentenceQueries] + self._tablesToTransform
        
        deleteQueries , insertQueries , dataDict = self._getDataFromNode(documents, tablesToFetch)
        deleteQueries = dict(deleteQueries)
        insertQueries = dict(insertQueries)
        # TODO: Optimize this - Create another method for fetching table names
        tablesToTransformNames = [self._getQueries(table)[0] for table in self._tablesToTransform]
        tableSentenceTableName = self._getQueries(self._tableSentenceQueries)[0]
        
        docString = '('+','.join(documents)+')'
        
        for tableName in tablesToTransformNames:
            deleteQuery = deleteQueries[tableName]
            if deleteQuery:
                deleteQuery = deleteQuery.replace('%d',docString)
                #delete local data
                self.localAstDb.execute_Non_Query(deleteQuery)
        
        tblsDeleteQuery,tblsInsertQuery = deleteQueries[tableSentenceTableName],insertQueries[tableSentenceTableName]
        tblsDeleteQuery = tblsDeleteQuery.replace('%d' , docString)
        tblsInsertQueryy = tblsInsertQuery.replace('%d' , docString)
        
        #delete local table sentences
        self.localAstDb.execute_Non_Query(tblsDeleteQuery)
        
        tblSentences = dataDict[tableSentenceTableName]
        
        if tblSentences:
            columnCount = len(tblSentences[0])
            vString = '('+'%s,'*(columnCount - 1)+'%s)'
            tblsInsertQuery = tblsInsertQuery.replace('%v',vString)
            self.localAstDb.executeInlot(tblsInsertQuery,tblSentences)
            
        msMap = self.getTSMasterSlaveMapping(documents)
        
        for tableName in tablesToTransformNames:
            deleteQuery,insertQuery = deleteQueries[tableName],insertQueries[tableName]
            deleteQuery = deleteQuery.replace('%d',docString)
            insertQuery = insertQuery.replace('%d',docString)
            #get data
            oldData = dataDict[tableName]
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
        return True
    
    def updateAutoProcessState(self,documents):
        docString = '('+','.join(documents)+')'
        selectQuery = 'select documentid,TableParsing,PeriodIdentification,\
                 ParameterIdentification,PeriodTranslation,AttributeExtraction,TableOldDataDeletion \
                 from cap2.autoprocessstate where documentid in ' + docString
        apData = self._executeSelectOnNode(selectQuery)
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
        FtpNodeComponent.copyFullNodeCapToLocalCap(self, documents)
    
    def copyDebugNodeCapToLocalCap(self,documents):
        self.updateAutoProcessState(documents)
        FtpNodeComponent.copyDebugNodeCapToLocalCap(self, documents)