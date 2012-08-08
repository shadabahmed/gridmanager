import getopt,sys,time
from database import *
from QuantDbComponent import QuantDbComponent
from QuantFtpComponent import QuantFtpComponent
from NodePoller import NodePoller

class QuantNodePoller(NodePoller):
    def __init__(self,capServerDetails,astServerDetails,nodeProfile,QuantSettings):
        NodePoller.__init__(self,nodeProfile)
        self.capServerDetails = capServerDetails
        self.astServerDetails = astServerDetails
        self.settings = QuantSettings
        self.pollerType = "QUANT "+self.mode 
        self.start()
    
    def initialize(self):
        self.localCapDb = database(*self.capServerDetails)
        self.localAstDb = database(*self.astServerDetails)
        
        if self.mode == "DB":
            self._nodeComponent = QuantDbComponent(self.localCapDb,self.localAstDb,self.nodeServerDetails,self.settings)
        else:
            self._nodeComponent = QuantFtpComponent(self.localCapDb,self.localAstDb,self.nodeServerDetails,self.settings)
            
    def _getCurrentDocsInProgress(self):
        query = 'select distinct documentid from cap2.tableparsingqueue_ap where tableextractionpoller = "'+self.ip \
                +'" and status = "IN PROGRESS"'
        inProgressDocs = self.localCapDb.select_Query(query)
        return inProgressDocs 
        
    def _copyDocsDataToNode(self,documents):
        self.printMsg('Started copying processing data for documents : ',documents)
        self._nodeComponent.copyLocalCapToNodeCap(documents)
        self.printMsg('Processing data copied for documents : ',documents)
    
    def _revertLocalDocsStatus(self,documents):
        docString = '('+','.join(documents)+')'
        query = 'update cap2.tableparsingqueue_ap set status = "PENDING" , tableextractionpoller = "None",timestamp = now() \
                 where status = "IN PROGRESS" and tableextractionpoller = "'+self.ip+'" and documentid in '+docString
        self.localCapDb.execute_Non_Query(query)
        self.printMsg('Docs Status reverted for documents :',documents)
    
    def _updateLocalDocsStatus(self,documents):
        docString = '('+','.join(documents)+')'
        query = 'update cap2.tableparsingqueue_ap set status = "IN PROGRESS" , tableextractionpoller = "' + self.ip + '" \
                 ,timestamp = now() where status = "PENDING" and tableextractionpoller = "None" and documentid in '+docString
        self.localCapDb.execute_Non_Query(query)
        self.printMsg('Docs Status Updated for documents :',documents)
    
    def _copyProcessedDocsData(self,successfulDocs,failedDocs):
        self.printMsg('Started copying processed data for documents : ',[successfulDocs,failedDocs])
        if successfulDocs:
            self._nodeComponent.copyFullNodeAstToLocalAst(successfulDocs)
            self._nodeComponent.copyFullNodeCapToLocalCap(successfulDocs)
            self.printMsg('Processed data copied for successful documents :',successfulDocs)
            if self._nodeComponent.transformTableSentenceReferences(successfulDocs):
                self.printMsg('Transformed data copied for successful documents :',successfulDocs)
        if failedDocs:
            self._nodeComponent.copyDebugNodeAstToLocalAst(failedDocs)
            self._nodeComponent.copyDebugNodeCapToLocalCap(failedDocs)
            self.printMsg('Debug data copied for failed documents :',failedDocs)
    
    def _updateCompletedDocsStatus(self,docsStatus):
        for docId,docStatus in docsStatus:
           query = 'update cap2.tableparsingqueue_ap set status = "'+docStatus+'",timestamp = now() where \
                   status = "IN PROGRESS" and tableextractionpoller = "'+self.ip+'" and \
                   documentid = '+docId
           self.localCapDb.execute_Non_Query(query)
        self.printMsg('Docs Status Updated on local Db for documents :',docsStatus)       
    
    def _cleanUpNodeDB(self,documents):
        self._nodeComponent.cleanNodeDb(documents)
