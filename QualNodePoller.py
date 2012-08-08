import getopt,sys
from time import *
import os
from ast.conf import conf
from ast.logger import *
from NodePoller import NodePoller
from QualDbComponent import QualDbComponent
from QualFtpComponent import QualFtpComponent
from database import *

class QualNodePoller(NodePoller):
    
    def __init__(self,capServerDetails,astServerDetails,nodeProfile,QualSettings):
        NodePoller.__init__(self,nodeProfile)
        self.capServerDetails = capServerDetails
        self.astServerDetails = astServerDetails
        self.settings = QualSettings
        self.pollerType = "QUAL "+self.mode 
        self.start()
    
    def initialize(self):
        self.localCapDb = database(*self.capServerDetails)
        self.localAstDb = database(*self.astServerDetails)
        
        if self.mode == "DB":
            self._nodeComponent = QualDbComponent(self.localCapDb,self.localAstDb,self.nodeServerDetails,self.settings)
        else:
            self._nodeComponent = QualFtpComponent(self.localCapDb,self.localAstDb,self.nodeServerDetails,self.settings)
        
    
    # TODO: move back to node poller
    def _getCurrentDocsInProgress(self):
        query = 'select distinct documentid from autoprocess.workflowstate_ap where actionitemid = 2\
                 and pollerid = "'+self.ip+':2" and state = "IN PROGRESS"'
        inProgressDocs = self.localCapDb.select_Query(query)
        return inProgressDocs

    # TODO: move back to node poller 
    def _copyDocsDataToNode(self,documents):
        self.printMsg('Started copying processing data for documents : ',documents)
        self._nodeComponent.copyLocalCapToNodeCap(documents)
        self.printMsg('Processing data copied for documents : ',documents)
    
    # TODO: move back to node poller
    def _revertLocalDocsStatus(self,documents):
        docString = '('+','.join(documents)+')'
        query = 'delete from autoprocess.workflowstate_ap where actionitemid = 2\
                 and documentid in '+docString+' and pollerid = "'+self.ip+':2\
                " and state = "IN PROGRESS"'
        self.localCapDb.execute_Non_Query(query)
        self.printMsg('Docs Status reverted for documents :',documents)
    
    # TODO: move back to nodepoller   
    def _updateLocalDocsStatus(self,documents):
        state = 'IN PROGRESS'
        for docId in documents:
           self.__insertQualDocState(docId,state)
        self.printMsg('Docs Status Updated for documents :',documents)
    
    def __insertQualDocState(self,doc,state):
        query = 'insert into autoprocess.workflowstate_ap (Documentid,ActionItemId,State,PollerId) \
             select ' + doc + ',"2","' + state + '","' + self.ip+':2"  from \
             (select count(*) as records from autoprocess.workflowstate_ap where documentid=' + doc + ' and actionitemid="2")data \
             where records = 0'
        status = self.localCapDb.execute_Non_Query(query)
    
    # TODO: Move back to node poller
    def _copyProcessedDocsData(self,successfulDocs,failedDocs):
        self.printMsg('Started copying processed data for documents : ',[successfulDocs,failedDocs])
        if successfulDocs:
            self._nodeComponent.copyFullNodeAstToLocalAst(successfulDocs)
            self._nodeComponent.copyFullNodeCapToLocalCap(successfulDocs)
            self.printMsg('Processed data copied for successful documents :',successfulDocs)
        if failedDocs:
            self._nodeComponent.copyDebugNodeAstToLocalAst(failedDocs)
            self._nodeComponent.copyDebugNodeCapToLocalCap(failedDocs)
            self.printMsg('Debug data copied for failed documents :',failedDocs)
    
    # TODO: Move back to node poller
    def _updateCompletedDocsStatus(self,docsStatus):
        for docId,docStatus in docsStatus:
            query = 'update autoprocess.workflowstate_ap set state = "' + docStatus + '", timestamp = now() where \
                    documentid = ' + docId + ' and actionitemid = 2 and pollerid="' \
                    + self.ip+':2"' 
            self.localCapDb.execute_Non_Query(query)
        self.printMsg('Docs Status Updated on local Db for documents :',docsStatus)
        
    def _cleanUpNodeDB(self,documents):
        self._nodeComponent.cleanNodeDb(documents)
