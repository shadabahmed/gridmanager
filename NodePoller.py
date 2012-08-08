import time
from time import strftime
from Queue import Queue
import thread
import threading
import sys

class NodePoller:
    sleepTimeConst = 10
    maxDocsToCopyConst = 2
    maxDocsToWriteBackConst = 4
    minDocsToWriteBackConst = 2
    
    def __init__(self,nodeProfile):
        self.terminated = True
                
        self.docsStatus = {}
        self.successfulDocs = []
        self.failedDocs = []
        self.completedDocs = []
        self.skippedDocs = []
        self.scheduledDocs = []
        self.docsInProcessing =[]
        
        self.docsQueue = Queue(-1)
        
        self.documentLoad = 0
        
        nodeProfile = nodeProfile.iteritems()
        
        for attribute,value in nodeProfile:
            if value.isdigit():
                value = int(value)
            setattr(self,attribute,value)
        
        self.nodeServerDetails = [self.host,self.port,self.user,self.password]
        self.minDocsToWriteBackConst = self.minDocsToWriteBack
        self.ip = self.host
        self._nodeComponent = None
        self.lock = threading.Lock()
    
    def initialize(self):
        pass
    
    def start(self):
        #self._poll() # for debugging only
        thread.start_new_thread(self._poll,()) # start the polling thread
    
    def getLoad(self):
        return self.documentLoad
    
    def updateDocLoad(self):
        self.lock.acquire() # will block if lock is already held
        try:
            self.docsInProcessing = [docId for docId in self.scheduledDocs if docId not in self.completedDocs]
            self.documentLoad = len(self.docsInProcessing)
            if self.documentLoad < self.minDocsToWriteBack and self.documentLoad > 0:
                self.minDocsToWriteBack = self.documentLoad
            elif self.minDocsToWriteBack < self.minDocsToWriteBackConst:
                self.minDocsToWriteBack = self.minDocsToWriteBackConst
        finally:
            self.lock.release()
    
    def _getCurrentDocsInProgress(self):
        pass
    
    def _getCurrentNodeState(self):
        initDocsQueue,processingDocsQueue,processedDocsQueue = self._nodeComponent.getNodeState(self.scheduledDocs)
        return initDocsQueue,processingDocsQueue,processedDocsQueue
    
    def _copyDocsDataToNode(self,documents):
        pass
    
    def _revertLocalDocsStatus(self,documents):
        pass
    
    def _updateLocalDocsStatus(self,documents):
        pass
    
    def _triggerDocsOnNode(self,documents):
        self._nodeComponent.triggerDocs(documents)
        self.printMsg('Documents triggered on node :',documents)
    
    def _getDocsStatusOnNode(self,documents):
        docsStatus = self._nodeComponent.getDocsStatus(documents)
        if docsStatus:
            self.printMsg('Documents finished on node :',docsStatus)
        return docsStatus
    
    def _copyProcessedDocsData(self,successfulDocs,failedDocs):
       pass
    
    def _updateCompletedDocsStatus(self,docsStatus):
        pass       
    
    def _cleanUpNodeDB(self,documents):
        pass

    def printMsg(self,msg,documents):
        print strftime("%a %H:%M:%S"),self.pollerType,self.ip,':',msg,documents
        sys.stdout.flush()
    
    def _poll(self):
        try:
            self.initialize()
            self.documentLoad = 0
            self.scheduledDocs = self._getCurrentDocsInProgress()
            initDocsQueue,processingDocsQueue,processedDocsQueue = self._getCurrentNodeState()
            self.updateDocLoad()
            self.terminated = False
            while(not self.terminated):
                
                # add to initialization queue
                if not self.docsQueue.empty():
                    newDocs = self.docsQueue.get(False)
                    initDocsQueue.extend(newDocs)
                    
                # check for initialization queue
                if initDocsQueue:
                    if len(initDocsQueue) < self.maxDocsToCopy :
                        docsToCopy =initDocsQueue[:]
                        initDocsQueue = []
                    else :
                        docsToCopy = initDocsQueue[:self.maxDocsToCopy]
                        del initDocsQueue[:self.maxDocsToCopy]
                        
                    self._copyDocsDataToNode(docsToCopy)
                    self._triggerDocsOnNode(docsToCopy)
                    processingDocsQueue.extend(docsToCopy)
                
                #check status for currently processed documents
                if processingDocsQueue:
                    docsStatus = self._getDocsStatusOnNode(processingDocsQueue)
                    if docsStatus:
                        for docId,docStatus in docsStatus:
                            processingDocsQueue.remove(docId)
                        processedDocsQueue.extend(docsStatus)
                
                #copy data for processed documents and update flag
                if len(processedDocsQueue) >= self.minDocsToWriteBack:
                    if len(processedDocsQueue) < self.maxDocsToWriteBack:
                        docsToWriteBack = processedDocsQueue[:]
                        processedDocsQueue = []
                    else:
                        docsToWriteBack = processedDocsQueue[:self.maxDocsToWriteBack]
                        del processedDocsQueue[:self.maxDocsToWriteBack]
                    completedDocs = [docId for docId,docStatus in docsToWriteBack]
                    failedDocs = [docId for docId,docsStatus in docsToWriteBack if docsStatus == 'FAILED']
                    successfulDocs = [docId for docId,docsStatus in docsToWriteBack if docsStatus == 'SUCCESS']
                    
                    self._copyProcessedDocsData(successfulDocs,failedDocs)
                    self._updateCompletedDocsStatus(docsToWriteBack)
                    self._cleanUpNodeDB(completedDocs)
                    
                    self.completedDocs.extend(completedDocs)
                    self.failedDocs.extend(failedDocs)
                    self.successfulDocs.extend(successfulDocs)
                    
                    self.updateDocLoad()
        
                processedDocs = [docId for docId,status in processedDocsQueue]
                
                self.skippedDocs = [docId for docId in self.scheduledDocs if docId not in self.completedDocs \
                                    and docId not in processingDocsQueue and docId not in initDocsQueue and \
                                    docId not in processedDocs]
                
                time.sleep(self.sleepTime*(len(processingDocsQueue) + 1))
        except Exception,detail:
            terminated = True
            self.printMsg("Crashed with following exception ",detail)
            return            

    def schedule(self,documents):
        if documents and not self.terminated:
            #self._deleteLocalDocsStatus(documents)
            self._updateLocalDocsStatus(documents)
            self.docsQueue.put(documents, True)
            self.scheduledDocs.extend(documents)
            self.updateDocLoad()
        
