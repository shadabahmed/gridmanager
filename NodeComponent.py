class NodeComponent:
    
    _capTablesToCopy = []                
    _fullAstTablesToCopy = []
    _debugAstTablesToCopy = []
    _fullCapTablesToCopy = []
    _debugCapTablesToCopy = []
    _tablesToClear = []
    _nodeStateQuery = ''
    _docsStatusQuery = ''
    triggerQueries = ['','']
    
    def __init__(self,settings):
        if settings:
            self._capTablesToCopy = settings.capTablesToCopy
            
            self._debugAstTablesToCopy = settings.debugAstTablesToCopy
            self._fullAstTablesToCopy = settings.fullAstTablesToCopy
            
            self._fullCapTablesToCopy = settings.fullCapTablesToCopy
            self._debugCapTablesToCopy = settings.debugCapTablesToCopy
            
            self._tablesToClear = settings.tablesToClear
            
            self._triggerQueries = settings.triggerQueries
            self._nodeStateQuery = settings.nodeStateQuery
            self._docsStatusQuery = settings.docsStatusQuery
    
    def copyLocalCapToNodeCap(self,documents):
        pass

    def copyFullNodeAstToLocalAst(self,documents):
        pass
    
    def copyDebugNodeAstToLocalAst(self,documents):
        pass
        
    def copyFullNodeCapToLocalCap(self, documents):
        pass
        
    def copyDebugNodeCapToLocalCap(self, documents):
        pass
    
    def cleanNodeDb(self,documents):
        pass
    
    def getNodeState(self,scheduledDocs):
        pass
    
    def getDocsStatus(self,documents):
        pass

    def triggerDocs(self,documents):
        pass
