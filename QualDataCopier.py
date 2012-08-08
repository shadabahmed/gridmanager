import getopt,sys,time
import os
from DataCopier import *

QualSettings = None
class QualDataCopier(DataCopier):
    
    def __init__(self,localCapDb,localAstDb,nodeDb,QualSettings):
        DataCopier.__init__(self, localCapDb, localAstDb, nodeDb)
        
        self._capTablesToCopy = QualSettings.capTablesToCopy
        
        self._debugAstTablesToCopy = QualSettings.debugAstTablesToCopy
        self._fullAstTablesToCopy = QualSettings.fullAstTablesToCopy
        
        self._fullCapTablesToCopy = QualSettings.fullCapTablesToCopy
        self._debugCapTablesToCopy = QualSettings.debugCapTablesToCopy
        
        self._tablesToClear = QualSettings.tablesToClear
        