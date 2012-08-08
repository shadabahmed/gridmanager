import getopt,sys,time
import os
from ast.conf import conf
from ast.OSSystemWrapper import *
from QualNodePoller import *
from QuantNodePoller import *
from database import *

class GridManager:
    def __init__(self,mode):
        self.path = os.path.dirname(__file__)
        
        self.autoprocess = 'AUTOPROCESS'
        self.cap = 'CAP'
        self.ast = 'AST'
        
        self.confObj = conf()
        capDetails = self.confObj.readPath(self.cap)
        self.capServerDetails = [capDetails['dbhost'],capDetails['dbport'],capDetails['dbuser'],capDetails['dbpasswd'],capDetails['dbname']]
        
        astDetails = self.confObj.readPath(self.ast)
        self.astServerDetails = [astDetails['dbhost'],astDetails['dbport'],astDetails['dbuser'],astDetails['dbpasswd'],astDetails['dbname']]
        
        autoprocessDetails = self.confObj.readPath(self.autoprocess)
        self.autoprocessServerDetails = [autoprocessDetails['dbhost'],autoprocessDetails['dbport'],autoprocessDetails['dbuser'],autoprocessDetails['dbpasswd'],autoprocessDetails['dbname']]
        
        self.capDatabaseObj = database(*self.capServerDetails)
        self.autoDatabaseObj = database(*self.autoprocessServerDetails)
        
        self.ip = self.capServerDetails[0]
        self.mode = mode
        
        if mode == 'UDS':
            if os.path.exists('udssettings.pyc'):
                os.remove('udssettings.pyc')
            settings = __import__('udssettings')
        else:
            if os.path.exists('fullsettings.pyc'):
                os.remove('fullsettings.pyc')
            settings = __import__('fullsettings')
        
        self.GridSettings = settings.GridSettings
        self.QualSettings = settings.QualSettings
        self.QuantSettings = settings.QuantSettings
        
        #sleep time
        self.sleepTime = self.GridSettings.sleepTime
        
        #self.qualDocsQuery = self.GridSettings.qualDocsQuery
        #self.quantDocsQuery = self.GridSettings.quantDocsQuery
        
        self.qualNodeProfileMap,self.quantNodeProfileMap = self.getNodes()
        self.qualProfiles,self.quantProfiles = self.getProfiles()
        
#        self.qualNodes,self.quantNodes = {},{}
        self.qualDocsLimit = self.QualSettings.qualDocsLimit
        self.quantDocsLimit = self.QuantSettings.quantDocsLimit
        
        self.qualDocsQuery = self.GridSettings.qualDocsQuery
        self.quantDocsQuery = self.GridSettings.quantDocsQuery
                
#        self.qualNodes = self.__getQualNodes()
#        self.quantNodes = self.__getQuantNodes()
        
        #self.__modifyConfFile(self.qualNodes,self.quantNodes)
        
        #self.path = __fi
        
        self.startPoller()
    
    def getProfiles(self):
        profileAttributes = ["mode","port","user","password","sleepTime","priority","minDocLoad","maxDocLoad","maxDocsToCopy","minDocsToWriteBack","maxDocsToWriteBack"]
        fprofile = open(self.path+"/profiles.txt",'r')
        qualProfiles , quantProfiles = {},{}
        for line in fprofile:
            line = line.strip()
            if line.startswith('#') or not line:
                pass 
            elif line.startswith("[qual]"):
                curDict = qualProfiles
            elif line.startswith("[quant]"):
                curDict = quantProfiles
            elif line.find("=") > 0:
                line = line.split('#')[0]
                profileName,profileValue = line.split("=")
                profileName = profileName.strip()
                profileValue = profileValue.strip()
                if profileValue:
                    profileValue = profileValue.split(':')
                    profileValue = dict(zip(profileAttributes,profileValue))
                else:
                    profileValue = {}
                if profileName != 'default':
                   defaultProfile = curDict['default']
                   for key in defaultProfile.keys():
                       if key not in profileValue:
                           profileValue[key] = defaultProfile[key]
                curDict[profileName] = profileValue
        fprofile.close()
        return qualProfiles,quantProfiles
    
    def getNodes(self):
        fnodes = open(self.path+"/nodes.txt",'r')
        qualNodes , quantNodes = [],[]
        for line in fnodes:
            line = line.strip()
            if line.startswith('#') or not line:
                pass 
            elif line.startswith("[qual]"):
                curList = qualNodes
            elif line.startswith("[quant]"):
                curList = quantNodes
            else: 
                line = line.split('#')[0]
                line = line.strip()
                if line.find(':') > 0:
                    node,nodeProfile = line.split(":")
                else:
                    node = line
                    nodeProfile = 'default'
                curList.append((node,nodeProfile))
        return qualNodes,quantNodes
              
    def __getQualNodes(self):
        nodes = self.GridSettings.qualNodes
        return nodes 
    
    def __getQuantNodes(self):
        nodes = self.GridSettings.quantNodes
        return nodes 
        
    def __pollForQualDocuments(self):
        selectQuery = self.qualDocsQuery.replace('%l',str(self.qualDocsLimit))
        result = self.autoDatabaseObj.select_Query(selectQuery)
        return result
    
    def __pollForQuantDocuments(self):
        selectQuery = self.quantDocsQuery.replace('%l',str(self.quantDocsLimit))
        result = self.capDatabaseObj.select_Query(selectQuery)
        return result
    
    def __initializePollers(self):
        self.quantPollers = {}
        self.qualPollers = {}
        self.qualNodes,self.quantNodes = [],[]
        
        for node,profileName in self.qualNodeProfileMap:
            nodeProfile = self.qualProfiles[profileName].copy()
            nodeProfile['host'] = node  
            self.qualPollers[node] = QualNodePoller(self.capServerDetails,self.astServerDetails,nodeProfile,self.QualSettings)
            self.qualNodes.append(node)
            
        for node,profileName in self.quantNodeProfileMap:
            nodeProfile = self.qualProfiles[profileName].copy()
            nodeProfile['host'] = node
            self.quantPollers[node] = QuantNodePoller(self.capServerDetails,self.astServerDetails,nodeProfile,self.QuantSettings)
            self.quantNodes.append(node)
            
    def startPoller(self):
#        try:
            self.__initializePollers()
            while True:
                if self.qualNodes:
                    qualDocs = self.__pollForQualDocuments()
                    qualNodesCount = len(self.qualNodes)
                    qualDocsParts = [qualDocs[i::qualNodesCount] for i in range(qualNodesCount)] 
                    qualDocsDict = dict(zip(self.qualNodes,qualDocsParts))
                
                if self.quantNodes:
                    quantDocs = self.__pollForQuantDocuments()
                    quantNodesCount = len(self.quantNodes)
                    quantDocsParts = [quantDocs[i::quantNodesCount] for i in range(quantNodesCount)] 
                    quantDocsDict = dict(zip(self.quantNodes,quantDocsParts))
                
                for node in self.qualNodes:
                    nodeDocs = qualDocsDict[node] 
                    if self.qualPollers[node].getLoad() < self.QualSettings.minDocLoad and len(nodeDocs) > 0 :
                        self.qualPollers[node].schedule(nodeDocs)
                
                for node in self.quantNodes:
                    nodeDocs = quantDocsDict[node]                 
                    if self.quantPollers[node].getLoad() < self.QuantSettings.minDocLoad and len(nodeDocs) > 0 :
                        self.quantPollers[node].schedule(nodeDocs)
                        
                time.sleep(self.sleepTime)
#        except Exception,detail:
#            print 'GridManager crashed with following exception :',detail
#            sys.exit(1)

if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:m:", ["help","mode"])
    except getopt.GetoptError:
        print 'Usage example: python GridManager.py -m FULL'
        sys.exit(2)
        
    mode = "FULL"
    for o, a in opts:
        if o in ("-h", "--help"):
            print 'Usage example: python GridManager.py -m FULL'
            sys.exit()      
        if o in ("-m","--mode"):
            mode = a

    if mode is None:
        print 'Please specify the mode'
        print 'Usage example: python GridManager.py -m FULL'
        sys.exit()

    print 'Starting GridManager in',mode,'mode ...'
    obj = GridManager(mode)
