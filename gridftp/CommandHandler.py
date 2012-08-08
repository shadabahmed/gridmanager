from ast.database import database
import gzip,cPickle
import sys

class StringFile:
    class StringHandler:
        def __init__(self):
            self.text = ''
        
        def write(self,text):
            self.text = self.text + text
        
        def flush(self):
            pass
        
    def __init__(self):
        self.stdout = sys.stdout
        self.stderr = sys.stderr
        sys.stdout = self.StringHandler() 
        sys.stderr = self.StringHandler()
        
    def restoreAndReturn(self):
        stdText,strText = sys.stdout.text,sys.stderr.text
        sys.stdout = self.stdout
        sys.stderr = self.stderr
        return stdText,strText

class CommandHandler:
    def __init__(self,target):
        self.db = database(target,target)
    
    def __del__(self):
        pass
        #self.db.close()
        
    def _cmd_GETNODESTATUS(self,commandObject):
        pass
    
    def _cmd_SELECT(self,commandObject):
        query = commandObject[0]
        data = self.db.selectQuery(query)
        
    
    def _cmd_COPYDOCDATAANDTRIGGER(self,commandObject):
        documents,delQueries,insertQueriesData,triggerQueries = commandObject
        docString = '(' + ','.join(documents)+')'
        
        for delQuery in delQueries:
            if delQuery:
                delQuery = delQuery.replace('%d',docString)
                self.db.execute_Non_Query(delQuery)
            
        for insertQuery,data in insertQueriesData:
            columnCount = len(data[0])
            vString = '('+'%s,'*(columnCount - 1)+'%s)'
            insertQuery = insertQuery.replace('%v',vString)
            self.db.executeInlot(insertQuery,data)
        
        triggerInsertQuery,triggerDeleteQuery = triggerQueries
        
        triggerDeleteQuery = triggerDeleteQuery.replace('%d',docString)
        self.db.execute_Non_Query(triggerDeleteQuery)
        
        self.db.executeInlot(triggerInsertQuery, documents)
        print 'Data copied to node'
    
    def _cmd_GENERICNONSELECTQUERY(self,commandObject):
        query = commandObject
        if query:
            self.db.execute_Non_Query(query)
            print 'Generic non select query executed'
    
    def _cmd_GENERICSELECTQUERY(self,commandObject):
        selectQuery,downFileName = commandObject
        if selectQuery:
            data = self.db.selectQuery(selectQuery)
            dfp = gzip.open(downFileName,'wb')
            cPickle.dump(data,dfp)
            dfp.close()
            print 'Generic select query executed'
    
    def _cmd_DOCSELECTQUERY(self,commandObject):
        documents,selectQuery,downFileName = commandObject
        docString = '(' + ','.join(documents)+')'
        if selectQuery:
            selectQuery = selectQuery.replace('%d',docString)
            data = self.db.selectQuery(selectQuery)
            dfp = gzip.open(downFileName,'wb')
            cPickle.dump(data,dfp)
            dfp.close()
            print 'Doc select query executed'
    
    def _cmd_DOCSNONSELECTQUERIES(self,commandObject):
        documents,queries = commandObject
        docString = '(' + ','.join(documents)+')'
        for query in queries:
            if query:
                query = query.replace('%d',docString)
                self.db.execute_Non_Query(query)
        print 'Generic docs non select queries executed'
    
    def _cmd_GETDOCDATA(self,commandObject):
        documents,tableSelectQueries,downFileName = commandObject
        docString = '(' + ','.join(documents)+')'
        data = {}
        for tableName,selectQuery in tableSelectQueries:
            if selectQuery:
                selectQuery = selectQuery.replace('%d',docString)
                data[tableName] = self.db.selectQuery(selectQuery)
        dfp = gzip.open(downFileName,'wb')
        cPickle.dump(data,dfp)
        dfp.close()
        print 'Document data fetched and stored in',downFileName
    
    def _cmd_TRIGGER(self,commandObject):
        pass
    
    def _cmd_GETDOCSTATUS(self,commandObject):
        pass
    
    def _cmd_COPYPROCESSEDDATA(self,commandObject):
        pass
    
    def _cmd_CLEANDB(self,commandObject):
        pass
    
    def unpackFile(self,commandFile):
        fp = gzip.open(commandFile,'rb')
        cmdObject = cPickle.load(fp)
        fp.close()
        return cmdObject
    
    def processCommand(self,commandFile):
        #unpack file
        fp = gzip.open(commandFile,'rb')
        cmdObject = cPickle.load(fp)
        fp.close()

        cmd = cmdObject[0]
        cmdMethod = getattr(self, '_cmd_' + cmd)
        sf = StringFile()
        cmdMethod(cmdObject[1:])
        return sf.restoreAndReturn()

if __name__ == "__main__":
    cmdHandler = CommandHandler('238')
    std,str = cmdHandler.processCommand('f5bdc927d4316c8f4069c95333ac289b.gru')
    print std,str
    pass
