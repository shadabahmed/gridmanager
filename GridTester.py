import getopt,sys,time
import os
from database import *
import re
        
class GridTester:
    def __init__(self,srcDb,destDb,mode,workflow):
        if workflow == "QL":
            self.tablesToCompare = self.qlTablesToCompare
        else:
            self.tablesToCompare = self.qtTablesToCompare
        self.workflow = workflow    
        self.srcDb = database(srcDb,srcDb)
        self.destDb = self.srcDb #database(destDb,destDb)
        self.reg = re.compile('[\s]+')
        
    def saveAsFile(self,document,table,data):
        file = open('./logs/'+document+'.'+table+'.'+self.workflow+'.txt','w')
        for line in data:
            file.write(str(line)+'\n')
        file.close()
    
    __sentbfQuery = 'select concat(bflessneg6," ",bfneg6toneg5," ",bfneg5toneg4," ",bfneg4toneg3," ",bfneg3toneg2," ",\
                            bfneg2to0," ",bf0to2," ",bf2to3," ",bf3to4," ",bf4to5," ",bf5to6," ",bfgreater6) from astprod.abstractsentencebf \
                            where documentid = %d order by sentenceid'
                            
    __sentmptQuery = 'select concat(mptlessneg6," ",mptneg6toneg5," ",mptneg5toneg4," ",\
                    mptneg4toneg3," ",mptneg3toneg2," ",mptneg2to0," ",MPT0to2," ",MPT2to3," ",MPT3to4," ",\
                    MPT4to5," ",MPT5to6," ",MPTGreater6) \
                    from astprod.abstractsentencempt where documentid = %d order by sentenceid'
                    
    __sentwordQuery = 'select nodeToWordMap from astprod.abstractsentencenodetowordmap where documentid = %d \
                        order by sentenceid'     
                            
    
    __snipbfQuery = 'select concat(bflessneg6," ",bfneg6toneg5," ",bfneg5toneg4," ",bfneg4toneg3," ",\
                    bfneg3toneg2," ",bfneg2to0," ",bf0to2," ",bf2to3," ",bf3to4," ",bf4to5," ",bf5to6," ",bfgreater6) \
                     from astprod.abstractsnippetbf where documentid = %d order by snippetid'
   
    __snipdesigdenormQuery = 'select isceo,iscfo,ischairman,iscoo,iscto,isevpsvpvp,isiro,isother,\
                       ispresident,isnone from astprod.abstractsnippetdesignationdenormalizedmap where documentid = %d \
                       order by snippetid'
                       
    __snipdesigQuery = 'select designationid from astprod.abstractsnippetiddesignationidmap where documentid = %d \
                        order by snippetid'
    
    __snipvpQuery = 'select viewpointid from astprod.abstractsnippetidviewpointidgroupedmap where documentid = %d \
                        order by snippetid'
    
    __snipmptQuery = 'select concat(mptlessneg6," ",mptneg6toneg5," ",mptneg5toneg4," ",mptneg4toneg3," ",\
                    mptneg3toneg2," ",mptneg2to0," ",MPT0to2," ",MPT2to3," ",MPT3to4," ",MPT4to5," ",MPT5to6," ",MPTGreater6) from \
                    astprod.abstractsnippetmpt where documentid =%d order by snippetid'
                    
    __snipwordQuery = 'select nodeToWordMap from astprod.abstractsnippetnodetowordmap where documentid =%d \
                        order by snippetid'
    
    __snipvpdenormQuery = 'select isunspecified,isactuals,isguidance from astprod.abstractsnippetviewpointdenormalizedmap \
                            where documentid = %d order by snippetid'
    
    __qdpQuery = 'select DatapointType,PertainsToStart,PertainsToEnd,PeriodStart,PeriodEnd,ParameterId,NodeId,IsAbridged,CommentatorId \
                 from astprod.qualdatapoint where documentid = %d order by snippetid'
    
    __tqSelectQuery = 'select QuantValue,isNND,translation,trend,isRange,isApprox from astprod.textquant \
                        where documentid = %d order by sentenceid,quantid'
    
    __sentQuery = 'select Sentence,IsTableSentence,StemmedSentence,PorterStemmedSentence,ParsedSentence,AbstractSentence from \
                    astprod.manualsentences where documentid = %d order by sentenceid'
    
    __clauseQuery = 'select Clause,IsTableSentence,StemmedClause,PorterStemmedClause,ParsedClause,AbstractClause, \
                    clauseType,ismigrated from astprod.clauses where documentid = %d order by sentenceid,clauseid'
    
    __uifSelectQuery = 'select NotionId,PeriodStart,PeriodEnd,IsGuidance,\
                        PertainsToStart,PertainsToEnd from astprod.uifeedback where documentid = %d order by snippetid,notionid'
    
    qlTablesToCompare = [ ('astprod.abstractsentencebf',__sentbfQuery),\
                         ('astprod.abstractsentencempt',__sentmptQuery),\
                         ('astprod.abstractsentencenodetowordmap',__sentwordQuery),\
                         ('astprod.abstractsnippetbf',__snipbfQuery),\
                         ('astprod.abstractsnippetdesignationdenormalizedmap',__snipdesigdenormQuery),\
                         ('astprod.abstractsnippetiddesignationidmap',__snipdesigQuery),\
                         ('astprod.abstractsnippetidviewpointidgroupedmap',__snipvpQuery),\
                         ('astprod.abstractsnippetmpt',__snipmptQuery),\
                         ('astprod.abstractsnippetnodetowordmap',__snipwordQuery),\
                         ('astprod.abstractsnippetviewpointdenormalizedmap',__snipvpdenormQuery),\
                         ('astprod.qualdatapoint',__qdpQuery),\
                         ('astprod.textquant',__tqSelectQuery),\
                         ('astprod.clauses',__clauseQuery),\
                         ('astprod.uifeedback',__uifSelectQuery)]
    
    
    __tstagsQuery = 'select Nodes from astprod.abstracttablesentencesearchtags where \
                    documentid = %d order by TableSentenceId'
    
    __tsbfQuery = 'select concat(bflessneg6," ",bfneg6toneg5," ",bfneg5toneg4," ",bfneg4toneg3," ",bfneg3toneg2," ",bfneg2to0) from \
                 astprod.abstracttablesnippetbf where documentid = %d order by snippetid'
                 
    __tsmpQuery = 'select concat(mptlessneg6," ",mptneg6toneg5," ",mptneg5toneg4," ",mptneg4toneg3," ",mptneg3toneg2," ",mptneg2to0) \
                    from astprod.abstracttablesnippetmpt where documentid = %d order by snippetid'
    
    __tsQuery = 'select LineItem,rowNumber,columnNumber,CellValue,Unit,Currency,\
                Magnitude,TableSentence,GAAP,Consolidation,Proforma,ParamNature,Accuracy,Auditing,\
                DataPointType,Value,Nature,Consolidated,tableType,TablesentenceStemmed,Bookmark,\
                OriginalTableSentence,VirtualColumnNumber from astprod.tablesentence where documentid = %d \
                order by TableSentenceId'
    
    __tmpquery = 'select tmp.MasterParameterId,tmp.score from astprod.tablemp tmp,astprod.tablesentence ts \
                  where tmp.TableSentenceId = ts.TableSentenceId  and tmp.Score < 0 and ts.documentid = %d order by tmp.tablesentenceid,tmp.MasterParameterId'

    __tpQuery  = 'select tp.PeriodExpression,tp.AbstractPeriod, \
                    tp.PertainsToStart,tp.PertainsToEnd,tp.PeriodStart,tp.PeriodEnd, \
                    tp.isForwardPeriod,tp.isHistoricalPeriod,tp.isAsOfPeriod,tp.isOffsetable \
                    from astprod.tableperiod tp,astprod.tablesentence ts where tp.tablesentenceid = ts.tablesentenceid and \
                    ts.documentid = %d order by tp.periodid'
  
    __tbfSelectQuery = 'select tbf.NodeId,tbf.Score from astprod.tablebf tbf,\
                        astprod.tablesentence ts where tbf.tablesentenceid = ts.tablesentenceid and \
                        ts.documentid = %d  order by tbf.tablesentenceid,tbf.NodeId'

     
    __tarpSelectQuery = 'select tarp.MasterParameterId,tarp.score from astprod.tablearp tarp,\
                        astprod.tablesentence ts where tarp.tablesentenceid = ts.tablesentenceid and \
                        tarp.score < 0 and ts.documentid = %d  order by tarp.MasterParameterId,tarp.tablesentenceid , tarp.arpid'
                  
    qtTablesToCompare = [('astprod.abstracttablesentencesearchtags',__tstagsQuery),\
                         ('astprod.abstracttablesnippetbf',__tsbfQuery),\
                         ('astprod.abstracttablesnippetmpt',__tsmpQuery),\
                         ('astprod.tablesentence',__tsQuery),\
                         ('astprod.tablearp',__tarpSelectQuery),\
                         ('astprod.tablebf',__tbfSelectQuery),\
                         ('astprod.tableperiod',__tpQuery),\
                         ('astprod.tablemp',__tmpquery)]
    
    
    tablesToSplitAndSort = ['astprod.abstracttablesnippetmpt','astprod.abstracttablesentencesearchtags',\
                            'astprod.abstracttablesnippetbf','astprod.abstractsnippetmpt','astprod.abstractsnippetbf',\
                            'astprod.abstractsnippetnodetowordmap','astprod.abstractsentencebf',\
                            'astprod.abstractsentencempt','astprod.abstractsentencenodetowordmap']
    
    def splitAndSort(self,srcResult,destResult):
        tempSrc = []
        tempDest = []
        for row in srcResult:
            row = row[0]
            row = re.sub(self.reg,' ',row)
            tempr = row.strip()
            tempr = tempr.split(' ')
            tempr.sort()
            tempSrc.append(tempr)
        for row in destResult:
            row = row[0]
            row = re.sub(self.reg,' ',row)
            tempr = row.strip()
            tempr = tempr.split(' ')
            tempr.sort()
            tempDest.append(tempr)
        return tempSrc,tempDest
    
    def check(self,data1,data2):
        for row in data1:
            if row not in data2:
                return False
        return True
    
    def runTest(self,srcDoc,destDoc):
        print 'For documents '+srcDoc+' and '+destDoc + ':'
        for tableName,query in self.tablesToCompare:
            print 'Comparing : '+tableName + ' : ',
            srcQuery = query.replace('%d',srcDoc)
            destQuery = query.replace('%d',destDoc)
            srcResult = self.srcDb.selectQuery(srcQuery)
            destResult = self.destDb.selectQuery(destQuery)
            
            if tableName in self.tablesToSplitAndSort:
                srcResult,destResult = self.splitAndSort(srcResult, destResult)
 
            if not srcResult or not destResult:
                if not srcResult:
                    print 'Empty data for',srcDoc
                if not destResult:
                    print 'Empty data for',srcDoc
            elif self.check(srcResult,destResult):
                print 'OK'
                self.saveAsFile(srcDoc,tableName,srcResult)
                self.saveAsFile(destDoc,tableName,destResult)
            else:
                print 'Not matching'
                self.saveAsFile(srcDoc,tableName,srcResult)
                self.saveAsFile(destDoc,tableName,destResult)
                
if __name__ == '__main__':
    
    obj = GridTester('153','239','FULL','QL')
    docs = [('236277','236624')] #[('236561','236615'),('236560','236614')]#,('236278','236623'),('236512','236622'),('236620','236621'),('236617','236618'),]
    #[('236277','236624'),('236278','236623'),('236512','236622'),('236620','236621'),('236617','236618'),('236562','236616'),('236561','236615'),('236560','236614'),('236559','236613')]
    for sourceDoc,destDoc in docs:
        obj.runTest(sourceDoc,destDoc)
