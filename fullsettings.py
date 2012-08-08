class QualSettings:
    minDocLoad = 3
    sleepTime = 1
    maxDocsToCopy = 3
    maxDocsToWriteBack = 5
    minDocsToWriteBack = 4
    qualDocsLimit = 4
    useFtp = True
    
    __triggerDeleteQuery = 'delete from autoprocess.workflowstate_ap where documentid in %d'
    
    __triggerInsertQuery = 'insert into autoprocess.workflowstate_ap(documentid,actionitemid,state,pollerid) \
                   values(%s,1,"SUCCESS","Autoprocess")'
    
    triggerQueries = [__triggerInsertQuery,__triggerDeleteQuery]
    
    nodeStateQuery = 'select distinct documentid from autoprocess.workflowstate_ap where actionitemid = 1 \
                and documentid in %d'
    
    docsStatusQuery = 'select documentid,state from autoprocess.workflowstate_ap where ActionItemId = "2" \
                and (state = "SUCCESS" or state = "FAILED") and documentid in %d'
    
    __dptSelectQuery = 'select dpt.* from cap2.datapointtext dpt,cap2.datapoint dp \
                    where dpt.datapointid = dp.datapointid and dp.parameterid < 200 and dp.documentid in %d'
    __dptDeleteQuery = 'delete cap2.datapointtext from cap2.datapointtext,cap2.datapoint \
                    where cap2.datapointtext.datapointid = cap2.datapoint.datapointid \
                    and cap2.datapoint.documentid in %d'
    __dpSelectQuery = 'select * from cap2.datapoint where parameterid < 200 and documentid in %d'
    
    __companyeventSelectQuery = 'select ce.* from cap2.companyevent ce, cap2.document d \
                        where ce.companyid=d.companyid and ce.companyeventid=d.companyeventid and d.documentid in %d'

    __companyeventInsertQuery = 'insert ignore into cap2.companyevent values %v'

    __companyeventDeleteQuery = 'ignore'

    
    capTablesToCopy = [('cap2.companyevent',__companyeventSelectQuery,__companyeventInsertQuery,__companyeventDeleteQuery),\
                       'cap2.document','cap2.documentfile',('cap2.datapoint',__dpSelectQuery),'cap2.newsnippet',\
                       'cap2.newsentence','cap2.qlsnippetnode_ap',('cap2.datapointtext',__dptSelectQuery,__dptDeleteQuery)]                 
    
    __clausesSelectQuery = 'select SentenceId,Clause,DocumentId,IsTableSentence,StemmedClause,PorterStemmedClause,\
                           ParsedClause,AbstractClause,clauseType,ismigrated  from astprod.clauses where \
                           documentid in %d'
    
    __clausesInsertQuery = 'insert into astprod.clauses(SentenceId,Clause,DocumentId,IsTableSentence,StemmedClause,PorterStemmedClause,\
                           ParsedClause,AbstractClause,clauseType,ismigrated) values %v'
    
    __qdpSelectQuery = 'select SnippetId,DatapointType,PertainsToStart,PertainsToEnd,PeriodStart,PeriodEnd,ParameterId,\
                        NodeId,IsAbridged,DocumentId,CommentatorId from astprod.qualdatapoint where documentid in %d'
    
    __qdpInsertQuery = 'insert into astprod.qualdatapoint(SnippetId,DatapointType,PertainsToStart,PertainsToEnd,PeriodStart,PeriodEnd,ParameterId,\
                        NodeId,IsAbridged,DocumentId,CommentatorId) values %v'
    
    __cfSelectQuery = 'select CommentatorId,Bookmark,DocumentId,CommentatorName,Designation, \
                       SnippetId from astprod.commentatorfeedback where documentid in %d'
    
    __cfInsertQuery = 'insert in astprod.commentatorfeedback(CommentatorId,Bookmark,DocumentId,CommentatorName,Designation, \
                       SnippetId) values %v'
    
    __astdpSelectQuery = 'select PersonId,DocumentId,ParameterId,DataPointType,Value,RangeHi,Duplicate,\
                        PertainsToStart,PertainsToEnd,CreatedBy,CreatedTime,LastEditedBy,LastEditTime,Bookmark,Filename,\
                        PeriodEnd,PeriodStart,IsComplete,DeletedBy,DeletedTime,TableSnippetId,TableBookmark,QualifierID,\
                        IsAbridged,UnitId,GaapId,ConsolidationId,AuditingId,ProformaId,CurrencyId,ParamNatureId,MagnitudeId,\
                        StatementType,AccuracyId,PeriodConfidence,NotionConfidence,autoincrementdatapointid from \
                        astprod.astdatapoint where documentid in %d' 
                        
    __astdpInsertQuery = 'insert into astprod.astdatapoint(PersonId,DocumentId,ParameterId,DataPointType,\
                        Value,RangeHi,Duplicate,PertainsToStart,PertainsToEnd,CreatedBy,CreatedTime,LastEditedBy,LastEditTime,\
                        Bookmark,Filename,PeriodEnd,PeriodStart,IsComplete,DeletedBy,DeletedTime,TableSnippetId,TableBookmark,\
                        QualifierID,IsAbridged,UnitId,GaapId,ConsolidationId,AuditingId,ProformaId,CurrencyId,ParamNatureId,\
                        MagnitudeId,StatementType,AccuracyId,PeriodConfidence,NotionConfidence,autoincrementdatapointid) \
                        values %v'
    
    
    __coSelectQuery = 'select SnippetId,ClassifierTypeId,PredictedNotionId,Rank,Score,DocumentId,confidence \
                      from astprod.classifieroutput where documentid in %d'
    
    __coInsertQuery = 'insert into astprod.classifieroutput(SnippetId,ClassifierTypeId,PredictedNotionId,Rank,Score,DocumentId,\
                        confidence) values %v'
                        
    __fcSelectQuery = 'select SnippetId,NotionId,Rank,Score,DocumentId,ClassifierTypeId,Occurrence,confidence from \
                        astprod.finalclassification where documentid in %d'
                        
    __fcInsertQuery = 'insert into astprod.finalclassification (SnippetId,NotionId,Rank,Score,DocumentId,ClassifierTypeId,Occurrence,\
                        confidence) values %v'
                        
    __merSelectQuery = 'select snippetid,managemententityname,companyspecificdesignation,relationedgeid,status,tickerid,documentid,startdate,enddate,standarddesignation,Synonym,\
                        CommentatorID,snippettype from astprod.managemententityresolution where documentid in %d'
                        
    __merInsertQuery = 'insert into astprod.managemententityresolution(snippetid,managemententityname,\
                        companyspecificdesignation,relationedgeid,status,tickerid,documentid,startdate,\
                        enddate,standarddesignation,Synonym,CommentatorID,snippettype) values %v'
    
    
    __qadpSelectQuery = 'select PersonId,DocumentId,ParameterId,DataPointType,Value,RangeHi,Duplicate,PertainsToStart,\
                        PertainsToEnd,CreatedBy,CreatedTime,LastEditedBy,LastEditTime,Bookmark,Filename,PeriodEnd,\
                        PeriodStart,IsComplete,DeletedBy,DeletedTime,TableSnippetId,TableBookmark,QualifierID,IsAbridged,\
                        UnitId,GaapId,ConsolidationId,AuditingId,ProformaId,CurrencyId,ParamNatureId,MagnitudeId,StatementType,\
                        AccuracyId,SnippetId,Status,QALevel from astprod.qadatapoint where documentid in %d'
    
    __qadpInsertQuery = 'insert into astprod.qadatapoint(PersonId,DocumentId,ParameterId,DataPointType,Value,RangeHi,Duplicate,PertainsToStart,\
                        PertainsToEnd,CreatedBy,CreatedTime,LastEditedBy,LastEditTime,Bookmark,Filename,PeriodEnd,\
                        PeriodStart,IsComplete,DeletedBy,DeletedTime,TableSnippetId,TableBookmark,QualifierID,IsAbridged,\
                        UnitId,GaapId,ConsolidationId,AuditingId,ProformaId,CurrencyId,ParamNatureId,MagnitudeId,StatementType,\
                        AccuracyId,SnippetId,Status,QALevel) values %v'
    
    __qasSelectQuery = 'select SnippetId,Bookmark,DocumentId,isauto from astprod.qasnippets where documentid in %d'
    
    __qasInsertQuery = 'insert into astprod.qasnippets(SnippetId,Bookmark,DocumentId,isauto) values %v'
    
    __ssSelectQuery = 'select SentenceId,DocumentId,NumberOfQuants,NumberOfPeriods,isCauseEffect,isComparative,\
                        isTrendIndicating,isForwardLooking,isSentiment,colorValue,hasOpsUnit,isGuidance,\
                        hasForwardPeriod,isCautionPhrase,indefiniteCauseEffect,reconstitutionvalue from \
                        astprod.sentencesummary where documentid in %d'
                        
    __ssInsertQuery = 'insert into astprod.sentencesummary(SentenceId,DocumentId,NumberOfQuants,NumberOfPeriods,isCauseEffect,isComparative,\
                        isTrendIndicating,isForwardLooking,isSentiment,colorValue,hasOpsUnit,isGuidance,\
                        hasForwardPeriod,isCautionPhrase,indefiniteCauseEffect,reconstitutionvalue) values %v'
                        
    __scSelectQuery = 'select snippetid,commentatorname,tickerid,documentid from astprod.Snippetcommentator \
                        where documentid in %d'
                        
    __scInsertQuery = 'insert into astprod.Snippetcommentator(snippetid,commentatorname,tickerid,documentid) \
                        values %v'
                        
    __tpSelectQuery = 'select SentenceId,PeriodExpression,AbstractPeriod,PertainsToStart,\
                        PertainsToEnd,PeriodEnd,PeriodStart,isForwardPeriod,isHistoricalPeriod,\
                        isAsOfPeriod,documentId from astprod.textperiod where documentid in %d'
                        
    __tpInsertQuery = 'insert into astprod.textperiod(SentenceId,PeriodExpression,AbstractPeriod,PertainsToStart,\
                        PertainsToEnd,PeriodEnd,PeriodStart,isForwardPeriod,isHistoricalPeriod,\
                        isAsOfPeriod,documentId) values %v'
    
    __tqSelectQuery = 'select SentenceId,QuantValue,documentId,isNND,translation,trend,isRange,isApprox \
                       from astprod.textquant where documentid in %d'
                       
    __tqInsertQuery = 'insert into astprod.textquant(SentenceId,QuantValue,documentId,isNND,translation,\
                        trend,isRange,isApprox) values %v'
    
    __rbsSelectQuery = 'select SentenceId,NodeId,DocumentId,IsNotion,MPTDistance,IsBestFitNode,IsManualNotion,\
                        IsCompanyParameter,ModifiedTime,clauseid,words,IsBestFitMptNode from astprod.manualrbstags \
                        where documentid in %d'
    
    __rbsInsertQuery = 'insert into astprod.manualrbstags(SentenceId,NodeId,DocumentId,IsNotion,MPTDistance,IsBestFitNode,IsManualNotion,\
                        IsCompanyParameter,ModifiedTime,clauseid,words,IsBestFitMptNode) values %v'
    
    __qldpSelectQuery = 'select SnippetId,DatapointType,PertainsToStart,PertainsToEnd,PeriodStart,PeriodEnd,\
                        ParameterId,NodeId,IsAbridged,DocumentId,CommentatorId from astprod.qualdatapoint where documentid \
                        in %d'
    
    __qldpInsertQuery = 'insert into astprod.qualdatapoint(SnippetId,DatapointType,PertainsToStart,PertainsToEnd,PeriodStart,PeriodEnd,\
                        ParameterId,NodeId,IsAbridged,DocumentId,CommentatorId) values %v'
    
    __snippetSelectQuery = 'select SnippetId,BookMark,DocumentId,"" from astprod.snippet where documentid in %d'
    
    __uifSelectQuery = 'select SnippetId,DocumentId,DatapointId,Bookmark,NotionId,PeriodStart,PeriodEnd,IsGuidance,Source,\
                        PertainsToStart,PertainsToEnd,User from astprod.uifeedback where documentid in %d'
    
    __uifInsertQuery = 'insert into astprod.uifeedback(SnippetId,DocumentId,DatapointId,Bookmark,NotionId,PeriodStart,PeriodEnd,IsGuidance,Source,\
                        PertainsToStart,PertainsToEnd,User) values %v'
    
    fullAstTablesToCopy = ['astprod.abstractsentencebf','astprod.abstractsentencempt',\
                             'astprod.abstractsentencenodetowordmap','astprod.abstractsnippetbf',\
                             'astprod.abstractsnippetdesignationdenormalizedmap','astprod.abstractsnippetiddesignationidmap',\
                             'astprod.abstractsnippetidviewpointidgroupedmap','astprod.abstractsnippetmpt',\
                             'astprod.abstractsnippetnodetowordmap','astprod.abstractsnippetviewpointdenormalizedmap',\
                             'astprod.downloadperformance',('astprod.clauses',__clausesSelectQuery,__clausesInsertQuery),
                             ('astprod.qualdatapoint',__qdpSelectQuery,__qdpInsertQuery),\
                             ('astprod.commentatorfeedback',__cfSelectQuery,__cfInsertQuery),\
                             ('astprod.uifeedback',__uifSelectQuery,__uifInsertQuery),\
                             'astprod.completeddocuments','astprod.confirmedtablesnippets',('astprod.snippet',__snippetSelectQuery),\
                             'astprod.astdocument',('astprod.astdatapoint',__astdpSelectQuery,__astdpInsertQuery),\
                             ('astprod.classifieroutput',__coSelectQuery,__coInsertQuery),\
                             ('astprod.finalclassification',__fcSelectQuery,__fcInsertQuery),\
                             ('astprod.managemententityresolution',__merSelectQuery,__merInsertQuery),\
                             ('astprod.qadatapoint',__qadpSelectQuery,__qadpInsertQuery),\
                             ('astprod.qasnippets',__qasSelectQuery,__qasInsertQuery),\
                             ('astprod.sentencesummary',__ssSelectQuery,__ssInsertQuery),\
                             ('astprod.Snippetcommentator',__scSelectQuery,__scInsertQuery),'astprod.snippetrisk',\
                             ('astprod.textperiod',__tpSelectQuery,__tpInsertQuery),\
                             ('astprod.textquant',__tqSelectQuery,__tqInsertQuery),\
                             ('astprod.qualdatapoint',__qldpSelectQuery,__qldpInsertQuery)]
    
    debugAstTablesToCopy = ['astprod.downloadperformance']
    fullCapTablesToCopy = []
    debugCapTablesToCopy = []
    tablesToClear = ['astprod.manualrbstags','astprod.snippets','astprod.originalsnippet']
    
class QuantSettings:
    
    minDocLoad = 20
    sleepTime = .1
    maxDocsToCopy = 50
    maxDocsToWriteBack = 250
    minDocsToWriteBack = 250
    quantDocsLimit = 50
    useFtp = True
    
    __triggerDeleteQuery = 'delete from cap2.tableparsingqueue_ap where documentid in %d'
    
    __triggerInsertQuery = 'insert into cap2.tableparsingqueue_ap(documentid,status,tableextractionpoller) \
                   values(%s,"PENDING","None")'
    
    triggerQueries = [__triggerInsertQuery,__triggerDeleteQuery]
    
    nodeStateQuery = 'select distinct documentid from cap2.tableparsingqueue_ap where documentid in %d'
    
    docsStatusQuery = 'select documentid,status from cap2.tableparsingqueue_ap where (status = "FAILED" or status = "SUCCESS") \
                 and documentid in %d'
    
    __tableContextSelectQuery = 'select tc.* from autoprocess.tablecontext tc,autoprocess.tablesnippet ts where tc.tableid = \
                         ts.tableid and ts.documentid in %d'
                         
    __tableContextDeleteQuery = 'delete autoprocess.tablecontext from autoprocess.tablecontext,autoprocess.tablesnippet \
                                where autoprocess.tablecontext.tableid = autoprocess.tablesnippet.tableid and \
                                autoprocess.tablesnippet.documentid in %d'
                                
    __tableFootNoteSelectQuery = 'select tf.* from autoprocess.tablefootnote tf,autoprocess.tablesnippet ts where \
                                    tf.tableid = ts.tableid and ts.documentid in %d'
    
    __tableFootNoteDeleteQuery = 'delete autoprocess.tablefootnote from autoprocess.tablefootnote,autoprocess.tablesnippet \
                                 where autoprocess.tablefootnote.tableid = autoprocess.tablesnippet.tableid and \
                                 autoprocess.tablesnippet.documentid in %d'
    
    capTablesToCopy = ['cap2.document','autoprocess.tablesnippet','cap2.qtsnippetnode_ap',\
                    ('autoprocess.tablecontext',__tableContextSelectQuery,__tableContextDeleteQuery),\
                    'autoprocess.tablematchnotconfirmed','autoprocess.tablematchconfirmed',\
                    ('autoprocess.tablefootnote',__tableFootNoteSelectQuery,__tableFootNoteDeleteQuery),\
                    'cap2.autoprocessstate']
    
    fullAstTablesToCopy = ['astprod.abstracttablesnippetmpt','astprod.abstracttablesnippetbf',\
                            ]
    
    debugAstTablesToCopy = []
    
    __tableSentenceSelectQuery = 'select TableSentenceId,TableId,LineItem,rowNumber,columnNumber,\
                                CellValue,Unit,Currency,Magnitude,DocumentId,TableSentence,GAAP,Consolidation,Proforma,\
                                ParamNature,Accuracy,Auditing,DataPointType,Value,Nature,Consolidated,tableType,\
                                TablesentenceStemmed,Bookmark,OriginalTableSentence,VirtualColumnNumber from \
                                astprod.TableSentence where documentid in %d'
 
    __tableSentenceInsertQuery = 'insert into astprod.TableSentence(SlaveTableSentenceId,TableId,LineItem,rowNumber,columnNumber,\
                                CellValue,Unit,Currency,Magnitude,DocumentId,TableSentence,GAAP,Consolidation,Proforma,\
                                ParamNature,Accuracy,Auditing,DataPointType,Value,Nature,Consolidated,tableType,\
                                TablesentenceStemmed,Bookmark,OriginalTableSentence,VirtualColumnNumber) values %v'
    tableSentenceQueries = ('astprod.tablesentence',__tableSentenceSelectQuery,__tableSentenceInsertQuery)
    
    __tbfSelectQuery = 'select tbf.TableSentenceId,tbf.NodeId,tbf.Score from astprod.tablebf tbf,\
                        astprod.tablesentence ts where tbf.tablesentenceid = ts.tablesentenceid and \
                        ts.documentid in %d'
                        
    __tbfInsertQuery = 'insert into astprod.tablebf(TableSentenceId,NodeId,Score) values %v'
    
    __tbfDeleteQuery = 'delete astprod.tablebf from astprod.tablebf,astprod.tablesentence where \
                        astprod.tablebf.tablesentenceid = astprod.tablesentence.tablesentenceid \
                        and astprod.tablesentence.documentid in %d'
    
    __tmpSelectQuery = 'select tmp.TableSentenceId,tmp.MasterParameterId,tmp.Score from astprod.tablemp tmp,\
                        astprod.tablesentence ts where tmp.tablesentenceid = ts.tablesentenceid and \
                        ts.documentid in %d'
    __tmpInsertQuery = 'insert into astprod.tablemp(TableSentenceId,MasterParameterId,Score) values %v'
    
    __tmpDeleteQuery = 'delete astprod.tablemp from astprod.tablemp,astprod.tablesentence where \
                        astprod.tablemp.tablesentenceid = astprod.tablesentence.tablesentenceid \
                        and astprod.tablesentence.documentid in %d'
    
    __tpSelectQuery = 'select tp.TableSentenceId,tp.PeriodExpression,tp.AbstractPeriod, \
                    tp.PertainsToStart,tp.PertainsToEnd,tp.PeriodStart,tp.PeriodEnd, \
                    tp.isForwardPeriod,tp.isHistoricalPeriod,tp.isAsOfPeriod,tp.isOffsetable \
                    from astprod.tableperiod tp,astprod.tablesentence ts where tp.tablesentenceid = ts.tablesentenceid and \
                    ts.documentid in %d'
    
    __tpInsertQuery = 'insert into astprod.tableperiod(TableSentenceId,PeriodExpression,AbstractPeriod, \
                    PertainsToStart,PertainsToEnd,PeriodStart,PeriodEnd,isForwardPeriod,isHistoricalPeriod, \
                    isAsOfPeriod,isOffsetable) values %v'
                    
    __tpDeleteQuery = 'delete astprod.tableperiod from astprod.tableperiod,astprod.tablesentence where \
                        astprod.tableperiod.tablesentenceid = astprod.tablesentence.tablesentenceid \
                        and astprod.tablesentence.documentid in %d'
    
    __tarpSelectQuery =  'select tarp.tablesentenceid,tarp.arpid,tarp.Score,tarp.MasterParameterId from astprod.tablearp tarp,\
                        astprod.tablesentence ts where tarp.tablesentenceid = ts.tablesentenceid and \
                        ts.documentid in %d'

    __tarpInsertQuery = 'insert into astprod.tablearp(TableSentenceId,ARPId,Score,MasterParameterId) \
                        values %v'
    __tarpDeleteQuery = 'delete astprod.tablearp from astprod.tablearp,astprod.tablesentence where \
                        astprod.tablearp.tablesentenceid = astprod.tablesentence.tablesentenceid \
                        and astprod.tablesentence.documentid in %d'

    tablesToTransform = [('astprod.tablearp',__tarpSelectQuery,__tarpInsertQuery,__tarpDeleteQuery),\
                         ('astprod.tablebf',__tbfSelectQuery,__tbfInsertQuery,__tbfDeleteQuery),\
                         ('astprod.tablemp',__tmpSelectQuery,__tmpInsertQuery,__tmpDeleteQuery),\
                         ('astprod.tableperiod',__tpSelectQuery,__tpInsertQuery,__tpDeleteQuery),\
                         'astprod.abstracttablesentencesearchtags']
    
    __apsSelectQuery = 'select documentid,TableParsing,PeriodIdentification,\
                 ParameterIdentification,PeriodTranslation,AttributeExtraction,TableOldDataDeletion \
                 from cap2.autoprocessstate where documentid in %d'
    
    __apsInsertQuery = 'insert into cap2.autoprocessstate values %v on duplicate key \
                update TableParsing = VALUES(TableParsing) , PeriodIdentification = \
                VALUES(PeriodIdentification) , PeriodResolution = VALUES(PeriodResolution) ,\
                ParameterIdentification = VALUES(ParameterIdentification) , PeriodTranslation = \
                VALUES(PeriodTranslation) , AttributeExtraction = VALUES(AttributeExtraction),\
                TableOldDataDeletion=VALUES(TableOldDataDeletion)'
    
    __apsDeleteQuery = 'ignore'
    
    autoProcessStateQueries = ('cap2.autoprocessstate',__apsSelectQuery,__apsInsertQuery,__apsDeleteQuery)
    
    fullCapTablesToCopy = []
    debugCapTablesToCopy = []
    tablesToClear = []

class GridSettings:
    #nodes = ['192.168.1.245','192.168.1.232','192.168.1.250','192.168.1.180']
    qualNodes = [] #
    quantNodes = ['10.25.0.119']#,'192.168.1.180','192.168.1.250']
    sleepTime = 1
    ip = '192.168.1.203'
    name = 'GridManager:QL'
    ftpPort,ftpUsr,ftpPwd = '21','SYSAUTO','qwdfvb'
    
    qualDocsQuery = 'select documentid  from autoprocess.workflowstate_ap where (actionitemid in (1) and state ="success")\
                 OR actionitemid = 2 group by documentid having  sum(case when actionitemid = 2 then 1 else 0 end) \
                 = 0 and sum(case when actionitemid in(1) then 1 else 0 end) = 1 order by timestamp limit %l'
    
    quantDocsQuery = 'select documentid from cap2.tableparsingqueue_ap where status = "PENDING" \
                    and tableextractionpoller = "None"  order by documentid limit %l'
                     

    
    