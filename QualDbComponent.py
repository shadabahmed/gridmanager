import getopt,sys,time
import os
from DbNodeComponent import DbNodeComponent

QualSettings = None
class QualDbComponent(DbNodeComponent):
    
    def __init__(self,localCapDb,localAstDb,nodeServerDetails,QualSettings):
        self.dbName = 'autoprocess'
        DbNodeComponent.__init__(self, localCapDb, localAstDb, nodeServerDetails,QualSettings)