import getopt,sys,time
import os
from FtpNodeComponent import FtpNodeComponent

QualSettings = None
class QualFtpComponent(FtpNodeComponent):
    
    def __init__(self,localCapDb,localAstDb,nodeServerDetails,QualSettings):
        FtpNodeComponent.__init__(self, localCapDb, localAstDb,nodeServerDetails,QualSettings)