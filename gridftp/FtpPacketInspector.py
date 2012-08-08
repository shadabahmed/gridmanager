import gzip,cPickle,re

fileName = "C:\\Documents and Settings\\shadab.ahmed\\Desktop\\6115cdaaf071a3b677510c4e9995df0e.gru"

fp = gzip.open(fileName,'rb')
packet = cPickle.load(fp)
fp.close()

def printChunk(packet,x=''):
    if getattr(packet, '__iter__', False):
        for chunk in packet:
            printChunk(chunk,x+'\t')
    else:
        print x+re.sub('[\s]+',' ',str(packet))
    
printChunk(packet)

    