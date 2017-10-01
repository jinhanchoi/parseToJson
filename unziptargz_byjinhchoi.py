import os
import tarfile
import multiprocessing
from multiprocessing import current_process
def searchTarget():
    targetList = []
    for (path, dir, files) in os.walk("./"):
        for filename in files:
            ext = os.path.splitext(filename)[-1]
            if ext == '.gz':
                 targetList.append(filename)
    
    return targetList

def tarUnzip(filename):
    proc_name = current_process().name 
    print('{0} start unzip {1} by: {2}'.format( filename, "start", proc_name))
    tar = tarfile.open(filename)
    tar.extractall("./results")
    tar.close()
    print('{0} end unzip {1} by: {2}'.format( filename, "start", proc_name))
    
def convertToJson(filename):
    proc_name = current_process().name
    print('{0} start convert to json by: {1}').format(filename, proc_name))
    with open(filename,"r") as origFile:
        lines = origFile.readlines()
        for line in lines:
            if(line != ""):
                header = line[:73]
                body = line[73:]
                print(header)
                print(body)
                
def checkResultDirAndCreate():
    if not os.path.isdir('./results'):
        os.mkdir('./results')

if __name__ == '__main__': 
    targetList = searchTarget()
    print("total targets is  %s" %len(targetList))
    checkResultDirAndCreate()
    pool = multiprocessing.Pool(min(multiprocessing.cpu_count(),len(targetList)))
    pool.map(convertToJson, targetList, chunksize=1)
    pool.close()