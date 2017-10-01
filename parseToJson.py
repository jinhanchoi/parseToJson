import os
import tarfile
import json
import multiprocessing
from multiprocessing import current_process
def searchTarget():
    targetList = []
    for (path, dir, files) in os.walk("./"):
        for filename in files:
            ext = os.path.splitext(filename)[-1]
            if ext == '.TXT':
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
    print('{0} start convert to json by: {1}'.format(filename, proc_name))
    with open(filename,"r",encoding="EUC-KR") as origFile:
        lines = origFile.readlines()
        for line in lines:
            if(line != "\n"):
                lineByteArray = bytearray()
                lineByteArray.extend(line.encode("EUC-KR"))
                header = lineByteArray[:73]
                body = lineByteArray[73:]
                headerData = headerToJson(header)
    
                bodyToJson(headerData['opCode'],body)

                headerJson = json.dumps(headerData)


def headerToJson(header):
    #print(header.decode("EUC-KR"))
    headerData = {}
    headerData['opCode'] = header[:2].decode("EUC-KR")
    headerData['dataLength'] = header[2:8].decode("EUC-KR")
    headerData['seqNum'] = header[8:14].decode("EUC-KR")
    headerData['carSerial'] = header[14:31].decode("EUC-KR")
    headerData['carType'] = header[31:33].decode("EUC-KR")
    headerData['carRegNum'] = header[33:45].decode("EUC-KR")
    headerData['businessNum'] = header[45:55].decode("EUC-KR")
    headerData['driverCode'] = header[55:74].decode("EUC-KR")
    return headerData

def bodyToJson(opcode,body):
    #print(header.decode("EUC-KR"))
    bodyData = {}
    if(opcode == "01"):
        makeAccumDataJson(body)
    elif(opcode == "02"):
        makeBusinessDataJson(body)
    elif(opcode == "03"):
        makeEngineDataJson(body)
    elif(opcode == "04"):
        makeSettingDataJson(body)
    elif(opcode == "05"):
        makeBtnDataJson(body)
    elif(opcode == "70"):
        makeDriveDataJson(body)
    #print(headerData)
    # json_data = json.dumps(bodyData)
    #print(json_data)
    # return json_data

def makeAccumDataJson(body):
    print("a")
def makeBusinessDataJson(body):
    print("b")
def makeEngineDataJson(body):
    print("e") 
def makeSettingDataJson(body):
    print("s")
def makeBtnDataJson(body):
    print("nn")
def makeDriveDataJson(body):
    print("d")


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