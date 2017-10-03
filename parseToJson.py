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
        filenameWithoutExt = os.path.splitext(filename)[0]
        results = []
        with open("./results/"+filenameWithoutExt+".json",'w',encoding="EUC-KR") as newFile:
            for line in lines:
                if(line != "\n"):
                    data = {}

                    lineByteArray = bytearray()
                    lineByteArray.extend(line.encode("EUC-KR"))
                    
                    header = lineByteArray[:73]
                    body = lineByteArray[73:]

                    headerData = headerToJson(header)
                    bodyData = bodyToJson(headerData['opCode'],body)
                    headerData.update(bodyData)
                    # print(headerData)
                    data = headerData
                    # data['header'] = headerData
                    # data['body'] = bodyData

                    # headerJson = json.dumps(headerData)
                    # bodyJson = json.dumps(bodyData)
                    results.append(data)
            newFile.write(json.dumps(results))


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
    if(opcode == "01"):
        return makeAccumDataJson(body)
    elif(opcode == "02"):
        return makeBusinessDataJson(body)
    elif(opcode == "03"):
        return makeEngineDataJson(body)
    elif(opcode == "04"):
        return makeSettingDataJson(body)
    elif(opcode == "05"):
        return makeBtnDataJson(body)
    elif(opcode == "70"):
        return makeDriveDataJson(body)

def makeAccumDataJson(body):
    accumData = {}
    accumData['inDateTime'] = body[:14].decode("EUC-KR")
    accumData['outDateTime'] = body[14:28].decode("EUC-KR")
    accumData['drivingDistance'] = body[28:35].decode("EUC-KR")
    accumData['chargeDistance'] = body[35:42].decode("EUC-KR")
    accumData['customerCnt'] = body[42:46].decode("EUC-KR")
    accumData['totalIncome'] = body[46:53].decode("EUC-KR")
    accumData['overChargeCnt'] = body[53:57].decode("EUC-KR")
    return accumData

def makeBusinessDataJson(body):
    businessData = {}
    businessData['paymentDateTime'] = body[:14].decode("EUC-KR")
    businessData['totalPrice'] = body[14:20].decode("EUC-KR")
    businessData['callPrice'] = body[20:24].decode("EUC-KR")
    businessData['extraPrice'] = body[24:30].decode("EUC-KR")
    businessData['overChargeType'] = body[30:31].decode("EUC-KR")
    businessData['getInDateTime'] = body[31:45].decode("EUC-KR")
    businessData['getInX'] = body[45:54].decode("EUC-KR")
    businessData['getInY'] = body[54:63].decode("EUC-KR")
    businessData['getOutDateTime'] = body[63:77].decode("EUC-KR")
    businessData['getOutX'] = body[77:86].decode("EUC-KR")
    businessData['getOutY'] = body[86:95].decode("EUC-KR")
    businessData['rideDistance'] = body[95:102].decode("EUC-KR")
    businessData['emptyDistance'] = body[102:108].decode("EUC-KR")
    return businessData

def makeEngineDataJson(body):
    engineData = {}
    engineData['paymentDateTime'] = body[:14].decode("EUC-KR")
    engineData['totalPrice'] = body[14:15].decode("EUC-KR")
    return engineData

def makeSettingDataJson(body):
    settingData = {}
    settingData['changeDateTime'] = body[:14].decode("EUC-KR")
    settingData['slowdownLate1'] = body[14:19].decode("EUC-KR")
    settingData['basicPrice1'] = body[19:25].decode("EUC-KR")
    settingData['afterPrice1'] = body[25:30].decode("EUC-KR")
    settingData['basicOverPrice1'] = body[30:36].decode("EUC-KR")
    settingData['afterOverPrice1'] = body[36:41].decode("EUC-KR")
    settingData['basicDistance1'] = body[41:46].decode("EUC-KR")
    settingData['afterDistance1'] = body[46:51].decode("EUC-KR")
    settingData['slowdownLate2'] = body[51:56].decode("EUC-KR")
    settingData['basicPrice2'] = body[56:62].decode("EUC-KR")
    settingData['afterPrice2'] = body[62:67].decode("EUC-KR")
    settingData['basicOverPrice1'] = body[67:73].decode("EUC-KR")
    settingData['afterOverPrice2'] = body[73:78].decode("EUC-KR")
    settingData['basicDistance2'] = body[78:83].decode("EUC-KR")
    settingData['afterDistance2'] = body[83:88].decode("EUC-KR")
    return settingData

def makeBtnDataJson(body):
    btnData = {}
    btnData['actionDateTime'] = body[:14].decode("EUC-KR")
    btnData['btnType'] = body[14:16].decode("EUC-KR")
    btnData['btnStatus'] = body[16:17].decode("EUC-KR")
    btnData['carPositionX'] = body[17:26].decode("EUC-KR")
    btnData['carPositionY'] = body[26:35].decode("EUC-KR")

    return btnData

def makeDriveDataJson(body):
    dtgHeaderLength= 39
    dtgSingleBodyLength = 68
    dtgDatas = {}
    headerOrig = body[:39]
    bodyOrig = body[39:]
    bodyCnt = int((len(bodyOrig)-1)/dtgSingleBodyLength)

    dtgHeader = {}
    dtgBodys = []

    dtgHeader['deviceModel'] = headerOrig[:20].decode("EUC-KR")
    dtgHeader['TripSeq'] = headerOrig[20:34].decode("EUC-KR")
    dtgHeader['dtgCode'] = headerOrig[34:36].decode("EUC-KR")
    dtgHeader['dtgCnt'] = headerOrig[36:39].decode("EUC-KR")

    for i in range(1,bodyCnt):
        dtgBody = {}
        startIdx = int((i*68) - 68)
        endIdx = int(startIdx + 68)
        targetBody = bodyOrig[startIdx:endIdx]
        dtgBody['dailyDistance'] = targetBody[:4].decode("EUC-KR")
        dtgBody['accumDistance'] = targetBody[4:11].decode("EUC-KR")
        dtgBody['evnetDateTime'] = targetBody[11:25].decode("EUC-KR")
        dtgBody['speed'] = targetBody[25:28].decode("EUC-KR")
        dtgBody['rpm'] = targetBody[28:32].decode("EUC-KR")
        dtgBody['breakOnOff'] = targetBody[32:33].decode("EUC-KR")
        dtgBody['carPostionX'] = targetBody[33:42].decode("EUC-KR")
        dtgBody['carPostionY'] = targetBody[42:51].decode("EUC-KR")
        dtgBody['azimuth'] = targetBody[51:54].decode("EUC-KR")
        dtgBody['accVx'] = targetBody[54:60].decode("EUC-KR")
        dtgBody['accVx'] = targetBody[60:66].decode("EUC-KR")
        dtgBody['deviceStatus'] = targetBody[66:68].decode("EUC-KR")
        dtgBodys.append(dtgBody)
    
    dtgDatas['dtgHeader'] = dtgHeader
    dtgDatas['dtgBody'] = dtgBodys
    # print(dtgDatas)
    return dtgDatas


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