import os
import sys
import tarfile
import csv
import time
from functools import partial
import copy
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
def get_csv_writer(filename, rows, delimiter): 
    with open(filename, 'w') as csvfile: 
        fieldnames = rows[0].keys() 
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=delimiter) 
        writer.writeheader() 
        for row in rows: 
            try: writer.writerow(row) 
            except Exception as detail: 
                print(detail)

def convertToCSV(filename,options):
    proc_name = current_process().name
    print('{0} start convert to csv by: {1}'.format(filename, proc_name))
    with open(filename,"r",encoding="EUC-KR") as origFile:
        try:
            lines = origFile.readlines()
            filenameWithoutExt = os.path.splitext(filename)[0]
            results = []
            for line in lines:
                if(line != "\n"):
                    data = {}

                    lineByteArray = bytearray()
                    lineByteArray.extend(line.encode("EUC-KR"))
                    
                    header = lineByteArray[:73]
                    headerData = headerToDict(header)
                
                    if(headerData['opCode'] in options):
                        body = lineByteArray[73:]
                        bodyData = bodyToDict(headerData['opCode'],body,options)
                        try:
                            for body in bodyData:
                                data = {}
                                data.update(headerData)
                                data.update(body)
                                results.append(data)
                        except Exception as detail:
                            print(bodyData)
            filenameWithoutExt = "events/"+filenameWithoutExt+'_events' if "70" not in options else "dtg/"+filenameWithoutExt+'_dtg'
            if results:
                get_csv_writer("./results/"+filenameWithoutExt+".csv",results,',')
        except Exception as detail:
            print(filename)
def getDefaultDataFormat(options):
    data = {}
    if "70" not in options:
        data['inDateTime'] = ""
        data['outDateTime'] = ""
        data['drivingDistance'] = ""
        data['chargeDistance'] = ""
        data['customerCnt'] = ""
        data['totalIncome'] = ""
        data['overChargeCnt'] = ""

        data['paymentDateTime'] = ""
        data['totalPrice'] = ""
        data['callPrice'] = ""
        data['extraPrice'] = ""
        data['overChargeType'] = ""
        data['getInDateTime'] = ""
        data['getInX'] = ""
        data['getInY'] = ""
        data['getOutDateTime'] = ""
        data['getOutX'] = ""
        data['getOutY'] = ""
        data['rideDistance'] = ""
        data['emptyDistance'] = ""

        data['engineStartDateTime'] = ""
        data['engineOnOff'] = ""

        data['changeDateTime'] = ""
        data['slowdownLate1'] = ""
        data['basicPrice1'] = ""
        data['afterPrice1'] = ""
        data['basicOverPrice1'] = ""
        data['afterOverPrice1'] = ""
        data['basicDistance1'] = ""
        data['afterDistance1'] = ""
        data['slowdownLate2'] = ""
        data['basicPrice2'] = ""
        data['afterPrice2'] = ""
        data['basicOverPrice1'] = ""
        data['afterOverPrice2'] = ""
        data['basicDistance2'] = ""
        data['afterDistance2'] = ""

        data['actionDateTime'] = ""
        data['btnType'] = ""
        data['btnStatus'] = ""
        data['carPositionX'] = ""
        data['carPositionY'] = ""
    else:
        data['deviceModel'] = ""
        data['TripSeq'] = ""
        data['dtgCode'] = ""
        data['dtgCnt'] = ""
        data['dailyDistance'] = ""
        data['accumDistance'] = ""
        data['eventDateTime'] = ""
        data['speed'] = ""
        data['rpm'] = ""
        data['breakOnOff'] = ""
        data['carPostionX'] = ""
        data['carPostionY'] = ""
        data['azimuth'] = ""
        data['accVx'] = ""
        data['accVy'] = ""
        data['deviceStatus'] = ""
    return data


def headerToDict(header):
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

def bodyToDict(opcode,body,options):

    data = getDefaultDataFormat(options)
   
    if(opcode == "01"):
        return makeAccumDataDict(body, data)
    elif(opcode == "02"):
        return makeBusinessDataDict(body, data)
    elif(opcode == "03"):
        return makeEngineDataDict(body, data)
    elif(opcode == "04"):
        return makeSettingDataDict(body, data)
    elif(opcode == "05"):
        return makeBtnDataDict(body, data)
    elif(opcode == "70"):
        return makeDriveDataDict(body, data)

def makeAccumDataDict(body,data):
    results=[]
    data['inDateTime'] = body[:14].decode("EUC-KR")
    data['outDateTime'] = body[14:28].decode("EUC-KR")
    data['drivingDistance'] = body[28:35].decode("EUC-KR")
    data['chargeDistance'] = body[35:42].decode("EUC-KR")
    data['customerCnt'] = body[42:46].decode("EUC-KR")
    data['totalIncome'] = body[46:53].decode("EUC-KR")
    data['overChargeCnt'] = body[53:57].decode("EUC-KR")
    results.append(data)
    return results

def makeBusinessDataDict(body,data):
    results=[]
    data['paymentDateTime'] = body[:14].decode("EUC-KR")
    data['totalPrice'] = body[14:20].decode("EUC-KR")
    data['callPrice'] = body[20:24].decode("EUC-KR")
    data['extraPrice'] = body[24:30].decode("EUC-KR")
    data['overChargeType'] = body[30:31].decode("EUC-KR")
    data['getInDateTime'] = body[31:45].decode("EUC-KR")
    data['getInX'] = body[45:54].decode("EUC-KR")
    data['getInY'] = body[54:63].decode("EUC-KR")
    data['getOutDateTime'] = body[63:77].decode("EUC-KR")
    data['getOutX'] = body[77:86].decode("EUC-KR")
    data['getOutY'] = body[86:95].decode("EUC-KR")
    data['rideDistance'] = body[95:102].decode("EUC-KR")
    data['emptyDistance'] = body[102:108].decode("EUC-KR")
    results.append(data)
    return results

def makeEngineDataDict(body,data):
    results=[]
    data['engineStartDateTime'] = body[:14].decode("EUC-KR")
    data['engineOnOff'] = body[14:15].decode("EUC-KR")
    results.append(data)
    return results

def makeSettingDataDict(body,data):
    results=[]
    data['changeDateTime'] = body[:14].decode("EUC-KR")
    data['slowdownLate1'] = body[14:19].decode("EUC-KR")
    data['basicPrice1'] = body[19:25].decode("EUC-KR")
    data['afterPrice1'] = body[25:30].decode("EUC-KR")
    data['basicOverPrice1'] = body[30:36].decode("EUC-KR")
    data['afterOverPrice1'] = body[36:41].decode("EUC-KR")
    data['basicDistance1'] = body[41:46].decode("EUC-KR")
    data['afterDistance1'] = body[46:51].decode("EUC-KR")
    data['slowdownLate2'] = body[51:56].decode("EUC-KR")
    data['basicPrice2'] = body[56:62].decode("EUC-KR")
    data['afterPrice2'] = body[62:67].decode("EUC-KR")
    data['basicOverPrice1'] = body[67:73].decode("EUC-KR")
    data['afterOverPrice2'] = body[73:78].decode("EUC-KR")
    data['basicDistance2'] = body[78:83].decode("EUC-KR")
    data['afterDistance2'] = body[83:88].decode("EUC-KR")
    results.append(data)
    return results

def makeBtnDataDict(body,data):
    results=[]
    data['actionDateTime'] = body[:14].decode("EUC-KR")
    data['btnType'] = body[14:16].decode("EUC-KR")
    data['btnStatus'] = body[16:17].decode("EUC-KR")
    data['carPositionX'] = body[17:26].decode("EUC-KR")
    data['carPositionY'] = body[26:35].decode("EUC-KR")
    results.append(data)
    return results

def makeDriveDataDict(body,data):
    dtgHeaderLength= 39
    dtgSingleBodyLength = 68

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
        dtgBody = copy.copy(data)
        startIdx = int((i*68) - 68)
        endIdx = int(startIdx + 68)
        targetBody = bodyOrig[startIdx:endIdx]
        dtgBody['deviceModel'] = dtgHeader['deviceModel']
        dtgBody['TripSeq'] = dtgHeader['TripSeq']
        dtgBody['dtgCode'] = dtgHeader['dtgCode']
        dtgBody['dtgCnt'] = dtgHeader['dtgCnt']
        dtgBody['dailyDistance'] = targetBody[:4].decode("EUC-KR")
        dtgBody['accumDistance'] = targetBody[4:11].decode("EUC-KR")
        dtgBody['eventDateTime'] = targetBody[11:25].decode("EUC-KR")
        dtgBody['speed'] = targetBody[25:28].decode("EUC-KR")
        dtgBody['rpm'] = targetBody[28:32].decode("EUC-KR")
        dtgBody['breakOnOff'] = targetBody[32:33].decode("EUC-KR")
        dtgBody['carPostionX'] = targetBody[33:42].decode("EUC-KR")
        dtgBody['carPostionY'] = targetBody[42:51].decode("EUC-KR")
        dtgBody['azimuth'] = targetBody[51:54].decode("EUC-KR")
        dtgBody['accVx'] = targetBody[54:60].decode("EUC-KR")
        dtgBody['accVy'] = targetBody[60:66].decode("EUC-KR")
        dtgBody['deviceStatus'] = targetBody[66:68].decode("EUC-KR")
        dtgBodys.append(dtgBody)
    return dtgBodys


def checkResultDirAndCreate():
    if not os.path.isdir('./results'):
        os.mkdir('./results')
    if not os.path.isdir('./results/events'):
        os.mkdir('./results/events')
    if not os.path.isdir('./results/dtg'):
        os.mkdir('./results/dtg')
    if not os.path.isdir("./results/merged"):
        os.mkdir('./results/merged')

def findMergeTarget(subfix):
    targetDir = "./results/events/"
    targetList = []
    if subfix == "_dtg":
        targetDir = "./results/dtg/"
    for (path, dir, files) in os.walk(targetDir):
        for filename in files:
            filenameWithoutExt = os.path.splitext(filename)[0]
            ext = os.path.splitext(filename)[-1]
            if subfix in filenameWithoutExt:   
                if ext == '.csv':
                    targetList.append(targetDir+filename)
    return targetList

def mergeCSVTargetsRead(filename):
    with open(filename,'r',encoding="EUC-KR") as f:
        lines = f.readlines()
        datas = list(filter(lambda x : x != "\n",lines))
    return datas

if __name__ == '__main__':
    if not sys.argv[1:]:
        print("please insert options : ex) 01 02 03 04 05 or 70")
        sys.exit(0)

    targetList = searchTarget()
    count_t = time.time()
    print("total targets is  %s" %len(targetList))
    checkResultDirAndCreate()
    convertToCSV_options = partial(convertToCSV, options=sys.argv[1:])
    pool = multiprocessing.Pool(min(multiprocessing.cpu_count(),len(targetList)))
    pool.map(convertToCSV_options, targetList, chunksize=1)
    pool.close()
    pool.join()

    time.sleep(2)

    print("start merge to one csv file!")
    needHeader = True
    mergeType=""
    if("70" in sys.argv[1:]):
        mergeType = "_dtg"
    else:
        mergeType = "_events"
    mergeTarget = findMergeTarget(mergeType)
    print("total merge targets is  %s" %len(mergeTarget))

    p = multiprocessing.Pool(min(multiprocessing.cpu_count(),len(targetList)))
    with open('./results/merged/result'+mergeType+".csv",'w') as rFile:
        for result in p.imap(mergeCSVTargetsRead, mergeTarget):
            result = result if needHeader else result[1:]
            rFile.write("".join(result))

            needHeader = False
    p.close()
    p.join()

   

    print('Running Time : %.02f' % (time.time() - count_t))