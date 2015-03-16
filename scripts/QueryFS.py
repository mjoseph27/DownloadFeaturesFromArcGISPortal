#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      arok5348
#
# Created:     06/03/2015
# Copyright:   (c) arok5348 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import arcpy
import ProcessRestRequest
import json
import os

# creates featureclass when set to true
bCreateTable = True
MAX_NO_THREADS = 5



def main():

    max_split = arcpy.GetParameter(3)    
    service_url = arcpy.GetParameterAsText(0)
    if "query" not in service_url:
        service_url = "{}/query".format(service_url.rstrip("/"))
    params = arcpy.GetParameterAsText(2) or {}
    if params:
        params = json.loads(params)

    if "f" not in params:
        params["f"] = "json"
    #params["outFields"] = "*"
    #params["where"] = "1=1"
    initiateThreadCycles(max_split, service_url, params)





def getThreadName(start, finish):
    '''find the first and last oid and name the thread'''
    if start == finish:
        return "OID {}".format(start)
    else:
        return "from OID {} to {}".format(start, finish)



def createFeatureClass(queryResult, outputTable):
    '''Creates a table based on field definitions from JSON'''
    #arcpy.AddMessage(u"Create Feature Class {}".format(outputTable))
    if arcpy.Exists(outputTable):
        arcpy.Delete_management(outputTable)
    rs = arcpy.gp.fromEsriJson(json.dumps(queryResult, ensure_ascii=False))
    rs.save(outputTable)
    arcpy.Delete_management(rs)
# End def createFeatureClass

def appendFeatures(queryResult, outputTable):
    '''Adds records in the featuresetJSON to the output table'''
    # create a temp file and write JSON
    #arcpy.AddMessage(u"Append Features {}".format(tempTable))
    rs = arcpy.gp.fromEsriJson(json.dumps(queryResult, ensure_ascii=False))
    # save recordset as table and append
    tempTable = arcpy.CreateUniqueName("tempRS", "in_memory")
    rs.save(tempTable)
    arcpy.Append_management(tempTable, outputTable, "NO_TEST")
    arcpy.Delete_management(rs)
    arcpy.Delete_management(tempTable)


def processResponse(runningThreadCount):
    '''processes response JSON and creates table or appends records'''
    global bCreateTable
    while runningThreadCount > 0:
        q = ProcessRestRequest.outResponseQueue
        if q.qsize() > 0:
            response = q.get()
            runningThreadCount = runningThreadCount - 1
            if "Error" not in response["response"]:
                arcpy.AddMessage(u"Received and processing: {}".format(response["name"]))
                result = response["response"]
                if "features" in result:
                    # skip if empty featuresets were returned
                    if len(result["features"]) > 0:
                        outputTable = arcpy.GetParameterAsText(1)
                        if bCreateTable:
                            createFeatureClass(result, outputTable)
                            bCreateTable = False
                        else:
                            appendFeatures(result, outputTable)
                    else:
                        arcpy.AddMessage("Empty Featureset :{}".format(response["name"]))
                        raise SystemExit
                else:
                    arcpy.AddMessage("Error in query response:{}".format(response["name"]))
                    arcpy.AddError(result)
                    raise SystemExit
            else:
                arcpy.AddError(u"HTTP Request Error {}".format(response["name"]))
                raise SystemExit
        else:
            time.sleep(1)
    return True

# End def processResponse

def getOIDs(service_url, params, referer):

    #Set returnidsonly
    params["returnIdsOnly"] = True
    resp = ProcessRestRequest.getResponse(service_url, params, referer)
    if "objectIdFieldName" in resp:
        jsonResp = json.loads(resp)
        if jsonResp.get("objectIds"):
            params.pop("returnIdsOnly")
            return jsonResp
        else:
            arcpy.AddError("No features found for url: {}".format(service_url))
            SystemExit()
    else:
        arcpy.AddMessage(resp)
        arcpy.AddError("Unable to retrieve OIDs for url: {}".format(service_url))
        SystemExit()




def initiateThreadCycles(max_split, service_url, params, referer=None):
    # Get OIDs and calculate thread cycles
    #arcpy.AddMessage("ThreadCycles")
    respOID = getOIDs(service_url, params, referer)
    listOID = respOID["objectIds"]
    OIDFieldName = respOID["objectIdFieldName"]
    count = len(listOID)
    #Find number of threads based on max_split
    total_threads = count / max_split
    if count % max_split != 0:
        total_threads = total_threads + 1
    # if total_threads is more than max_threads create thread cycles
    if total_threads > MAX_NO_THREADS :
        no_thread_cycles = total_threads / MAX_NO_THREADS
        if total_threads % MAX_NO_THREADS > 0:
            no_thread_cycles = no_thread_cycles + 1
    else:
        no_thread_cycles = 1

    #initiate thread cycles
    for curr_thread_cycle in range(0, no_thread_cycles):
        # calculate already processed features and provide a mark to start position for next thread cycle
        startCycleRow = curr_thread_cycle * max_split * (MAX_NO_THREADS)
        number_of_threads = MAX_NO_THREADS
        # the number of threads in the last cycle may be less than max_no_threads
        if curr_thread_cycle == no_thread_cycles - 1 :
            lastCycleThreadCount = total_threads % MAX_NO_THREADS
            if lastCycleThreadCount != 0:
                number_of_threads = lastCycleThreadCount
        arcpy.AddMessage(u"Running Thread cycle {}".format(curr_thread_cycle + 1))
        arcpy.AddMessage(u"Number of threads {}".format(number_of_threads))
        initiateThreads(listOID, startCycleRow, max_split, number_of_threads, service_url, params, referer)

# End def initiateThreadCycles


def initiateThreads(listOID, startCycleRow, max_split, number_of_threads, service_url, params, referer):
    # initiate threads
    threads = []
    lenListOID = len(listOID)
    for curr_thread in range (0, number_of_threads):
        # get geometries
        startRow = startCycleRow + (curr_thread * max_split)
        finishRow = startRow + max_split
        # the last run may not have max_num_features
        if finishRow >= lenListOID:
            finishRow = lenListOID
        params["objectids"] = ",".join([str(num) for num in listOID[startRow:(finishRow)]])
        name = getThreadName(listOID[startRow], listOID[finishRow - 1])
        arcpy.AddMessage("Requesting OIDs: {}".format(name))
        t = (ProcessRestRequest.ProcessRestReq(name, service_url, params, referer))
        t.start()
        threads.append(t)
    processResponse(number_of_threads)
    for t in threads:
        t.join()


if __name__ == '__main__':
    main()
