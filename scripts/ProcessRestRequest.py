
import os, Queue, threading, json, urllib, urllib2, StringIO, gzip

outResponseQueue = Queue.Queue()


def writeToQueue(name, response="Error"):
    outResponseQueue.put({"name" : name,
                          "response":response})

def writeToTempFile(fileFullPathAndName, data):
    tmpFile = open(fileFullPathAndName, "w")
    tmpFile.write(data)
    tmpFile.close()

def getResponse(serviceUrl, params=None, referer=None):

    try:

         req = urllib2.Request(serviceUrl)
         req.add_header('Accept-encoding', 'gzip, deflate')
         if referer:
            req.add_header("referer",referer)
         if params:
            params = urllib.urlencode(params)
            resp = urllib2.urlopen(req, params)
         else:
            resp = urllib2.urlopen(req)
         if resp.info().get('Content-Encoding') == 'gzip':
            buf = StringIO.StringIO(resp.read())
            f = gzip.GzipFile(fileobj=buf)
            return f.read()
         else:
            print "not gzipped"
            return resp.read()


    except urllib2.HTTPError as e:
        return "HTTPError {}".format(str(e))
    except Exception as e:
        return "Error {}".format(str(e))



class ProcessRestReq(threading.Thread):

    def __init__(self, name, url, params=None, referer=None, scratchWkspc=None):
        threading.Thread.__init__(self)
        self.name = name
        self.url = url
        self.scratchWkspc = scratchWkspc
        self.params = params
        self.referer = referer

    def run(self):
        try:
            if self.params and self.scratchWkspc:
                filePath = os.path.join(self.scratchWkspc, "request_{}.json".format(self.name))
                writeToTempFile(filePath, json.dumps(self.params))
            response = getResponse(self.url, self.params, self.referer)
            if self.scratchWkspc:
                filePath = os.path.join(self.scratchWkspc, "request_{}.json".format(self.name))
                writeToTempFile(filePath, response)
            if "Error" in response:
                 writeToQueue(self.name, response)
            else:
                respJSON = json.loads(response)
                writeToQueue(self.name, respJSON)
        except Exception as e:
            writeToQueue(self.name, "Error : {}".format(str(e)))


