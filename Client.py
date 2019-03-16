from botocore import exceptions as botoex
import requests
import base64
import boto3
import sys
import os
import constants

# Information about the current file in read or write

currentFileName = ""
currentFileSize = 0
currentNumberOfBlocks = 0
writeResponse = None
readResponse = None
nameNodeIP = ""

# This method does a POST method to the NameNodeServer
# it will send the name of the file along with the size of the file
# so that the NameNode can generate a block:DataNode list.
# Format of JSON in POST requests:
# {
#   filename: 'file.txt',
#   filesize: 12345
# }

# Format is
# {
#   'blockID1': ['dataNodeIP1', 'dataNodeIP2, ... ],
#   'blockID2': ['dataNodeIP3', 'dataNodeIP4, ... ],
#   'blockID2': ['dataNodeIP7', 'dataNodeIP6, ... ],
# }
def putToNameNode():
    global currentFileName
    global currentFileSize
    global writeResponse
    global nameNodeIP

    task = {"filename": currentFileName, "filesize": currentFileSize}
    resp = requests.post('http://' + nameNodeIP + ':5000/AllocateBlocks/', json=task)
    # resp = requests.post('http://127.0.0.1:5002/AllocateBlocks/', json=task)
    if resp.status_code == 200:
        print("Successful post")
        print(resp.json())
        writeResponse = resp.json()
    elif resp.status_code == 406:
        print("HDFS does not have enough storage for replication; please consider adding more data nodes")
        sys.exit(1)
    elif resp.status_code == 409:
        print("file already exists")
        sys.exit(1)
    else:
        print("Error code: " + str(resp.status_code))
        sys.exit(1)


def getFromNameNode():
    global currentFileName
    global readResponse
    global nameNodeIP

    resp = requests.get('http://' + nameNodeIP + ':5000/fileblocks/' + currentFileName)
    if resp.status_code == 200:
        readResponse = resp.json()
        print(resp.json())
    elif resp.status_code == 404:
        print("file does not exist")
        sys.exit(1)
    elif resp.status_code == 416:
        print("file is incomplete, please finish uploading first")
        sys.exit(1)
    else:
        print("Error code: " + str(resp.status_code))


def getAllBlocksDNs():
    global currentFileName
    global readResponse
    global nameNodeIP

    resp = requests.get('http://' + nameNodeIP + ':5000/AllBlocksDNs/' + currentFileName)

    # resp = requests.get('http://127.0.0.1:5002/AllBlocksDNs/' + currentFileName)
    if resp.status_code != 200:
        if resp.status_code == 404:
            print("file does not exist")
            sys.exit(1)
        print("Error code: " + str(resp.status_code))
    else:
        readResponse = resp.json()
        blockArr = list()
        for block in readResponse:
            blockArr.append(block)

        print("==============================")
        print("==============================")
        print("LISTING BLOCKS: [DATA NODES] ")
        print("==============================")
        print("==============================")

        blockArr.sort(key=lambda x: int(x.split('-')[-1]))
        for blockID in blockArr:
            ip = readResponse[blockID]
            print(str(blockID) + " : " + str(ip))

# This method parses through the block list and data nodes
# returned from the NameNodeServer and distributes all
# the blocks accordingly.
def putToDataNode():
    global currentFileName
    global currentNumberOfBlocks
    global writeResponse
    print(currentFileName)

    with open(currentFileName, 'rb') as f:
        for block in writeResponse:
            blockID = block
            replicaIPList = writeResponse[block]
            if replicaIPList:
                blockData = f.read(constants.BLOCKSIZE)
                for ip in replicaIPList:
                    send(blockID, ip, blockData)
            else:
                f.seek(constants.BLOCKSIZE, os.SEEK_CUR)



# This method sends the given block to the given DataNodeServer
# Called after putToDataNode()
# Parameters:
# blockID - name of the block e.g. file.block.1
# ip - IP address of the DataNode the block should reside in
# blockData - the binary chunk of the data to be sent to the block
def send(blockID, ip, blockData):
    data = str(base64.b64encode(blockData))
    data = data[2: len(data) - 1]

    task = {"data": data, "size": blockData.__sizeof__()}
    resp = requests.post('http://' + ip + '/BlockData/' + blockID, json=task)
    # resp = requests.post('http://127.0.0.1:' + ip + '/BlockData/' + blockID, json=task)
    if resp.status_code != 200:
        print("Error code: " + str(resp.status_code))
    else:
        print(blockID + " written to " + ip)


# Reading block:ip
def getFromDataNode():
    blockArr = list()
    for block in readResponse:
        blockArr.append(block)

    blockArr.sort(key=lambda x: int(x.split('-')[-1]))
    with open("COPY" + currentFileName, "wb") as fh:
        for blockID in blockArr:
            ip = readResponse[blockID]
            resp = requests.get('http://' + ip + '/BlockData/' + blockID)
            # resp = requests.get('http://127.0.0.1:' + ip + '/BlockData/' + blockID)
            object = resp.json()
            fh.write(base64.b64decode(object["data"]))
    fh.close()

def main():
    global nameNodeIP
    global currentFileName
    global currentFileSize

    if len(sys.argv) < 3:
        print("Missing command line argument.")
        print("Need to enter Name Node's IP, aws access key id, aws secret")
        print("Terminating")
        sys.exit(1)
    else:
        nameNodeIP = sys.argv[1]
        keyid = sys.argv[2]
        keysecret = sys.argv[3]

    #Get user action + filename
    print("Welcome to the Seattle University File System (SUFS)!")
    action = input("Enter write, read, or list: ")
    action = action.lower()

    if action != "read" and action != "write" and action != "list":
        print("Invalid action, terminating")
        sys.exit(1)

    file = input("What file would you like to read/write/list data nodes?: ")

    currentFileName = file
    if action == "write":
        print("============")
        print("============")
        print("WRITING FILE")
        print("============")
        print("============")
        resource = boto3.resource('s3',
                                  aws_access_key_id=keyid,
                                  aws_secret_access_key=keysecret
                                  )
        bk = resource.Bucket(constants.BUCKETNAME)
        try:
             bk.download_file(currentFileName, currentFileName)
        except botoex.ClientError as err:
             if err.response['Error']['Code'] == '404':
                 print("file not found on S3")
                 sys.exit(1)
             raise

        currentFileSize = os.path.getsize(currentFileName)

        putToNameNode()
        putToDataNode()
    elif action == "read":
        print("============")
        print("============")
        print("READING FILE")
        print("============")
        print("============")
        getFromNameNode()
        if readResponse is not None:
            getFromDataNode()
    elif action == "list":
        getAllBlocksDNs()

if __name__ == "__main__":
    main()
