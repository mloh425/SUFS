import json
import math
import os
import random
import threading
import time
import requests
import constants

from readerwriterlock import rwlock
import werkzeug.exceptions as HTTPStatus
from flask import Flask, abort
from flask_restful import Api, Resource, request
from collections import Counter

app = Flask(__name__)
api = Api(app)

gLock = rwlock.RWLockRead()
LastSeenDNs = {}

'''
Map of the entire filesystem i.e FSData
Structure:
{
    <DNID> : {
        "BlockList": {
            <blockid>: {"size" <length-in-bytes>}
        },
        "AvailableCapacity": <length-in-bytes>,
        "TotalCapacity": <length-in-bytes>
    },
    <DNID> : {
        "BlockList": {
            <blockid>: {"size" <length-in-bytes>}
        },
        "AvailableCapacity": <length-in-bytes>,
        "TotalCapacity": <length-in-bytes>
    },
    <DNID> : {
        "BlockList": {
            <blockid>: {"size" <length-in-bytes>}
        },
        "AvailableCapacity": <length-in-bytes>,
        "TotalCapacity": <length-in-bytes>
    }
}
'''
FSData = {}

gMetadataLock = rwlock.RWLockRead()
METADATA_FILE = "metadata.json"


def _loadMetaData():
    try:
        return json.load(open(METADATA_FILE, "r"))
    except FileNotFoundError:
        return {}


def loadMetaData():
    with gMetadataLock.gen_rlock():
        return _loadMetaData()


def addToMetaData(filename, blockids):
    with gMetadataLock.gen_wlock():
        metadata = _loadMetaData()
        metadata[filename] = blockids
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f)


def rebalanceData():
    # Get the list of active dns for each blockid.
    activeDNs = None
    dnsByAvlCap = None
    degradedBlocks = []

    with gLock.gen_rlock():
        activeDNs = getActiveDNs()
        dnsByAvlCap = getDNsByAvailableCapacity()
        allBlockIDs = {}
        for dnid, dnDetails in FSData.items():
            if dnid not in activeDNs:
                print("=============")
                print("=============")
                print("NODE FAILED: ", dnid)
                print("=============")
                print("=============")
                degradedBlocks.extend(dnDetails["BlockList"].keys())
                continue
            for blockid, _ in dnDetails["BlockList"].items():
                if blockid in allBlockIDs:
                    allBlockIDs[blockid].append(dnid)
                else:
                    allBlockIDs[blockid] = [dnid]

    for blockid, dns in allBlockIDs.items():
        if len(dns) >= constants.REPLICATION_FACTIOR:
            continue
        if blockid not in degradedBlocks:
            # this block is not part of the degraded set, so it is not part of a node which has gone down.
            # This is possible a first time written degraded block.
            print("skipping undegraded block; client is possibly still replicating ", blockid)
            continue
        # Find the DNs which do not have a copy yet.
        possibleTargetDNs = list(set(activeDNs).difference(set(dns)))
        # Select the DNs which have the most available capacity.
        possibleTargetDNs.sort(key=dnsByAvlCap.get, reverse=True)
        # Iterate over the selected DNs.
        for dnid in possibleTargetDNs[:constants.REPLICATION_FACTIOR - len(dns)]:
            # This can be improved for better parallelization, but picking the random request DNID for now.
            requestDN = random.choice(dns)
            print("requesting DN(" + requestDN + ") to copy block " + blockid + " to DN(" + dnid + ")")
            resp = requests.post("http://" + requestDN + "/SendCopy", json={"block_id": blockid, "target_dn": dnid})
            # resp = requests.post("http://127.0.0.1:" + requestDN + "/SendCopy", json={"block_id": blockid, "target_dn": dnid})

            if resp.status_code != 200:
                print("Error code: " + str(resp.status_code))
                # DO not remove the dnid from, fsdata either.
                return
            print(blockid + " written to " + dnid)

    with gLock.gen_wlock():
        for dnid, dnDetails in list(FSData.items()):
            if dnid not in activeDNs:
                del FSData[dnid]
                print("REMOVED FROM THE FILESYSTEM: ", dnid)


def redundancyManager():
    while True:
        try:
            rebalanceData()
        except Exception as err:
            print(err)
        time.sleep(3)


def getActiveDNs():
    """
    NOTE: It is assumed that the caller will acquire the global data structure lock.

    :return:
    """
    ActiveDNs = []
    currentTime = time.monotonic()
    for DataNodeName, timeSeen in LastSeenDNs.items():
        if (currentTime - timeSeen) < constants.HEARTBEAT_TIMEOUT:
            ActiveDNs.append(DataNodeName)
    return ActiveDNs


def getDNsByAvailableCapacity():
    """
    NOTE: It is assumed that the caller will acquire the global data structure lock.

    :return:
    """
    dns_by_available_capacity = {}
    active_dns = getActiveDNs()
    for dnid, dn_details in filter(lambda e: e[0] in active_dns, FSData.items()):
        dns_by_available_capacity[dnid] = dn_details["AvailableCapacity"]

    return dns_by_available_capacity


# get list of active DataNodes
class Heartbeat(Resource):
    def post(self):
        args = request.get_json(force=True)
        with gLock.gen_wlock():
            LastSeenDNs[args["DataNodeName"]] = time.monotonic()


# receiving block report from DataNodes
class BlockReport(Resource):
    def post(self, DNID):
        args = request.get_json(force=True)
        if type(args["BlockReport"]) != dict:
            abort(HTTPStatus.BadRequest.code)

        with gLock.gen_wlock():
            FSData[DNID] = {
                "BlockList": args["BlockReport"],
                "AvailableCapacity": int(args["AvailableCapacity"]),
                "TotalCapacity": int(args["TotalCapacity"])
            }


def getExistingDNsForBlockID(block_id):
    """

    :rtype: list
    """
    for dnid, dn_details in FSData.items():
        if block_id in dn_details["BlockList"].keys():
            yield dnid


class AllocateBlocks(Resource):
    def post(self):
        """
        This methods responds the client with the information about the available active's DNs and list of blocks.
        The response structure will look like this:

        Response:
        {
            # {
            # “File1block1": [“127.0.0.1”, “127.0.0.2",],
            # “File2block2”: [“127.0.0.3", “127.0.0.2”,]
            # }
        }
        :return:
        """
        args = request.get_json(force=True)
        if "filesize" not in args or "filename" not in args or type(args["filename"]) != str or type(
                args["filesize"]) != int:
            abort(HTTPStatus.BadRequest.code)

        allocation_table = {}
        with gLock.gen_rlock():
            dns_by_available_capacity = getDNsByAvailableCapacity()
            for block_num in range(math.ceil(args["filesize"] / constants.BLOCKSIZE)):
                block_id = args["filename"] + ".block-" + str(block_num)
                allocation_table[block_id] = []
                existing_allocation = list(getExistingDNsForBlockID(block_id))
                remainingReplication = constants.REPLICATION_FACTIOR - len(existing_allocation)
                for _ in range(remainingReplication):
                    # print(dns_by_available_capacity)
                    unique_dns = list(
                        filter(lambda dnid: dnid not in allocation_table[block_id] and dnid not in existing_allocation,
                               dns_by_available_capacity))
                    # print("getting biggest dn from", unique_dns, "excluding", allocation_table[block_id])
                    try:
                        biggest_dn = max(unique_dns, key=dns_by_available_capacity.get)
                    except ValueError:
                        # If we cannot find a unique dana node for this block then tell the client that we do not have
                        # enough nodes.
                        abort(HTTPStatus.NotAcceptable.code)
                    else:
                        dns_by_available_capacity[biggest_dn] -= constants.BLOCKSIZE
                        allocation_table[block_id].append(biggest_dn)

        # If there are no blocks to be copied over to DNs then abort saying this file already exists.
        if not sum([], sum(allocation_table.values(), [])):
            print("dropping request to write the same file again: ", args["filename"])
            abort(HTTPStatus.Conflict.code)

        addToMetaData(args["filename"], list(allocation_table.keys()))
        return allocation_table


class GetFileBlocks(Resource):
    def get(self, filename):
        """
        This methods responds the client with the load-balanced set of data-nodes to read the blocks from.
        The response structure will look like this:

        Response:
        {
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-10" : "5005",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-4" : "5004",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-1" : "5004",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-5" : "5004",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-6" : "5003",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-9" : "5004",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-7" : "5003",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-2" : "5003",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-0" : "5005",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-8" : "5005",
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-3" : "5003"
        }

        :type filename: str
        :param filename: name of the file
        :return:
        """
        blocks = {}
        with gLock.gen_rlock():
            active_DNs = getActiveDNs()
            for dnid, dn_details in FSData.items():
                if dnid not in active_DNs:
                    # Skip inactive DNs.
                    continue
                for blockID, _ in dn_details["BlockList"].items():
                    if os.path.splitext(blockID)[0] == filename:
                        if blockID in blocks:
                            blocks[blockID].append(dnid)
                        else:
                            blocks[blockID] = [dnid]

        if not blocks:
            abort(HTTPStatus.NotFound.code)

        # Distribute the read load evenly among all data nodes.
        uniformDNs = {}
        counter = Counter()
        # Prefer the blocks which have the lowest DN count.
        for blockID, dns in sorted(blocks.items(), key=lambda e: len(e[1])):
            selectedDN = None
            # First find a DN which has not been used earlier.
            for dn in dns:
                if dn not in uniformDNs.values():
                    selectedDN = dn
                    break
            else:
                # If we cannot find a unique DN then use the one with the lowest frequency.
                for dn, _ in counter.most_common()[::-1]:
                    # We cannot use a DN which doesn't serve this blockID, so filter out those which do.
                    if dn in dns:
                        selectedDN = dn
                        break

            # Record the block DN host for final read.
            uniformDNs[blockID] = selectedDN
            # Also update our stats so that we can avoid re-using this DN too much
            # and uniformly distribute the read-load over all the DNs.
            counter.update([selectedDN])

        metadata = loadMetaData()
        if filename not in metadata:
            abort(HTTPStatus.NotFound.code)
        if set(metadata[filename]) != set(uniformDNs.keys()):
            abort(HTTPStatus.RequestedRangeNotSatisfiable.code)

        return uniformDNs


# Get entire block structure with all DNs
class GetAllBlocksDNs(Resource):
    def get(self, filename):
        """
        This methods responds the client with the load-balanced set of data-nodes to read the blocks from.
        The response structure will look like this:

        Response:
        {
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-10" : "5005", "5004", "5003"
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-4" : "5004", "5005", "5003"
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-1" : "5004", "5003" , "5005"
        }

        :type filename: str
        :param filename: name of the file
        :return:
        """
        blocks = {}
        with gLock.gen_rlock():
            active_DNs = getActiveDNs()
            for dnid, dn_details in FSData.items():
                if dnid not in active_DNs:
                    # Skip inactive DNs.
                    continue
                for blockID, _ in dn_details["BlockList"].items():
                    if os.path.splitext(blockID)[0] == filename:
                        if blockID in blocks:
                            blocks[blockID].append(dnid)
                        else:
                            blocks[blockID] = [dnid]

        if not blocks:
            abort(HTTPStatus.NotFound.code)

        return blocks


# Get entire block structure with all DNs
class GetAllBlocksDNs(Resource):
    def get(self, filename):
        """
        This methods responds the client with the set of data-nodes to read the blocks from.
        The response structure will look like this:

        Response:
        {
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-10" : "5005", "5004", "5003"
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-4" : "5004", "5005", "5003"
           "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-1" : "5004", "5003" , "5005"
        }

        :type filename: str
        :param filename: name of the file
        :return:
        """
        blocks = {}
        with gLock.gen_rlock():
            active_DNs = getActiveDNs()
            for dnid, dn_details in FSData.items():
                if dnid not in active_DNs:
                    # Skip inactive DNs.
                    continue
                for blockID, _ in dn_details["BlockList"].items():
                    if os.path.splitext(blockID)[0] == filename:
                        if blockID in blocks:
                            blocks[blockID].append(dnid)
                        else:
                            blocks[blockID] = [dnid]

        if not blocks:
            abort(HTTPStatus.NotFound.code)

        return blocks


api.add_resource(AllocateBlocks, "/AllocateBlocks/")
api.add_resource(Heartbeat, "/heartbeat/")
api.add_resource(GetFileBlocks, "/fileblocks/<string:filename>")
api.add_resource(GetAllBlocksDNs, "/AllBlocksDNs/<string:filename>")
api.add_resource(BlockReport, "/BlockReport/<string:DNID>")

if __name__ == '__main__':
    threading.Thread(target=redundancyManager).start()
    app.run(host='0.0.0.0', port='5000')
