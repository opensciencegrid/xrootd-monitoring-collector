"""
Convert to a format WLCG would like.

{
"site_name":"T2_US_Nebraska",
 "fallback": true,
 "user_dn":"/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Brian Bockelman",
 "client_host":"brian-test",
 "client_domain":"unl.edu",
 "server_host":"cmsstor359",
 "server_domain":"fnal.gov",
 "unique_id":"8ABDCAFE-B469-E211-82E9-00163ED711AD-0",
 "file_lfn":"/store/relval/CMSSW_6_1_0-GR_R_61_V6_RelVal_wEl2012C/SingleElectron/RECO/v1/00000/FAC88284-414C-E211-84E7-002618943947.root",
 "file_size":2198266271,
 "read_single_sigma":630363,
 "read_single_average":161060,
 "read_vector_average":1.26992e+07,
 "read_vector_sigma":7.91128e+06,
 "read_vector_count_average":21.3929,
 "read_vector_count_sigma":70.4056,
 "read_bytes":358477665,
 "read_bytes_at_close":358477665,
 "read_single_operations":18,
 "read_single_bytes":2899080,
 "read_vector_operations":28,
 "read_vector_bytes":355578585,
 "start_time":1359423686,
 "end_time":1359423788,
 "ipv6": false
}


"""

import uuid
import time
import socket
import urllib.parse

def Convert(source_record, metadata_producer, metadata_type):
    """
    Convert to the WLCG format documented here:
    https://twiki.cern.ch/twiki/bin/view/Main/GenericFileMonitoring
    """
    to_return = {}
    to_return["site_name"] = source_record.get("site", "")
    to_return["fallback"] = True
    to_return["user_dn"] = source_record.get("user_dn", "")
    # client_host (without domain information)
    to_return["client_host"] = source_record.get("host", "")
    to_return["client_domain"] = source_record.get("user_domain", "")
    to_return["server_host"] = source_record.get("server_hostname", "")
    if "server_hostname" in source_record and source_record["server_hostname"] != "":
        to_return["server_domain"] = ".".join(source_record["server_hostname"].split(".")[-2:])
    else:
        to_return["server_hostname"] = ""
    to_return["server_ip"] = source_record.get("server_ip", "")
    # Generate a random uuid
    to_return["unique_id"] = str(uuid.uuid4())
    to_return["file_lfn"] = source_record.get("filename", "")
    to_return["file_size"] = source_record.get("filesize", 0)
    to_return["read_bytes"] = source_record["read"] + source_record["readv"]
    to_return["read_single_bytes"] = source_record.get("read", 0)
    to_return["read_vector_bytes"] = source_record.get("readv", 0)
    to_return["ipv6"] = source_record.get("ipv6", False)
    if "user_dn" in source_record:
        to_return["user_dn"] = source_record["user_dn"]
        # user
        # appears to be just everything after the CN in the user_dn
        try:
            split_dn = source_record["user_dn"].split("CN=")
            to_return["user"] = split_dn[1]
        except Exception as e:
            print("Failed to split the DN to create the WLCG user attribute: ", str(e))


    # start_time
    # When we received the file open event
    to_return['start_time'] = source_record['start_time']

    # Use timestamp from original record, or now if there is no timestamp
    to_return['end_time'] = source_record['end_time']

    # operation_time
    to_return['operation_time'] = source_record['operation_time']

    # operation is either "read", "write", or "unknown"
    if source_record['read'] > 0 or source_record['readv'] > 0:
        to_return['operation'] = 'read'
    elif source_record['write'] > 0:
        to_return['operation'] = 'write'
    else:
        to_return['operation'] = 'unknown'

    # server_site
    to_return['server_site'] = source_record.get('site', "")

    # user_protocol
    if "protocol" in source_record:
        to_return["user_protocol"] = source_record["protocol"]

    # vo
    if "vo" in source_record:
        to_return["vo"] = source_record["vo"]

    # write_bytes
    to_return["write_bytes"] = source_record['write']
    # remote_access - boolean
    # is_transfer (optional)
    # user_fqan (optional)
    # user_role (optional)
    # server_username (optional)

    # Whole bunch of values to copy from the source_record
    values_to_copy = ["read_average",
                      "read_bytes_at_close",
                      "read_max",
                      "read_min",
                      "read_operations",
                      "read_sigma",
                      "read_single_average",
                      "read_single_bytes",
                      "read_single_max",
                      "read_single_min",
                      "read_single_operations",
                      "read_single_sigma",
                      "read_vector_average",
                      "read_vector_count_average",
                      "read_vector_count_max",
                      "read_vector_count_min",
                      "read_vector_count_sigma",
                      "read_vector_max",
                      "read_vector_min",
                      "read_vector_operations",
                      "read_vector_sigma",
                      "throughput",
                      "write_average",
                      "write_bytes_at_close",
                      "write_max",
                      "write_min",
                      "write_operations",
                      "write_sigma"]

    for key in values_to_copy:
        to_return[key] = source_record.get(key, 0)

    if 'appinfo' in source_record:
        # Convert from 
        # 162_https://glidein.cern.ch/162/190501:101553:heewon:crab:RPCEfficiency:SingleMuon:Run2018D-PromptReco-v2_0
        # would translate into:
        # CRAB_Workflow= 190501:101553:heewon:crab:RPCEfficiency:SingleMuon:Run2018D-PromptReco-v2
        # CRAB_Id= 162
        # CRAB_Rerty=0
        # or
        # b) 3-99_https://glidein.cern.ch/3-99/190608:212153:dseith:crab:SingleMuon:2017:E_2
        # would translate into:
        # CRAB_Workflow= 190608:212153:dseith:crab:SingleMuon:2017:E
        # CRAB_Id= 3-99
        # CRAB_Rerty=2

        # Try to split the appinfo by "_"
        split_appinfo = source_record['appinfo'].decode().split("_")
        if len(split_appinfo) == 3:
            to_return['CRAB_Id'] = split_appinfo[0]
            to_return['CRAB_Retry'] = split_appinfo[2]

            # Grab everything after the last "/"
            try:
                to_return['CRAB_Workflow'] = split_appinfo[1].split("/")[-1]
            except:
                pass

    if 'HasFileCloseMsg' in source_record:
        to_return['HasFileCloseMsg'] = source_record['HasFileCloseMsg']
 
    # Add the metadata
    to_return["metadata"] = {
        "producer": metadata_producer,
        "type": metadata_type,
        "timestamp": to_return['end_time'],
        "type_prefix": "raw",
        "host": socket.gethostname(),
        "_id": to_return['unique_id']
    }
    
    return to_return

# check the path on TPC
def tpcPathCheckWLCG(url):
    try:
        parts = urllib.parse.urlparse(url)
        path = parts.path.lstrip('/')

        if path.startswith('store') or path.startswith('user/dteam'):
            return True
        else:
            return False
    except:
        return False

def ConvertGstream(source_record,tpc=False):
    if tpc:
        typeT = "tpc"
    else:
        typeT = "metric"
      
    idd = str(uuid.uuid4())
    # Add the metadata
    source_record["metadata"] = {
        "producer": "cms-xrootd-cache",
        "type": typeT,
        "timestamp": int(round(time.time()*1000)),
        "type_prefix": "raw",
        "host": socket.gethostname(),
        "_id": idd
    }

    return source_record
