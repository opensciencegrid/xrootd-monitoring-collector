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

def Convert(source_record):
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
    to_return["server_host"] = source_record.get("server_hostname", "").split(".")[0]
    if "server_hostname" in source_record and source_record["server_hostname"] != "":
        to_return["server_domain"] = ".".join(source_record["server_hostname"].split(".")[1:])
    else:
        to_return["server_hostname"] = ""
    # Generate a random uuid
    to_return["unique_id"] = str(uuid.uuid4())
    to_return["file_lfn"] = source_record.get("filename", "")
    to_return["fize_size"] = source_record.get("filesize", 0)
    to_return["read_bytes"] = source_record["read"] + source_record["readv"]
    to_return["read_single_bytes"] = source_record.get("read", 0)
    to_return["read_vector_bytes"] = source_record.get("readv", 0)
    to_return["ipv6"] = source_record.get("ipv6", False)
    
    
    # Add the metadata
    to_return["metadata"] = {
        "producer": "cms",
        "type": "aaa-test",
        "timestamp": int(round(time.time()*1000)),
        "type_prefix": "raw",
        "host": socket.gethostname(),
        "_id": to_return['unique_id']
    }
    
    return to_return
    






