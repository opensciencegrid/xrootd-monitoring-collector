import struct
import decoding
import argparse
import socket


# A single packet is:
# 1. Header
# 2. 

sequenceNum = 0

class FilePacket():
    def __init__(self):
        pass

class UserPacket():
    """
    userid   = namedtuple("userid", ["protocol", "username", "pid", "sid", "host"])
    authinfo = namedtuple("authinfo", ["ap", "dn", "hn", "on", "rn", "gn", "info", 'execname', 'moninfo', "inetv"])
    """
    def __init__(self):
        pass

    def generate_packet():
        decoding.userid("")

def create_header(code, packet_size):
    global sequenceNum
    packet_size += 8
    if sequenceNum > 255:
        sequenceNum = 0
    header = decoding.header(str.encode(code[0]), sequenceNum, packet_size, 0)
    pack = struct.pack("!cBHI", *header)
    sequenceNum += 1
    return pack

def wrap_user_pack(dictID, pack):
    mapheader = decoding.mapheader(dictID, pack)
    pack = struct.pack("!I" + str(len(pack)) + "s", *mapheader)

    return pack

def main():
    # Create an example packet
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", "-p", help="Port to send the packet", default=9930)
    parser.add_argument("--host", help="Host to send the packets", default="localhost")
    args = parser.parse_args()
    userid = decoding.userid("xroot", "dweitzel", 10, 2059, "red-gridftp4.unl.edu")
    user_encoded = decoding.revUserInfo(userid)
    print(user_encoded)
    decoded = decoding.userInfo(user_encoded)
    print(decoded)

    # ["program", "version", "instance", "port", "site", "addr"]
    srvInfo = decoding.srvinfo("xrootd", "4.9", "another-inst", "1950", "Nebraska", "addr")
    srv_encoded = decoding.revServerInfo(srvInfo)
    decoded = decoding.serverInfo(srv_encoded, "addr2")
    print(srv_encoded)
    print(decoded)

    create_header('f', 10)
    wrapped_user = wrap_user_pack(0, user_encoded + b"\n" + srv_encoded)
    final_packet = create_header("=", len(wrapped_user)) + wrapped_user
    print(final_packet)
    authinfo = decoding.authinfo("x509", "derek", "test.unl.edu", "", "", "", "", "test_exe", "", "6")
    auth_encoded = decoding.revAuthorizationInfo(authinfo)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    host = socket.gethostbyname(args.host)
    sock.sendto(final_packet, (host, args.port))
    counter = 0
    while counter < 1:
        # ["ap", "dn", "hn", "on", "rn", "gn", "info", 'execname', 'moninfo', "inetv"])
        user_encoded = decoding.revUserInfo(userid)
        wrapped_user = wrap_user_pack(counter, user_encoded + b"\n" + auth_encoded)
        final_packet = create_header("u", len(wrapped_user)) + wrapped_user
        sock.sendto(final_packet, (host, args.port))
        counter += 1

    # Send 



if __name__ == "__main__":
    main()
