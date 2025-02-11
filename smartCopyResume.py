#!/usr/bin/env python3
import os
import sys
import math
import time
import uuid
import socket
import argparse
from datetime import datetime

from zeroconf import Zeroconf

#
# Site Name
#
SITE = socket.gethostname().split('-', 1)[0]


# Maximum number of bytes to receive from MCS
MCS_RCV_BYTES = 16*1024


def get_time():
    """
    Return a two-element tuple of the current MJD and MPM.
    """
    
    # determine current time
    dt = datetime.utcnow()
    year        = dt.year             
    month       = dt.month      
    day         = dt.day    
    hour        = dt.hour
    minute      = dt.minute
    second      = dt.second     
    millisecond = dt.microsecond / 1000
    
    # compute MJD         
    # adapted from http://paste.lisp.org/display/73536
    # can check result using http://www.csgnetwork.com/julianmodifdateconv.html
    a = (14 - month) // 12
    y = year + 4800 - a          
    m = month + (12 * a) - 3                    
    p = day + (((153 * m) + 2) // 5) + (365 * y)   
    q = (y // 4) - (y // 100) + (y // 400) - 32045
    mjd = int(math.floor( (p+q) - 2400000.5))  
    
    # compute MPM
    mpm = int(math.floor( (hour*3600 + minute*60 + second)*1000 + millisecond ))
    
    return (mjd, mpm)


def get_reference():
    ref = 0
    while ref == 0 or ref == 999999999:
        ref = int.from_bytes(uuid.uuid1().bytes[:4], byteorder='big')
        ref %= 1000000000
    return ref


def buildPayload(source, cmd, data=None):
    mjd, mpm = get_time()
    ref = get_reference()
    
    payload = ''
    payload += 'SCM'
    payload += source
    payload += cmd
    payload += '%9i' % ref
    if data is None:
        payload += '%4i' % 0
    else:
        payload += '%4i' % len(data)
    payload += '%6i' % mjd
    payload += '%9i' % mpm
    payload += ' '
    if data is not None:
        payload += data
        
    return payload


def parsePayload(payload):
    dataLen = int(payload[18:22], 10)
    cmdStatus = payload[38]
    subStatus = payload[39:46]
    data      = payload[46:46+dataLen-7]
    
    return cmdStatus, subStatus, data


def main(args):
    # Connect to the smart copy command server
    tPoll = time.time()
    nametag = SITE.replace('lwa', '').lower()
    zinfo = None
    while time.time() - tPoll <= 10.0:
        zeroconf = Zeroconf()
        zinfo = zeroconf.get_service_info("_sccs%s._udp.local." % nametag, "Smart copy server._sccs%s._udp.local." % nametag)
        
        if zinfo is not None:
            if 'message_out_port' not in zinfo.properties \
               and b'message_out_port' not in zinfo.properties:
                zinfo = None
                
        if zinfo is not None:
            break
            
        zeroconf.close()
        time.sleep(1)
    if zinfo is None:
        raise RuntimeError("Cannot find the smart copy command server")
        
    try:
        outHost = socket.inet_ntoa(zinfo.addresses[0])
    except AttributeError:
        outHost = socket.inet_ntoa(zinfo.address)
    outPort = zinfo.port
    try:
        inHost = socket.gethostname().split('-')[1].upper()
    except IndexError:
        inHost = socket.gethostname().upper()
    inHost = inHost[:3]
    while len(inHost) < 3:
        inHost += "_"
    try:
        inPort = int(zinfo.properties['message_out_port'], 10)
    except KeyError:
        inPort = int(zinfo.properties[b'message_out_port'], 10)
        
    cmds = []
    nDR = 5 if SITE == 'lwa1' else 4
    for i in range(1, nDR+1):
        dr = 'DR%i' % i
        if args.all or dr in args.DR:
            cmds.append( buildPayload(inHost, "RES", data=dr) )
            
    try:
        sockOut = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sockOut.settimeout(5)
        sockIn  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sockIn.bind(("0.0.0.0", inPort))
        sockIn.settimeout(5)
        
        for cmd in cmds:
            cmd = cmd.encode()
            sockOut.sendto(cmd, (outHost, outPort))
            data, address = sockIn.recvfrom(MCS_RCV_BYTES)
            
            data = data.decode()
            cStatus, sStatus, info = parsePayload(data)
            info = info.split('\n')
            if len(info) == 1:
                print(cStatus, sStatus, info[0])
            else:
                print(cStatus, sStatus)
                for line in info:
                    print("  %s" % line)
                    
        sockIn.close()
        sockOut.close()
    except socket.error as e:
        raise RuntimeError(str(e))
        
    zeroconf.close()
    time.sleep(0.1)


if __name__ == "__main__":
    def data_recorder(value):
        value = value.upper()
        if value[:2] != 'DR':
            raise argparse.ArgumentError
        try:
            int(value[2:], 10)
        except ValueError:
            raise argparse.ArgumentError
        return value
        
    parser = argparse.ArgumentParser(
        description='Resume the processing queue on the specified DR',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('DR', type=data_recorder, nargs='*',
                        help='data recoder name')
    parser.add_argument('-a', '--all', action='store_true',
                        help='resume copies on all DRs')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s', 
                        help='display version information')
    args = parser.parse_args()
    main(args)
    
