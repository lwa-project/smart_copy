#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import os
import sys
import math
import time
import socket
import argparse
from datetime import datetime

from zeroconf import Zeroconf


SITE = socket.gethostname().split('-', 1)[0]


# Maximum number of bytes to receive from MCS
MCS_RCV_BYTES = 16*1024


def getTime():
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


def buildPayload(source, cmd, data=None):
    ref = 1
    
    mjd, mpm = getTime()
    
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
    zeroconf = Zeroconf()
    zinfo = zeroconf.get_service_info("_sccs._udp.local.", "Smart copy server._sccs._udp.local.")
    if zinfo is None:
        raise RuntimeError("Cannot find the smart copy command server")
        
    if args.version:
        ## Smart copy command server info
        print(zinfo)
        
    else:
        outHost = socket.inet_ntoa(zinfo.address)
        outPort = zinfo.port
        try:
            inHost = socket.gethostname().split('-')[1].upper()
        except IndexError:
            inHost = socket.gethostname().upper()[:3]
        while len(inHost) < 3:
            inHost += "_"
        inPort = int(zinfo.properties['MESSAGEOUTPORT'], 10)
        
        cmds = []
        nDR = 3 if SITE == 'lwasv' else 5
        for i in range(1, nDR+1):
            dr = 'DR%i' % i
            if args.all or dr in args.DR:
                cmds.append( buildPayload(inHost, "PAU", data=dr) )
                
        try:
            sockOut = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sockOut.settimeout(5)
            sockIn  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sockIn.bind(("0.0.0.0", inPort))
            sockIn.settimeout(5)
            
            for cmd in cmds:
                sockOut.sendto(cmd, (outHost, outPort))
                data, address = sockIn.recvfrom(MCS_RCV_BYTES)
                
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
        description='Pause the processing queue on the specified DR',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('DR', type=data_recorder, nargs='*',
                        help='data recoder name')
    parser.add_argument('-a', '--all', action='store_true',
                        help='pause copies on all DRs')
    parser.add_argument('-v', '--version', action='store_true', 
                        help='display version information')
    args = parser.parse_args()
    main(args)
    