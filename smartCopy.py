#!/usr/bin/env python3

import os
import re
import sys
import zmq
import math
import time
import socket
import argparse
import subprocess
from datetime import datetime

from zeroconf import Zeroconf

#
# Site Name
#
SITE = socket.gethostname().split('-', 1)[0]


# Maximum number of bytes to receive from MCS
MCS_RCV_BYTES = 16*1024


# Regular expresion to check for remote paths
REMOTE_PATH_RE = re.compile(r'^((?P<user>[a-zA-Z0-9]+)\@)?(?P<host>[a-zA-Z0-9\-]+)(?<!\\)\:')


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


def buildPayload(source, cmd, data=None, refSocket=None):
    if refSocket is None:
        ref = 1
    else:
        try:
            refSocket.send(b"next_ref")
            ref = int(refSocket.recv(), 10)
        except zmq.ZMQError as e:
            raise RuntimeError("Cannot access reference ID server: %s" % str(e))
            
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
        refPort = int(zinfo.properties['message_ref_port'], 10)
    except KeyError:
        inPort = int(zinfo.properties[b'message_out_port'], 10)
        refPort = int(zinfo.properties[b'message_ref_port'], 10)
        
    context = zmq.Context()
    sockRef = context.socket(zmq.REQ)
    sockRef.connect("tcp://%s:%i" % (outHost, refPort))
    sockRef.setsockopt(zmq.RCVTIMEO, 5000)
    
    infs = []
    cmds = []
    destPath = args.destination[0]
    for srcPath in args.source:
        try:
            host, hostpath = re.split(r'(?<!\\)\:', srcPath, 1)
        except ValueError:
            host, hostpath = '', srcPath
        if host == '':
            host = inHost
            hostpath = os.path.abspath(hostpath)
            
        if host == inHost:
            if not os.path.exists(hostpath):
                raise RuntimeError("Source file '%s' does not exist" % hostpath)
                
        try:
            dest, destpath = re.split(r'(?<!\\)\:', destPath, 1)
        except ValueError:
            dest, destpath = '', destPath
        if dest == '':
            dest = inHost
            destpath = os.path.abspath(destpath)
        else:
            if host == inHost:
                try:
                    p = subprocess.Popen(['timeout', '-k', '5', '10', 'ssh', dest, 'date'])
                    if p.wait() != 0:
                        raise RuntimeError("Cannot login to %s" % dest)
                except subprocess.CalledProcessError as e:
                    raise RuntimeError("Cannot test login for %s: %s", dest, str(e))
                    
        infs.append( "Queuing copy for %s:%s to %s:%s" % (host, hostpath, dest, destpath) )
        cmds.append( buildPayload(inHost, "SCP", data="%s:%s->%s:%s" % (host, hostpath, dest, destpath), refSocket=sockRef) )
        
    try:
        sockOut = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sockOut.settimeout(5)
        sockIn  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sockIn.bind(("0.0.0.0", inPort))
        sockIn.settimeout(5)
        
        for inf,cmd in zip(infs,cmds):
            print(inf)
            
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
        
    sockRef.close()
    context.term()
    
    zeroconf.close()
    time.sleep(0.1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Queue a file copy for later.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('source', type=str, nargs='+',
                        help='filename to copy')
    parser.add_argument('destination', type=str, nargs=1,
                        help='destination to copy to')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s',
                        help='display version information')
    args = parser.parse_args()
    main(args)
    
