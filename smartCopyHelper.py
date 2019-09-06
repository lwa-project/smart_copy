#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

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

from lsl.common import mcs, metabundle
try:
    from lsl.common import metabundleADP
    adpReady = True
except ImportError:
    adpReady = False


SITE = socket.gethostname().split('-', 1)[0]


_usernameRE = re.compile(r'ucfuser:[ \t]*(?P<username>[a-zA-Z1]+)(\/(?P<subdir>[a-zA-Z0-9\/\+\-_]+))?')


def parseMetadata(tarname):
    """
    Given a filename for a metadata tarball, parse the file and return a
    six-element tuple of:
     * filetag
     * DRSU barcode
     * beam
     * date
     * if the data is spectrometer or not
     * originally requested UCF copy path or None if there was none
    """
    
    try:
        parser = metabundle
        parser.getSessionSpec(tarname)
    except Exception as e:
        if adpReady:
            parser = metabundleADP
            parser.getSessionSpec(tarname)
        else:
            raise e
            
    project = parser.getSessionDefinition(tarname)
    isSpec = False
    if project.sessions[0].observations[0].mode not in ('TBW', 'TBN'):
        if project.sessions[0].spcSetup[0] != 0 and project.sessions[0].spcSetup[1] != 0:
            isSpec = True
            
    meta = parser.getSessionMetaData(tarname)
    tags = [meta[id]['tag'] for id in sorted(meta.keys())]
    barcodes = [meta[id]['barcode'] for id in sorted(meta.keys())]
    meta = parser.getSessionSpec(tarname)
    beam = meta['drxBeam']
    date = mcs.mjdmpm2datetime(int(meta['MJD']), int(meta['MPM']))
    datestr = date.strftime("%y%m%d")
    
    userpath = None
    if project.sessions[0].dataReturnMethod == 'UCF':
        mtch = _usernameRE.search(project.sessions[0].comments)
        if mtch is not None:
            userpath = mtch.group('username')
            if mtch.group('subdir') is not None:
                userpath = os.path.join(userpath, mtch.group('subdir'))
                
    return tags, barcodes, beam, date, isSpec, userpath


def getDRSUPath(beam, barcode):
    """
    Given a beam (data recorder) number and a DRSU barcode, convert the 
    barcode to a path on the data recorder.  Returns None if the DRSU cannot
    be found.
    """
    
    path = None
    
    try:
        p = subprocess.Popen(['ssh', "mcsdr@dr%s" % beam, 'mount', '-l', '-t', 'ext4'], cwd='/home/op1/MCS/tp', stdin=None, stdout=subprocess.PIPE)
        p.wait()
        
        for line in iter(p.stdout.readline, ''):
            line = line.split()
            try:
                if line[6]==("['%s']" % (barcode)):
                    path = line[2]
            except:
                pass
                
    except:
        pass
        
    return path


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


def buildPayload(source, cmd, data=None, refSocket=None):
    if refSocket is None:
        ref = 1
    else:
        refSocket.send(b"next_ref")
        ref = int(refSocket.recv(), 10)
        
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
    tPoll = time.time()
    zinfo = zeroconf.get_service_info("_sccs._udp.local.", "Smart copy server._sccs._udp.local.")
    while time.time() - tPoll <= 10.0 and zinfo is None:
        time.sleep(1)
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
        refPort = int(zinfo.properties['MESSAGEREFPORT'], 10)
        
        context = zmq.Context()
        sockRef = context.socket(zmq.REQ)
        sockRef.connect("tcp://%s:%i" % (outHost, refPort))
        
        infs = []
        cmds = []
        destUser = args.ucfuser[0]
        
        # Validate the UCF username
        try:
            output = subprocess.check_output(['ssh', 'mcsdr@lwaucf0', 'ls /data/network/recent_data/%s' % destUser])
        except subprocess.CalledProcessError:
            if destUser[:4] == 'eLWA':
                try:
                    output = subprocess.check_output(['ssh', 'mcsdr@lwaucf0', 'mkdir -p /data/network/recent_data/%s' % destUser])
                    print("NOTE: auto-created eLWA path: %s" % destUser)
                except subprocess.CalledProcessError:
                    raise RuntimeError("Could not auto-create eLWA path: %s" % destUser)
            elif destUser == 'original':
                ## This is ok since it just tells the script to use the orginal file path
                pass
            else:
                raise RuntimeError("Invalid UCF username/path: %s" % destUser)
                
        # Process the input files
        metadataDone = []
        for filename in args.filename:
            ## Parse the metadata
            try:
                filetags, barcodes, beam, date, isSpec, origPath = parseMetadata(filename)
            except KeyError:
                print("WARNING: could not parse '%s', skipping" % os.path.basename(filename))
                continue
                
            ## Go!
            inHost = "DR%i" % beam
            _drPathCache = {}
            _done = []
            for oid,(filetag,barcode) in enumerate(zip(filetags, barcodes)):
                ### Make sure we have a valid tag
                if filetag in ('', 'UNK'):
                    print("WARNING: invalid filetag '%s' for observation %i of '%s', skipping" % (filetag, oid+1, filename))
                    continue
                    
                ### See if we should transfer this file
                if len(args.observations) > 0:
                    if oid not in args.observations:
                        continue
                        
                ### See if we have already transferred this file
                if filetag in _done:
                    continue
                    
                ### Get the path on the DR
                try:
                    drPath = _drPathCache[(beam,barcode)]
                except KeyError:
                    _drPathCache[(beam,barcode)] = getDRSUPath(beam, barcode)
                    drPath = _drPathCache[(beam,barcode)]
                    
                ### Make the copy
                if drPath is None:
                    print("WARNING: could not find path for DRSU '%s' on DR%i, skipping" % (barcode, beam))
                    continue
                    
                srcPath= "%s:%s/DROS/%s/%s" % (inHost, drPath, 'Spec' if isSpec else 'Rec', filetag)
                if destUser == 'original':
                    if origPath is None:
                        print("WARNING: no original path found for '%s', skipping" % filetag)
                        continue
                    destPath = "%s:/mnt/network/recent_data/%s" % (inHost, origPath)
                else:
                    destPath = "%s:/mnt/network/recent_data/%s" % (inHost, destUser)
                
                try:
                    host, hostpath = srcPath.split(':', 1)
                except ValueError:
                    host, hostpath = '', srcPath
                if host == '':
                    host = inHost
                    hostpath = os.path.abspath(hostpath)
                    
                try:
                    dest, destpath = destPath.split(':', 1)
                except ValueError:
                    dest, destpath = '', destPath
                if dest == '':
                    dest = inHost
                    destpath = os.path.abspath(destpath)
                    
                infs.append( "Queuing copy for %s:%s to %s:%s" % (host, hostpath, dest, destpath) )
                cmds.append( buildPayload(inHost, "SCP", data="%s:%s->%s:%s" % (host, hostpath, dest, destpath), refSocket=sockRef) )
                
                if args.metadata:
                    mtdPath = os.path.abspath(filename)
                    if destUser == 'original':
                        destPath = "%s:/mnt/network/recent_data/%s" % (inHost, origPath)
                    else:
                        destPath = "%s:/data/network/recent_data/%s" % ('lwaucf0', destUser)
                        
                    try:
                        dest ,destpath = destPath.split(':', 1)
                    except ValueError:
                        dest, destpath = '', destPath
                    if dest == '':
                        dest = 'lwaucf0'
                        destpath = os.path.abspath(destpath)
                        
                    if mtdPath not in metadataDone:
                        infs.append( "Copying metadata %s to %s:%s" % (filename, dest, destpath) )
                        cmds.append( ["rsync", "-e ssh", "-avH", mtdPath, "mcsdr@%s:%s" % (dest, destpath)] )
                        metadataDone.append( mtdPath )
                        
                ### Update the done list
                _done.append( filetag )
                
        try:
            sockOut = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sockOut.settimeout(5)
            sockIn  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sockIn.bind(("0.0.0.0", inPort))
            sockIn.settimeout(5)
            
            for inf,cmd in zip(infs,cmds):
                print(inf)
                
                if inf[:4] != 'Copy':
                    ## Standard SmartCopy commands
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
                            
                else:
                    ## Custom metadata scp command
                    try:
                        output = subprocess.check_output(cmd)
                        print("  Done")
                    except subprocess.CalledProcessError:
                        print("  WARNING: failed to copy metadata, skipping")
                    
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
        description='Parse a metadata file and queue the data copy for later',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('filename', type=str, nargs='+',
                        help='metadata to examine')
    parser.add_argument('ucfuser', type=str, nargs=1,
                        help='destination to copy to')
    parser.add_argument('-v', '--version', action='store_true', 
                        help='display version information')
    parser.add_argument('-o', '--observations', type=str, default='-1',
                        help='comma separated list of obseration numbers to transfer (one based; -1 = tranfer all obserations)')
    parser.add_argument('-m', '--metadata', action='store_true',
                        help='include the metadata with the copy')
    args = parser.parse_args()
    if args.observations == '-1':
        args.observations = []
    else:
        args.observations = [int(v,10)-1 for v in args.observations.split(',')]
    main(args)
    