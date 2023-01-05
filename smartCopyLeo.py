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

from lsl.common import mcs, metabundle, metabundleADP


SITE = socket.gethostname().split('-', 1)[0]
DEFAULT_PATH = '/data2/from_%s/' % SITE


_usernameRE = re.compile(r'ucfuser:[ \t]*(?P<username>[a-zA-Z1]+)(\/(?P<subdir>[a-zA-Z0-9\/\+\-_]+))?')


def parseMetadata(tarname):
    """
    Given a filename for a metadata tarball, parse the file and return a
    seven-element tuple of:
     * project code
     * filetag
     * DRSU barcode
     * beam
     * date
     * if the data is spectrometer or not
     * originally requested UCF copy path or None if there was none
    """
    
    try:
        parser = metabundle
        parser.get_session_spec(tarname)
    except Exception as e:
        parser = metabundleADP
        parser.get_session_spec(tarname)
            
    project = parser.get_sdf(tarname)
    project_id = project.id
    isSpec = False
    if project.sessions[0].observations[0].mode not in ('TBW', 'TBN'):
        if project.sessions[0].spcSetup[0] != 0 and project.sessions[0].spcSetup[1] != 0:
            isSpec = True
            
    meta = parser.get_session_metadata(tarname)
    tags = [meta[id]['tag'] for id in sorted(meta.keys())]
    barcodes = [meta[id]['barcode'] for id in sorted(meta.keys())]
    meta = parser.get_session_spec(tarname)
    beam = meta['drx_beam']
    date = mcs.mjdmpm_to_datetime(int(meta['mjd']), int(meta['mpm']))
    datestr = date.strftime("%y%m%d")
    
    userpath = None
    if project.sessions[0].data_return_method == 'UCF':
        mtch = _usernameRE.search(project.sessions[0].comments)
        if mtch is not None:
            userpath = mtch.group('username')
            if mtch.group('subdir') is not None:
                userpath = os.path.join(userpath, mtch.group('subdir'))
                
    return project_id, tags, barcodes, beam, date, isSpec, userpath


def getDRSUPath(beam, barcode):
    """
    Given a beam (data recorder) number and a DRSU barcode, convert the 
    barcode to a path on the data recorder.  Returns None if the DRSU cannot
    be found.
    """
    
    path = None
    
    try:
        p = subprocess.Popen(['ssh', "mcsdr@dr%s" % beam, 'mount', '-l', '-t', 'ext4'], cwd='/home/op1/MCS/tp', stdin=None, stdout=subprocess.PIPE)
        output, _ = p.communicate()
        output = output.decode()
        
        for line in output.split('\n'):
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


def buildPayload(source, cmd, data=None, refSocket=None):
    if refSocket is None:
        ref = 1
    else:
        try:
            refSocket.send(b"next_ref")
            ref = int(refSocket.recv(), 10)
        except zmq.ZMQError as e:
            raise RuntimeError("Cannot access reference ID server: %s" % str(e))
            
    mjd, mpm = get_time()
    
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
    destPath = 'mcsdr@leo10g.unm.edu:%s' % DEFAULT_PATH
    
    # Process the input files
    for filename in args.filename:
        ## Parse the metadata
        try:
            project_id, filetags, barcodes, beam, date, isSpec, origPath = parseMetadata(filename)
        except KeyError:
            print("WARNING: could not parse '%s', skipping" % os.path.basename(filename))
            continue
            
        ## Go!
        _drPathCache = {}
        _done = []
        for oid,(filetag,barcode) in enumerate(zip(filetags, barcodes)):
            ## Make sure we have a valid tag
            if filetag in ('', 'UNK'):
                print("WARNING: invalid filetag '%s' for '%s', skipping" % (filetag, filename))
                continue
                
            ## Make sure we have spectrometer data
            if not isSpec and project_id not in args.allowed_projects:
                print("WARNING: '%s' has non-spectrometer data, skipping" % os.path.basename(filename))
                continue
                
            ### See if we should transfer this file
            if len(args.observations) > 0:
                if oid not in args.observations:
                    continue
                    
            ### See if we have already transferred this file
            if filetag in _done:
                continue
                
            ## Get the path on the DR
            drPath = getDRSUPath(beam, barcode)
            
            ## Make the copy
            if drPath is None:
                ### Problem
                print("WARNING: could not find path for DRSU '%s' on DR%i, skipping" % (barcode, beam))
                continue
            else:
                ### Everything is ok
                inHost = "DR%i" % beam
                srcPath= "%s:%s/DROS/%s/%s" % (inHost, drPath, 'Spec' if isSpec else 'Rec', filetag)
                
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
            
            ## Standard SmartCopy commands
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
        description='Parse a metadata file and queue the data copy for later',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('filename', type=str, nargs='+',
                        help='metadata to examine')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s', 
                        help='display version information')
    parser.add_argument('-o', '--observations', type=str, default='-1',
                        help='comma separated list of obseration numbers to transfer (one based; -1 = tranfer all obserations)')
    parser.add_argument('-m', '--metadata', action='store_true',
                        help='include the metadata with the copy')
    parser.add_argument('-a', '--allowed-projects', type=str,
                        help='comma separated list of projects to copy non-spectrometer data for')
    args = parser.parse_args()
    if args.observations == '-1':
        args.observations = []
    else:
        args.observations = [int(v,10)-1 for v in args.observations.split(',')]
    if args.allowed_projects is None:
        args.allowed_projects = []
    else:
        args.allowed_projects = [v for v in args.allowed_projects.split(',')]
    main(args)
    
