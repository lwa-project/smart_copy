#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import zmq
import math
import time
import getopt
import socket
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


def usage(exitCode=None):
    print """smartDeleteHelper.py - Parse a metadata file and queue the data deletion for later

Usage: smartDeleteHelper.py [OPTIONS] metadata [metadata [...]]

Options:
-h, --help         Display this help information
-v, --version      Display version information
-o, --obsevations  Comma separated list of obseration numbers to transfer
                (one based; default = -1 = tranfer all obserations)
-n, --now          Request that the delete(s) be executed as soon as the 
                command(s) reach the front of the queue (default = no,
                queue for batch delete
-q, --query        Query the smart copy server

Note:
  For -q/--query calls, valid MIB entries are:
    OBSSTATUS_DR# - whether or not DR# is recording data
    
    QUEUE_SIZE_DR# - size of the copy queue on DR#
    QUEUE_STATUS_DR# - status of the copy queue on DR#
    QUEUE_ENTRY_# - details of a copy command entry
    
    ACTIVE_ID_DR# - active copy command queue ID on DR#
    ACTIVE_STATUS_DR# - active copy command status/command on DR#
    ACTIVE_BYTES_DR# - active copy bytes transferred on DR#
    ACTIVE_PROGRESS_DR# - active copy progress on DR#
    ACTIVE_SPEED_DR#- active copy speed on DR#
    ACTIVE_REMAINING_DR# - active copy time remaining on DR#
"""

    if exitCode is not None:
        sys.exit(exitCode)
    else:
        return True


def parseOptions(args):
    """
    Parse the command line options and return a dictionary of the configuration
    """

    config = {}
    # Default parameters
    config['version'] = False
    config['obsID'] = -1
    config['now'] = False
    config['query'] = False
    config['args'] = []
    
    # Read in and process the command line flags
    try:
        opts, args = getopt.getopt(args, "hvo:nq", ["help", "version", "observations=", "now", "query"])
    except getopt.GetoptError, err:
        # Print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage(exitCode=2)
        
    # Work through opts
    for opt, value in opts:
        if opt in ('-h', '--help'):
            usage(exitCode=0)
        elif opt in ('-v', '--version'):
            config['version'] = True
        elif opt in ('-o', '--observations'):
            config['obsID'] = [int(v,10)-1 for v in value.split(',')]
        elif opt in ('-n', '--now'):
            config['now'] = True
        elif opt in ('-q', '--query'):
            config['query'] = True
        else:
            assert False
    
    # Add in arguments
    config['args'] = args
    
    # Validate
    if config['query'] and len(config['args']) != 1:
        raise RuntimeError("Only one argument is allowed for query operations")
    if not config['query'] and not config['version'] and len(config['args']) < 1:
        raise RuntimeError("Must specify at least one metadata file")
        
    # Return configuration
    return config


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
    # Parse the command line
    config = parseOptions(args)
    
    # Connect to the smart copy command server
    zeroconf = Zeroconf()
    tPoll = time.time()
    zinfo = zeroconf.get_service_info("_sccs._udp.local.", "Smart copy server._sccs._udp.local.")
    while time.time() - tPoll <= 10.0 and zinfo is None:
        time.sleep(1)
        zinfo = zeroconf.get_service_info("_sccs._udp.local.", "Smart copy server._sccs._udp.local.")
    if zinfo is None:
        raise RuntimeError("Cannot find the smart copy command server")
        
    if config['version']:
        ## Smart copy command server info
        print zinfo
        
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
        if config['query']:
            infs.append( "Querying '%s'" % config['args'][0] )
            cmds.append( buildPayload(inHost, 'RPT', data=config['args'][0], refSocket=sockRef) )
            
        else:
            filenames = config['args']
            
            # Process the input files
            for filename in filenames:
                ## Parse the metadata
                try:
                    filetags, barcodes, beam, date, isSpec, origPath = parseMetadata(filename)
                except KeyError:
                    print "WARNING: could not parse '%s', skipping" % os.path.basename(filename)
                    continue
                    
                ## Go!
                inHost = "DR%i" % beam
                _drPathCache = {}
                _done = []
                for oid,(filetag,barcode) in enumerate(zip(filetags, barcodes)):
                    ### Make sure we have a valid tag
                    if filetag in ('', 'UNK'):
                        print "WARNING: invalid filetag '%s' for observation %i of '%s', skipping" % (filetag, oid+1, filename)
                        continue
                        
                    ### See if we should transfer this file
                    if config['obsID'] != -1:
                        if oid not in config['obsID']:
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
                        
                    ### Make the delete
                    if drPath is None:
                        print "WARNING: could not find path for DRSU '%s' on DR%i, skipping" % (barcode, beam)
                        continue
                        
                    srcPath= "%s:%s/DROS/%s/%s" % (inHost, drPath, 'Spec' if isSpec else 'Rec', filetag)
                    
                    yesno = raw_input("remove %s? " % srcPath)
                    if yesno.lower() not in ('y', 'yes'):
                        ### Update the done list
                        _done.append( filetag )
                        continue
                        
                    try:
                        host, hostpath = srcPath.split(':', 1)
                    except ValueError:
                        host, hostpath = '', srcPath
                    if host == '':
                        host = inHost
                        hostpath = os.path.abspath(hostpath)
                        
                    flag = ''
                    if config['now']:
                        flag = '-tNOW '
                        
                    infs.append( "Queuing delete for %s:%s " % (host, hostpath) )
                    cmds.append( buildPayload(inHost, "SRM", data="%s%s:%s" % (flag, host, hostpath), refSocket=sockRef) )
                    
                    ### Update the done list
                    _done.append( filetag )
        try:
            sockOut = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sockOut.settimeout(5)
            sockIn  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sockIn.bind(("0.0.0.0", inPort))
            sockIn.settimeout(5)
            
            for inf,cmd in zip(infs,cmds):
                print inf
                
                sockOut.sendto(cmd, (outHost, outPort))
                data, address = sockIn.recvfrom(MCS_RCV_BYTES)
                
                cStatus, sStatus, info = parsePayload(data)
                info = info.split('\n')
                if len(info) == 1:
                    print cStatus, sStatus, info[0]
                else:
                    print cStatus, sStatus
                    for line in info:
                        print "  %s" % line
                        
            sockIn.close()
            sockOut.close()
        except socket.error as e:
            raise RuntimeError(str(e))
            
        sockRef.close()
        context.term()
        
    zeroconf.close()
    time.sleep(0.1)


if __name__ == "__main__":
    main(sys.argv[1:])
    
