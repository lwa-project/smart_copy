#!/usr/bin/env python3

import os
import re
import sys
import socket
import argparse
import netifaces
import subprocess

from emcs import Client

from lsl.common import sdf, mcs, metabundle, metabundleADP

#
# Site Name
#
SITE = socket.gethostname().split('-', 1)[0]


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
        parser.get_session_spec(tarname)
    except Exception as e:
        parser = metabundleADP
        parser.get_session_spec(tarname)
            
    project = parser.get_sdf(tarname)
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
        mtch = sdf.UCF_USERNAME_RE.search(project.sessions[0].comments)
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
        output, _ = p.communicate()
        output = output.decode()
        
        for line in output.split('\n'):
            line = line.split()
            try:
                if line[6]==("['%s']" % (barcode)):
                    path = line[2]
                    path = path.replace('Internal/0', 'Internal/*')
                    path = path.replace('Internal/1', 'Internal/*')
                    path = path.replace('Internal/2', 'Internal/*')
                    path = path.replace('Internal/3', 'Internal/*')
            except:
                pass
                
    except:
        pass
        
    return path


def get_server_address():
    """
    Return the IP address of the smart copy server by looking for an interface
    on a 10.1.x.0 network.
    """
    
    for interface in netifaces.interfaces():
        addrs = netifaces.ifaddresses(interface)
        if netifaces.AF_INET in addrs:
            for addr in addrs[netifaces.AF_INET]:
                ip = addr['addr']
                if ip.startswith('10.1.'):
                    network = '.'.join(ip.split('.')[:3])
                    return f"{network}.2"
    raise RuntimeError("Could not find 10.1.x.0 network interface")


def main(args):
    # Connect to the smart copy command server
    outHost = get_server_address()
    outPort = 5050
    try:
        inHost = socket.gethostname().split('-')[1].upper()
    except IndexError:
        inHost = socket.gethostname().upper()
    inHost = inHost[:3]
    while len(inHost) < 3:
        inHost += "_"
        
    infs = []
    cmds = []
    # Process the input files
    for filename in args.filename:
        ## Parse the metadata
        try:
            filetags, barcodes, beam, date, isSpec, origPath = parseMetadata(filename)
        except KeyError as e:
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
                
            ### Make the delete
            if drPath is None:
                print("WARNING: could not find path for DRSU '%s' on DR%i, skipping" % (barcode, beam))
                continue
                
            srcPath= "%s:%s/DROS/%s/%s" % (inHost, drPath, 'Spec' if isSpec else 'Rec', filetag)
            
            yesno = input("remove %s? " % srcPath)
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
            if args.now:
                flag = '-tNOW '
                
            infs.append( "Queuing delete for %s:%s " % (host, hostpath) )
            cmds.append( ("SRM", data="%s%s:%s" % (flag, host, hostpath)) )
            
            ### Update the done list
            _done.append( filetag )
            
    mcs_client = Client(server_address=(outHost, outPort), subsystem=inHost)
    mcs_client.start()
    
    for inf,cmd in zip(infs,cmds):
        print(inf)
        
        try:
            resp = mcs_client.send_command('SCM', cmd[0], data=cmd[1])
            print(resp, resp.reference)
            print(resp['data']['accepted'], resp['data']['status'], resp['data']['data'])
        except Exception as e:
            print(f"ERROR on '{inf}': {str(e)}")
        
    mcs_client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse a metadata file and queue the data deletion for later.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('filename', type=str, nargs='+',
                        help='metadata to examine')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s',
                        help='display version information')
    parser.add_argument('-o', '--observations', type=str, default='-1',
                        help='comma separated list of obseration numbers to transfer (one based; -1 = tranfer all obserations)')
    parser.add_argument('-n', '--now', action='store_true',
                        help='request that the delete(s) be executed as soon as the command(s) reach the front of the queue')
    args = parser.parse_args()
    if args.observations == '-1':
        args.observations = []
    else:
        args.observations = [int(v,10)-1 for v in args.observations.split(',')]
    main(args)
