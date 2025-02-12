#!/usr/bin/env python3

import os
import re
import sys
import socket
import argparse
import netifaces
import subprocess

from emcs import Client

from lsl.common import mcs, metabundle, metabundleADP

#
# Site Name and default path
#
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
    destPath = 'mcsdr@leo.phys.unm.edu:%s' % DEFAULT_PATH
    
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
                cmds.append( ("SCP", "%s:%s->%s:%s" % (host, hostpath, dest, destpath), refSocket=sockRef)) )
                
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
