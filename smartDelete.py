#!/usr/bin/env python3

import os
import sys
import socket
import argparse
import netifaces
from datetime import datetime

from emcs import Client

#
# Site Name
#
SITE = socket.gethostname().split('-', 1)[0]


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
    srcPaths = args.filename
    for srcPath in srcPaths:
        try:
            host, hostpath = srcPath.split(':', 1)
        except ValueError:
            host, hostpath = '', srcPath
        if host == '':
            host = inHost
            hostpath = os.path.abspath(hostpath)
            
        if host == inHost:
            if not os.path.exists(hostpath):
                raise RuntimeError("Source file '%s' does not exist" % hostpath)
                
        flag = ''
        if args.now:
            flag = '-tNOW '
            
        infs.append( "Queuing delete for %s:%s" % (host, hostpath) )
        cmds.append( ("SRM", "%s%s:%s" % (flag, host, hostpath)) )
        
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
        description='Queue a file deletion for later.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('filename', type=str, nargs='+',
                        help='filename to delete')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s', 
                        help='display version information')
    parser.add_argument('-n', '--now', action='store_true',
                        help='request that the delete(s) be executed as soon as the command(s) reach the front of the queue')
    args = parser.parse_args()
    main(args)
