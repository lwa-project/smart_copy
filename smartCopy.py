#!/usr/bin/env python3

import os
import re
import sys
import time
import socket
import argparse
import netifaces
import subprocess

from emcs import Client

#
# Site Name
#
SITE = socket.gethostname().split('-', 1)[0]


# Regular expresion to check for remote paths
REMOTE_PATH_RE = re.compile(r'^((?P<user>[a-zA-Z0-9]+)\@)?(?P<host>[a-zA-Z0-9\-]+)(?<!\\)\:')


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
        cmds.append( ("SCP", "%s:%s->%s:%s" % (host, hostpath, dest, destpath)) )
        
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
