#!/usr/bin/env python3
import os
import sys
import socket
import argparse
import netifaces

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
        
    cmds = []
    nDR = 5 if SITE == 'lwa1' else 4
    for i in range(1, nDR+1):
        dr = 'DR%i' % i
        if args.all or dr in args.DR:
            cmds.append( ("RES", dr) )
            
    mcs_client = Client(server_address=(outHost, outPort), subsystem=inHost)
    mcs_client.start()
    
    for cmd in cmds:
        try:
            resp = mcs_client.send_command('SCM', cmd[0], data=cmd[1])
            print(resp, resp.reference)
            print(resp['data']['accepted'], resp['data']['status'], resp['data']['data'])
        except Exception as e:
            print(f"ERROR on '{cmd}': {str(e)}")
            
    mcs_client.close()


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
