#!/usr/bin/env python3

import os
import re
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
        
    infs = []
    cmds = []
    for query in args.query:
        infs.append( "Querying '%s'" % query )
        cmds.append( ('RPT', query) )
        
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
    def mib(value):
        _queryRE = re.compile(r'(?P<name>[A-Z_]+)(?P<number>\d+)')
        _valid_names = ['OBSSTATUS_DR', 'QUEUE_SIZE_DR', 'QUEUE_STATUS_DR',
                        'QUEUE_ENTRY_', 'ACTIVE_ID_DR', 'ACTIVE_STATUS_DR',
                        'ACTIVE_BYTES_DR', 'ACTIVE_PROGRESS_DR', 
                        'ACTIVE_SPEED_DR', 'ACTIVE_REMAINING_DR']
        mtch = _queryRE.match(value)
        if mtch is None:
            _valid_names = ['SUMMARY', 'INFO', 'LASTLOG', 'SUBSYSTEM',
                            'SERIALNO', 'VERSION']
            if value not in _valid_names:
                raise argparse.ArgumentError
        else:
            if mtch.group('name') not in _valid_names:
                raise argparse.ArgumentError
        return value
        
    parser = argparse.ArgumentParser(
        description='Query the smart copy server about its state',
        epilog='Valid MIB entries are: OBSSTATUS_DR# - whether or not DR# is recording data, QUEUE_SIZE_DR# - size of the copy queue on DR#, QUEUE_STATUS_DR# - status of the copy queue on DR#, QUEUE_ENTRY_# - details of a copy command entry, ACTIVE_ID_DR# - active copy command queue ID on DR#, ACTIVE_STATUS_DR# - active copy command status/command on DR#, ACTIVE_BYTES_DR# - active copy bytes transferred on DR#, ACTIVE_PROGRESS_DR# - active copy progress on DR#, ACTIVE_SPEED_DR#- active copy speed on DR#, and ACTIVE_REMAINING_DR# - active copy time remaining on DR#.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('query', type=mib, nargs='+',
                        help='MIB to query')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s', 
                        help='display version information')
    args = parser.parse_args()
    main(args)
