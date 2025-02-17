#!/usr/bin/env python3

import os
import re
import git
import sys
import json
import time
import signal
import socket
import string
import struct
import logging
import argparse
import json_minify
try:
    from logging.handlers import WatchedFileHandler
except ImportError:
    from logging import FileHandler as WatchedFileHandler
import traceback
from io import StringIO
from collections import deque

from lwa_auth.tools import load_json_config

from MCS import *
from smartFunctions import SmartCopy

__version__ = '0.5'
__all__ = ['MCSCommunicate',]

#
# Site Name
#
SITE = socket.gethostname().split('-', 1)[0]


#
# Default Configuration File
#
DEFAULTS_FILENAME = '/lwa/software/defaults.json'


class MCSCommunicate(Communicate):
    """
    Class to deal with the communcating with MCS.
    """
    
    # Setup the command status dictionary, indexed by slot time in seconds (MPM/1000)
    commandStatus = {}
    
    def __init__(self, SubSystemInstance, config, opts):
            super(MCSCommunicate, self).__init__(SubSystemInstance, config, opts)
            
    def processCommand(self, data):
        """
        Interperate the data of a UDP packet as a SHL MCS command.
        """
        
        destination, sender, command, reference, datalen, mjd, mpm, data, address = self.parsePacket(data)
        
        self.logger.debug('Got command %s from %s: ref #%i', command, sender, reference)
    
        # check destination and sender
        if destination in (self.SubSystemInstance.subSystem, 'ALL'):
            # Calculate the fullSlotTime
            fullSlotTime = int(time.time())
            
            # PNG
            if command == 'PNG':
                status = True
                packed_data = ''
            
            # Report various MIB entries
            elif command == 'RPT':
                status = True
                packed_data = ''
                
                ## General Info.
                if data == 'SUMMARY':
                    summary = self.SubSystemInstance.currentState['status'][:7]
                    self.logger.debug('summary = %s', summary)
                    packed_data = summary
                elif data == 'INFO':
                    ### Trim down as needed
                    if len(self.SubSystemInstance.currentState['info']) > 256:
                        infoMessage = "%s..." % self.SubSystemInstance.currentState['info'][:253]
                    else:
                        infoMessage = self.SubSystemInstance.currentState['info'][:256]
                        
                    self.logger.debug('info = %s', infoMessage)
                    packed_data = infoMessage
                elif data == 'LASTLOG':
                    ### Trim down as needed
                    if len(self.SubSystemInstance.currentState['lastLog']) > 256:
                        lastLogEntry = "%s..." % self.SubSystemInstance.currentState['lastLog'][:253]
                    else:
                        lastLogEntry =  self.SubSystemInstance.currentState['lastLog'][:256]
                    if len(lastLogEntry) == 0:
                        lastLogEntry = 'no log entry'
                    
                    self.logger.debug('lastlog = %s', lastLogEntry)
                    packed_data = lastLogEntry
                elif data == 'SUBSYSTEM':
                    self.logger.debug('subsystem = %s', self.SubSystemInstance.subSystem)
                    packed_data = self.SubSystemInstance.subSystem
                elif data == 'SERIALNO':
                    self.logger.debug('serialno = %s', self.SubSystemInstance.serialNumber)
                    packed_data = self.SubSystemInstance.serialNumber
                elif data == 'VERSION':
                    self.logger.debug('version = %s', self.SubSystemInstance.version)
                    packed_data = self.SubSystemInstance.version
                    
                ## Observing status
                elif data.startswith('OBSSTATUS_'):
                    _, value = data.split('_', 1)
                    
                    status, packed_data = self.SubSystemInstance.getDRRecordState(value)
                    if status:
                        packed_data = str(packed_data)
                    else:
                        packed_data = self.SubSystemInstance.currentState['lastLog']
                        
                ## Queue status
                elif data.startswith('QUEUE_'):
                    _, prop, value = data.split('_', 2)
                    
                    if prop == 'SIZE':
                        status, packed_data = self.SubSystemInstance.getDRQueueSize(value)
                        if status:
                            packed_data = str(packed_data)
                        else:
                            packed_data = self.SubSystemInstance.currentState['lastLog']
                            
                    elif prop == 'STATS':
                        status, packed_data = self.SubSystemInstance.getDRQueueStats(value)
                        if status:
                            packed_data = str(packed_data)
                        else:
                            packed_data = self.SubSystemInstance.currentState['lastLog']
                            
                    if prop == 'STATUS':
                        status, packed_data = self.SubSystemInstance.getDRQueueState(value)
                        if status:
                            packed_data = str(packed_data)
                        else:
                            packed_data = self.SubSystemInstance.currentState['lastLog']
                            
                    elif prop == 'ENTRY':
                        status, packed_data = self.SubSystemInstance.getCopyCommand(value)
                        if status:
                            packed_data = str(packed_data)
                        else:
                            packed_data = self.SubSystemInstance.currentState['lastLog']
                            
                    else:
                        status = False
                        packed_data = 'Unknown MIB entry: %s' % data
                        
                ## Active status
                elif data.startswith('ACTIVE_'):
                    _, prop, value = data.split('_', 2)
                    
                    if prop == 'ID':
                        status, packed_data = self.SubSystemInstance.getActiveCopyID(value)
                        if status:
                            packed_data = str(packed_data)
                        else:
                            packed_data = self.SubSystemInstance.currentState['lastLog']
                            
                    elif prop == 'STATUS':
                        status, packed_data = self.SubSystemInstance.getActiveCopyStatus(value)
                        if status:
                            packed_data = str(packed_data)
                        else:
                            packed_data = self.SubSystemInstance.currentState['lastLog']
                            
                    elif prop == 'BYTES':
                        status, packed_data = self.SubSystemInstance.getActiveCopyBytes(value)
                        if status:
                            packed_data = str(packed_data)
                        else:
                            packed_data = self.SubSystemInstance.currentState['lastLog']
                            
                    elif prop == 'PROGRESS':
                        status, packed_data = self.SubSystemInstance.getActiveCopyProgress(value)
                        if status:
                            packed_data = str(packed_data)
                        else:
                            packed_data = self.SubSystemInstance.currentState['lastLog']
                            
                    elif prop == 'SPEED':
                        status, packed_data = self.SubSystemInstance.getActiveCopySpeed(value)
                        if status:
                            packed_data = str(packed_data)
                        else:
                            packed_data = self.SubSystemInstance.currentState['lastLog']
                            
                    elif prop == 'REMAINING':
                        status, packed_data = self.SubSystemInstance.getActiveCopyRemaining(value)
                        if status:
                            packed_data = str(packed_data)
                        else:
                            packed_data = self.SubSystemInstance.currentState['lastLog']
                            
                    else:
                        status = False
                        packed_data = 'Unknown MIB entry: %s' % data
                        
                ## Unknown MIB entries
                else:
                    status = False
                    self.logger.debug('%s = error, unknown entry', data)
                    packed_data = 'Unknown MIB entry: %s' % data
                    
            #
            # Control Commands
            #
            
            # INI
            elif command == 'INI':
                # Go
                status, exitCode = self.SubSystemInstance.ini(refID=reference)
                if status:
                    packed_data = ''
                else:
                    packed_data = "0x%02X! %s" % (exitCode, self.SubSystemInstance.currentState['lastLog'])
                    
                # Update the list of command executed
                try:
                    self.commandStatus[fullSlotTime].append( ('INI', reference, exitCode) )
                except KeyError:
                    self.commandStatus[fullSlotTime] = [('INI', reference, exitCode), ]
                    
            # SHT
            elif command == 'SHT':
                status, exitCode = self.SubSystemInstance.sht(mode=data)
                if status:
                    packed_data = ''
                else:
                    packed_data = "0x%02X! %s" % (exitCode, self.SubSystemInstance.currentState['lastLog'])
                    
                # Update the list of command executed
                try:
                    self.commandStatus[fullSlotTime].append( ('SHT', reference, exitCode) )
                except KeyError:
                    self.commandStatus[fullSlotTime] = [('SHT', reference, exitCode), ]
                    
            # SCP
            elif command == 'SCP':
                src, dest = data.split('->', 1)
                host, hostpath = re.split(r'(?<!\\)\:', src, 1)
                dest, destpath = re.split(r'(?<!\\)\:', dest, 1)
                
                status, exitCode = self.SubSystemInstance.addCopyCommand(host, host, hostpath, dest, destpath)
                if status:
                    packed_data = exitCode
                    exitCode = 0x00
                else:
                    packed_data = "0x%02X! %s" % (exitCode, self.SubSystemInstance.currentState['lastLog'])
                    
                # Update the list of command executed
                try:
                    self.commandStatus[fullSlotTime].append( ('SCP', reference, exitCode) )
                except KeyError:
                    self.commandStatus[fullSlotTime] = [('SCP', reference, exitCode), ]
                    
            # PAU
            elif command == 'PAU':
                dr = data
                
                status, exitCode = self.SubSystemInstance.pauseCopyQueue(dr)
                if status:
                    packed_data = str(exitCode)
                    exitCode = 0x00
                else:
                    packed_data = "0x%02X! %s" % (exitCode, self.SubSystemInstance.currentState['lastLog'])
                    
                # Update the list of command executed
                try:
                    self.commandStatus[fullSlotTime].append( ('PAU', reference, exitCode) )
                except KeyError:
                    self.commandStatus[fullSlotTime] = [('PAU', reference, exitCode), ]
                    
            # RES
            elif command == 'RES':
                dr = data
                
                status, exitCode = self.SubSystemInstance.resumeCopyQueue(dr)
                if status:
                    packed_data = str(exitCode)
                    exitCode = 0x00
                else:
                    packed_data = "0x%02X! %s" % (exitCode, self.SubSystemInstance.currentState['lastLog'])
                    
                # Update the list of command executed
                try:
                    self.commandStatus[fullSlotTime].append( ('RES', reference, exitCode) )
                except KeyError:
                    self.commandStatus[fullSlotTime] = [('RES', reference, exitCode), ]
                    
            # SCN
            elif command == 'SCN':
                id = data
                
                status, exitCode = self.SubSystemInstance.cancelCopyCommand(id)
                if status:
                    packed_data = exitCode
                    exitCode = 0x00
                else:
                    packed_data = "0x%02X! %s" % (exitCode, self.SubSystemInstance.currentState['lastLog'])
                    
                # Update the list of command executed
                try:
                    self.commandStatus[fullSlotTime].append( ('SCN', reference, exitCode) )
                except KeyError:
                    self.commandStatus[fullSlotTime] = [('SCN', reference, exitCode), ]
                    
            # DEL
            elif command == 'SRM':
                now = False
                if data[:5] == '-tNOW':
                    now = True
                    data = data.split('-tNOW', 1)[1]
                    data = data.strip()
                host, hostpath = re.split(r'(?<!\\)\:', data, 1)
                
                status, exitCode = self.SubSystemInstance.addDeleteCommand(host, host, hostpath, now=now)
                if status:
                    packed_data = exitCode
                    exitCode = 0x00
                else:
                    packed_data = "0x%02X! %s" % (exitCode, self.SubSystemInstance.currentState['lastLog'])
                    
                # Update the list of command executed
                try:
                    self.commandStatus[fullSlotTime].append( ('SRM', reference, exitCode) )
                except KeyError:
                    self.commandStatus[fullSlotTime] = [('SRM', reference, exitCode), ]
                    
            # 
            # Unknown command catch
            #
            
            else:
                status = False
                self.logger.debug('%s = error, unknown command', command)
                packed_data = 'Unknown command: %s' % command
                
            # Prune command status list of old values
            for previousSlotTime in list(self.commandStatus.keys())[:-4]:
                del self.commandStatus[previousSlotTime]
                
            # Return status, command, reference, and the result
            return sender, status, command, reference, packed_data, address


def main(args):
    """
    Main function of smart_cmnd.py.  This sets up the various configuation options 
    and start the UDP command handler.
    """
    
    # Setup logging
    logger = logging.getLogger(__name__)
    logFormat = logging.Formatter('%(asctime)s.%(msecs)03d [%(levelname)-8s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logFormat.converter = time.gmtime
    if args.log is None:
        logHandler = logging.StreamHandler(sys.stdout)
    else:
        logHandler = WatchedFileHandler(args.log)
    logHandler.setFormatter(logFormat)
    logger.addHandler(logHandler)
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        
    # Get current MJD and MPM
    mjd, mpm = getTime()
    
    # Git information
    try:
        repo = git.Repo(os.path.dirname(os.path.abspath(__file__)))
        branch = repo.active_branch.name
        hexsha = repo.active_branch.commit.hexsha
        shortsha = hexsha[-7:]
        dirty = ' (dirty)' if repo.is_dirty() else ''
    except git.exc.GitError:
        branch = 'unknown'
        hexsha = 'unknown'
        shortsha = 'unknown'
        dirty = ''
        
    # Report on who we are
    logger.info('Starting smart_cmnd.py with PID %i', os.getpid())
    logger.info('Version: %s', __version__)
    logger.info('Revision: %s.%s%s', branch, shortsha, dirty)
    logger.info('Site: %s', SITE)
    logger.info('Current MJD: %i', mjd)
    logger.info('Current MPM: %i', mpm)
    logger.info('All dates and times are in UTC except where noted')
    
    # Read in the configuration file
    config = load_json_config(args.config)
    
    # Set the site-dependant `message_out_host` IP address
    if SITE == 'lwa1':
        message_out_host = "10.1.1.2"
    elif SITE == 'lwasv':
        message_out_host = "10.1.2.2"
    elif SITE == 'lwana':
        message_out_host = "10.1.3.2"
    nametag = SITE.replace('lwa', '').lower()
    
    # Setup SmartCopy control
    lwaSC = SmartCopy(config)
    
    # Setup the communications channels
    ## Reference server
    refServer = ReferenceServer(config['mcs']['message_out_host'], config['mcs']['message_ref_port'])
    refServer.start()
    ## MCS server
    mcsComms = MCSCommunicate(lwaSC, config, args)
    mcsComms.start()
    
    # Initialize the copy manager
    lwaSC.ini()
    
    # Setup handler for SIGTERM so that we aren't left in a funny state
    def HandleSignalExit(signum, frame, logger=logger, MCSInstance=mcsComms):
        logger.info('Exiting on signal %i', signum)
        
        # Shutdown SmartCopy and close the communications channels
        tStop = time.time()
        logger.info('Shutting down SmartCopy, please wait...')
        MCSInstance.SubSystemInstance.sht()
        
        while MCSInstance.SubSystemInstance.currentState['info'] != 'System has been shut down':
            time.sleep(1)
        logger.info('Shutdown completed in %.3f seconds', time.time() - tStop)
        
        MCSInstance.stop()
        
        # Exit
        logger.info('Finished')
        logging.shutdown()
        sys.exit(0)
        
    # Hook in the signal handler - SIGTERM
    signal.signal(signal.SIGTERM, HandleSignalExit)
    
    # Loop and process the MCS data packets as they come in - exit if ctrl-c is 
    # received
    logger.info('Ready to communicate')
    while True:
        try:
            mcsComms.receiveCommand()
            
        except KeyboardInterrupt:
            logger.info('Exiting on ctrl-c')
            break
            
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.error("smart_cmnd.py failed with: %s at line %i", str(e), exc_traceback.tb_lineno)
                
            ## Grab the full traceback and save it to a string via StringIO
            fileObject = StringIO()
            traceback.print_tb(exc_traceback, file=fileObject)
            tbString = fileObject.getvalue()
            fileObject.close()
            ## Print the traceback to the logger as a series of DEBUG messages
            for line in tbString.split('\n'):
                logger.debug("%s", line)
                
    # If we've made it this far, we have finished so shutdown SmartCopy and close the 
    # communications channels
    tStop = time.time()
    print('\nShutting down SmartCopy, please wait...')
    logger.info('Shutting down SmartCopy, please wait...')
    lwaSC.sht()
    while lwaSC.currentState['info'] != 'System has been shut down':
        time.sleep(1)
    logger.info('Shutdown completed in %.3f seconds', time.time() - tStop)
    refServer.stop()
    mcsComms.stop()
    
    # Exit
    logger.info('Finished')
    logging.shutdown()
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='control the data copies around the station',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('-c', '--config', type=str, default=DEFAULTS_FILENAME,
                        help='name of the SHL configuration file to use')
    parser.add_argument('-l', '--log', type=str, 
                        help='name of the logfile to write logging information to')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='print debug messages as well as info and higher')
    args = parser.parse_args()
    main(args)
