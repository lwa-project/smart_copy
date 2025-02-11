#!/usr/bin/env python3

import os
import sys
import git
import time
import signal
import socket
import logging
import argparse
import threading
from pathlib import Path
from datetime import datetime
from typing import Tuple, Optional
from functools import wraps
import collections
try:
    from logging.handlers import WatchedFileHandler
except ImportError:
    from logging import FileHandler as WatchedFileHandler

from lwa_auth.tools import load_json_config
from MCS import MCSClient, MCSMessage
from smartFunctions import SmartCopy

__version__ = '0.6'

# Site Name
SITE = socket.gethostname().split('-', 1)[0]

# Default Configuration File
DEFAULTS_FILENAME = '/lwa/software/defaults.json'


def command_handler(command: str):
    """Decorator to register methods as command handlers"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)
        wrapper._command = command
        return wrapper
    return decorator


class SmartCommandProcessor:
    """
    Class to handle MCS command processing for the Smart Copy system.
    """
    def __init__(self, subsystem_instance: SmartCopy, config: dict):
        self.subsystem = subsystem_instance
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Setup MCS client
        host = self.config['mcs']['message_out_host']
        in_port = self.config['mcs']['message_in_port']
        out_port = self.config['mcs']['message_out_port']
        
        self.mcs_client = MCSClient(
            address=(host, out_port),
            subsystem=self.subsystem.subSystem
        )
        
        # Track recent reference IDs - use deque as a fixed-size FIFO
        self.recent_refs = collections.deque(maxlen=1000)
        self.ref_lock = threading.Lock()
        
        # Auto-register command handlers
        self._handlers: Dict[str, Callable] = {}
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if hasattr(attr, '_command'):
                self._handlers[attr._command] = attr
                
        # Initialize command tracking
        self.command_status = {}
        
    def process_message(self, msg: MCSMessage) -> MCSMessage:
        """Process an incoming MCS message and return the response"""
        
        # Check destination
        if msg.destination not in (self.subsystem.subSystem, 'ALL'):
            return None
            
        full_slot_time = int(time.time())
        self.logger.debug('Got command %s from %s: ref #%i', 
                          msg.command, msg.sender, msg.reference)
        
        # Check for duplicate reference ID
        with self.ref_lock:
            if msg.reference in self.recent_refs:
                self.logger.warning('Rejecting duplicate reference ID: %i', msg.reference)
                return msg.create_reply(False, self.subsystem.currentState['status'], 
                                      'Duplicate reference ID')
            self.recent_refs.append(msg.reference)
        
        # Look up handler
        handler = self._handlers.get(msg.command)
        if not handler:
            status = False
            response = f'Unknown command: {msg.command}'
        else:
            try:
                status, response = handler(msg)
            except Exception as e:
                self.logger.error("Command processing error: %s", str(e))
                status = False 
                response = f"Error: {str(e)}"
                
        # Prune old command status entries
        self._prune_command_status()
        
        # Create response
        return msg.create_reply(status, self.subsystem.currentState['status'], response)
        
    @command_handler('PNG')
    def ping(self, msg: MCSMessage) -> Tuple[bool, str]:
        return True, ''
        
    @command_handler('RPT')
    def report(self, msg: MCSMessage) -> Tuple[bool, str]:
        report_type = msg.data.decode() if isinstance(msg.data, bytes) else msg.data
        
        if not report_type:
            return False, "No report type specified"
            
        try:
            if report_type == 'SUMMARY':
                return True, self.subsystem.currentState['status'][:7]
                
            elif report_type == 'INFO':
                info = self.subsystem.currentState['info']
                if len(info) > 256:
                    info = f"{info[:253]}..."
                return True, info
                
            elif report_type == 'LASTLOG':
                log = self.subsystem.currentState['lastLog']
                if len(log) > 256:
                    log = f"{log[:253]}..."
                if not log:
                    log = 'no log entry'
                return True, log
                
            elif report_type.startswith('OBSSTATUS_'):
                dr = report_type.split('_', 1)[1]
                return self.subsystem.getDRRecordState(dr)
                
            elif report_type.startswith('QUEUE_'):
                return self._handle_queue_report(report_type)
                
            elif report_type.startswith('ACTIVE_'):
                return self._handle_active_report(report_type)
                
            else:
                return False, f'Unknown MIB entry: {report_type}'
                
        except Exception as e:
            return False, str(e)
            
    def _handle_queue_report(self, report_type: str) -> Tuple[bool, str]:
        """Handle queue-related reports"""
        _, prop, value = report_type.split('_', 2)
        
        if prop == 'SIZE':
            return self.subsystem.getDRQueueSize(value)
        elif prop == 'STATUS':
            return self.subsystem.getDRQueueState(value)
        elif prop == 'ENTRY':
            return self.subsystem.getCopyCommand(value)
        else:
            return False, f'Unknown queue property: {prop}'

    def _handle_active_report(self, report_type: str) -> Tuple[bool, str]:
        """Handle active copy reports"""
        _, prop, value = report_type.split('_', 2)
        
        if prop == 'ID':
            return self.subsystem.getActiveCopyID(value)
        elif prop == 'STATUS':
            return self.subsystem.getActiveCopyStatus(value)
        elif prop == 'BYTES':
            return self.subsystem.getActiveCopyBytes(value)
        elif prop == 'PROGRESS':
            return self.subsystem.getActiveCopyProgress(value)
        elif prop == 'SPEED':
            return self.subsystem.getActiveCopySpeed(value)
        elif prop == 'REMAINING':
            return self.subsystem.getActiveCopyRemaining(value)
        else:
            return False, f'Unknown active property: {prop}'
            
    @command_handler('INI')
    def ini(self, msg: MCSMessage) -> Tuple[bool, str]:
        status, exit_code = self.subsystem.ini(refID=msg.reference)
        if status:
            response = ''
        else:
            response = f"0x{exit_code:02X}! {self.subsystem.currentState['lastLog']}"
        self._track_command(full_slot_time, 'INI', msg.reference, exit_code)
        return status, exit_code
        
    @command_handler('SHT')
    def sht(self, msg: MCSMessage) -> Tuple[bool, str]:
        status, exit_code = self.subsystem.sht(mode=msg.data)
        if status:
            response = ''
        else:
            response = f"0x{exit_code:02X}! {self.subsystem.currentState['lastLog']}"
            self._track_command(full_slot_time, 'SHT', msg.reference, exit_code)
        return status, exit_code
        
    @command_handler('SCP')
    def copy(self, msg: MCSMessage) -> Tuple[bool, str]:
        status, response = self._handle_copy(msg.data, msg.reference)
        return status, response
        
    @command_handler('PAU')
    @command_handler('RES')
    def pause_resume(self, msg: MCSMessage) -> Tuple[bool, str]:
        status, response = self._handle_queue_control(msg.command, msg.data, msg.reference)
        return status, response
    
    @command_handler('SCN')
    def cancel(self, msg: MCSMessage) -> Tuple[bool, str]:
        status, response = self._handle_cancel(msg.data, msg.reference)
        return status, response
        
    @command_handler('SRM')
    def remove(self, msg: MCSMessage) -> Tuple[bool, str]:
        status, response = self._handle_delete(msg.data, msg.reference)
        return status, response
        
    def _track_command(self, slot_time: int, cmd: str, ref: int, exit_code: int):
        """Track command execution for status reporting"""
        try:
            self.command_status[slot_time].append((cmd, ref, exit_code))
        except KeyError:
            self.command_status[slot_time] = [(cmd, ref, exit_code)]

    def _prune_command_status(self):
        """Remove old command status entries"""
        for slot_time in list(self.command_status.keys())[:-4]:
            del self.command_status[slot_time]

    def start(self):
        """Start the command processor"""
        self.mcs_client.start()
        
    def stop(self):
        """Stop the command processor"""
        self.mcs_client.close()


def main(args):
    """Main function to run the smart copy command processor"""
    
    # Setup logging
    logger = logging.getLogger(__name__)
    log_format = logging.Formatter(
        '%(asctime)s.%(msecs)03d [%(levelname)-8s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    log_format.converter = time.gmtime
    
    if args.log is None:
        log_handler = logging.StreamHandler(sys.stdout)
    else:
        log_handler = WatchedFileHandler(args.log)
    log_handler.setFormatter(log_format)
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    # Get git info
    try:
        repo = git.Repo(Path(__file__).parent)
        branch = repo.active_branch.name
        commit = repo.active_branch.commit.hexsha
        dirty = ' (dirty)' if repo.is_dirty() else ''
    except git.exc.GitError:
        branch = commit = 'unknown'
        dirty = ''

    # Load config
    config = load_json_config(args.config)
    
    # Setup SmartCopy
    smart_copy = SmartCopy(config)
    
    # Setup command processor
    processor = SmartCommandProcessor(smart_copy, config)
    
    def handle_shutdown(signum, frame):
        """Handle shutdown signals"""
        logger.info('Shutting down on signal %i', signum)
        processor.stop()
        smart_copy.sht()
        while smart_copy.currentState['info'] != 'System has been shut down':
            time.sleep(1)
        logging.shutdown()
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Initialize systems
    smart_copy.ini()
    processor.start()
    
    logger.info('Ready to process commands')
    
    try:
        while True:
            msg = processor.mcs_client.receiver.get_message(timeout=1.0)
            if msg:
                response = processor.process_message(msg)
                if response:
                    processor.mcs_client.sender.send_message(response)
                    
    except KeyboardInterrupt:
        logger.info('Shutting down on CTRL-C')
        handle_shutdown(signal.SIGTERM, None)
        
    except Exception as e:
        logger.error("Fatal error: %s", str(e))
        handle_shutdown(signal.SIGTERM, None)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Smart copy command processor',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-c', '--config', type=str, default=DEFAULTS_FILENAME,
                        help='Configuration file path')
    parser.add_argument('-l', '--log', type=str,
                        help='Log file path')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug logging')
    main(parser.parse_args())
