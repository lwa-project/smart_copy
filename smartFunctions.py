
import os
import time
import shutil
import struct
import logging
import threading
from socket import gethostname

from smartThreads import *

__version__ = "0.5"
__all__ = ['commandExitCodes', 'subsystemErrorCodes', 'SmartCopy']


smartFunctionsLogger = logging.getLogger('__main__')


commandExitCodes = {0x00: 'Process accepted without error', 
                    0x01: 'Invalid arguments to command', 
                    0x02: 'Other error running command', 
                    0x03: 'Blocking operation in progress', 
                    0x04: 'Subsystem needs to be initialized',}


subsystemErrorCodes = {0x00: 'Subsystem operating normally',}


class SmartCopy(object):
    """
    Class for managing data copies at the station.
    
    A note about error codes for the overall system status (when
    SUMMARY is WARNING or ERROR):
     * See subsystemErrorCodes
    
    A note about exit codes from control commands:
     *  See commandExitCodes
    """
    
    def __init__(self, config):
        self.config = config
        
        # SmartCopy system information
        self.subSystem = 'SCM'
        self.serialNumber = '1'
        self.version = str(__version__)
        self.site = gethostname().split('-', 1)[0]
        
        # SmartCopy system state
        self.currentState = {}
        self.currentState['status'] = 'SHUTDWN'
        self.currentState['info'] = 'Need to INI SC'
        self.currentState['lastLog'] = 'Welcome to DP SC %s, version %s' % (self.serialNumber, self.version)
        
        ## Operational state
        self.currentState['activeProcess'] = []
        self.globalInhibit = None
        
        ## Monitoring and background threads (antenna statistics, flags, etc.)
        self.currentState['pollingThread'] = None
        self.currentState['drThreads'] = None
        self.currentState['errorThread'] = None
        
    def getState(self):
        """
        Return the current system state as a dictionary.
        """
        
        return self.currentState
        
    def ini(self, refID=0,):
        """
        Initialize SCM (in a seperate thread).
        """

        # Check for other operations in progress that could be blocking (INI and SHT)
        if 'INI' in self.currentState['activeProcess'] or 'SHT' in self.currentState['activeProcess']:
            smartFunctionsLogger.warning("INI command rejected due to process list %s", ' '.join(self.currentState['activeProcess']))
            self.currentState['lastLog'] = 'INI: %s - %s is active and blocking' % (commandExitCodes[0x03], self.currentState['activeProcess'])
            return False, 0x03
            
        # Start the process in the background
        tStart = time.time()
        thread = threading.Thread(target=self.__iniProcess, args=(tStart, refID))
        thread.setDaemon(1)
        thread.start()
        
        return True, 0
        
    def __iniProcess(self, tStart, refID):
        """
        Thread base to initialize DP (load firmware, calibrate, etc.)  Update the 
        current system state as needed.
        """
        
        # Update system state
        self.currentState['status'] = 'BOOTING'
        self.currentState['info'] = 'Running INI sequence'
        self.currentState['activeProcess'].append('INI')
        
        # Stop all threads.  If the don't exist yet, create them.
        ## Station status poller
        if self.currentState['pollingThread'] is not None:
            self.currentState['pollingThread'].stop()
        else:
            self.currentState['pollingThread'] = MonitorStation(SCCallbackInstance=self)
        ## DR managers
        if self.currentState['drThreads'] is not None:
            for dr in self.currentState['drThreads']:
                self.pauseCopyQueue(dr, internal=True)
                self.currentState['drThreads'][dr].stop()
        else:
            self.currentState['drThreads'] = {}
            self.globalInhibit = {}
            drs = (1,2,3,4,5) if self.site == 'lwa1' else (1,2,3,4)
            for i in drs:
                dr = 'DR%i' % i
                self.currentState['drThreads'][dr] = ManageDR(dr, self.config, SCCallbackInstance=self)
                self.globalInhibit[dr] = True
                
        # Start all threads back up
        for dr in self.currentState['drThreads']:
            self.currentState['drThreads'][dr].start()
            self.resumeCopyQueue(dr, internal=True)
        self.currentState['pollingThread'].start()
        self.currentState['errorThread'].start()
        
        self.currentState['status'] = 'NORMAL'
        self.currentState['info'] = 'System calibrated and operating normally'
        self.currentState['lastLog'] = 'INI: finished in %.3f s' % (time.time() - tStart,)
        
        # Update the current state
        smartFunctionsLogger.info("Finished the INI process (ref. #%i) in %.3f s", refID, time.time() - tStart)
        self.currentState['activeProcess'].remove('INI')
        
        return True, 0
        
    def sht(self):
        """
        Issue the SHT command to SCM.
        """
        
        # Check for other operations in progress that could be blocking (INI and SHT)
        if 'INI' in self.currentState['activeProcess'] or 'SHT' in self.currentState['activeProcess']:
            self.currentState['lastLog'] = 'SHT: %s - %s is active and blocking' % (commandExitCodes[0x03], self.currentState['activeProcess'])
            return False, 0x03
            
        thread = threading.Thread(target=self.__shtProcess)
        thread.setDaemon(1)
        thread.start()
        return True, 0
        
    def __shtProcess(self):
        """
        Thread base to shutdown SCM.  Update the current system state as needed.
        """
        
        # Start the timer
        tStart = time.time()
        
        # Update system state
        self.currentState['status'] = 'SHUTDWN'
        self.currentState['info'] = 'System is shutting down'
        self.currentState['activeProcess'].append('SHT')
        
        # Stop the various threads
        ## Station status poller
        smartFunctionsLogger.debug('Stopping station polling')
        if self.currentState['pollingThread'] is not None:
            self.currentState['pollingThread'].stop()
        ## DR managers
        smartFunctionsLogger.debug('Stopping data recorder managers')
        if self.currentState['drThreads'] is not None:
            for dr in self.currentState['drThreads']:
                self.pauseCopyQueue(dr, internal=True)
                self.currentState['drThreads'][dr].stop()
                
        self.currentState['status'] = 'SHUTDWN'
        self.currentState['info'] = 'System has been shut down'
        self.currentState['lastLog'] = 'System has been shut down'
        
        # Update the current state
        smartFunctionsLogger.info("Finished the SHT process in %.3f s", time.time() - tStart)
        self.currentState['activeProcess'].remove('SHT')
        
        return True, 0
        
    def addCopyCommand(self, dr, host, hostpath, dest, destpath):
        # Check the operational status of the system
        if self.currentState['status'] != 'NORMAL':
            self.currentState['lastLog'] = 'SCP: %s' % commandExitCodes[0x04]
            return False, 0x04
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'SCP: %s' % commandExitCodes[0x02]
            return False, 0x02
            
        try:
            status, value = self.currentState['drThreads'][dr].addCopyCommand(host, hostpath, dest, destpath)
            if not status:
                self.currentState['lastLog'] = 'SCP: %s - error queuing copy' % commandExitCodes[0x02]
                return False, 0x02
            else:
                return True, value
        except KeyError:
            self.currentState['lastLog'] = 'SCP: %s - unknown data recorder %s' % (commandExitCodes[0x01], dr)
            return False, 0x01
            
    def pauseCopyQueue(self, dr, internal=False):
        if not internal:
            # Check the operational status of the system
            if self.currentState['status'] != 'NORMAL':
                self.currentState['lastLog'] = 'PAU: %s' % commandExitCodes[0x04]
                return False, 0x04
            if self.currentState['drThreads'] is None:
                self.currentState['lastLog'] = 'PAU: %s' % commandExitCodes[0x02]
                return False, 0x02
                
        if dr == 'ALL':
            for dr in self.currentState['drThreads']:
                self.currentState['drThreads'][dr].pause()
                self.globalInhibit[dr] = True
                
            return True, 'Done'
            
        else:
            try:
                status, value = self.currentState['drThreads'][dr].pause()
                if not status:
                    self.currentState['lastLog'] = 'PAU: %s - error pausing queue' % commandExitCodes[0x02]
                    return False, 0x02
                else:
                    self.globalInhibit[dr] = True
                    
                    return True, value
            except KeyError:
                self.currentState['lastLog'] = 'PAU: %s - unknown data recorder %s' % (commandExitCodes[0x01], dr)
                return False, 0x01
                
    def resumeCopyQueue(self, dr, internal=False):
        if not internal:
            # Check the operational status of the system
            if self.currentState['status'] != 'NORMAL':
                self.currentState['lastLog'] = 'RES: %s' % commandExitCodes[0x04]
                return False, 0x04
            if self.currentState['drThreads'] is None:
                self.currentState['lastLog'] = 'RES: %s' % commandExitCodes[0x02]
                return False, 0x02
                
        if dr == 'ALL':
            for dr in self.currentState['drThreads']:
                self.currentState['drThreads'][dr].resume()
                self.globalInhibit[dr] = False
                
            return True, 'Done'
            
        else:
            try:
                status, value = self.currentState['drThreads'][dr].resume()
                if not status:
                    self.currentState['lastLog'] = 'RES: %s - error resuming queue' % commandExitCodes[0x02]
                    return False, 0x02
                else:
                    self.globalInhibit[dr] = False
                    
                    return True, value
            except KeyError:
                self.currentState['lastLog'] = 'RES: %s - unknown data recorder %s' % (commandExitCodes[0x01], dr)
                return False, 0x01
                
    def cancelCopyCommand(self, id):
        # Check the operational status of the system
        if self.currentState['status'] != 'NORMAL':
            self.currentState['lastLog'] = 'SCN: %s' % commandExitCodes[0x04]
            return False, 0x04
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'SCN: %s' % commandExitCodes[0x02]
            return False, 0x02
            
        found = False
        for dr in self.currentState['drThreads']:
            status, value = self.currentState['drThreads'][dr].cancelCopyCommand(id)
            if status:
                found = True
                break
                
        if found:
            return True, value
        else:
            self.currentState['lastLog'] = 'SCN: %s' % commandExitCodes[0x01]
            return False, 0x01
            
    def addDeleteCommand(self, dr, host, hostpath, now=False):
        # Check the operational status of the system
        if self.currentState['status'] != 'NORMAL':
            self.currentState['lastLog'] = 'SRM: %s' % commandExitCodes[0x04]
            return False, 0x04
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'SRM: %s' % commandExitCodes[0x02]
            return False, 0x02
            
        try:
            status, value = self.currentState['drThreads'][dr].addDeleteCommand(host, hostpath, now=now)
            if not status:
                self.currentState['lastLog'] = 'SRM: %s - error queuing delete' % commandExitCodes[0x02]
                return False, 0x02
            else:
                return True, value
        except KeyError:
            self.currentState['lastLog'] = 'SRM: %s - unknown data recorder %s' % (commandExitCodes[0x01], dr)
            return False, 0x01
            
    def getDRRecordState(self, dr):
        """
        Return the record state of the specified data recorder.  Return a two 
        element tuple of (success, value) where success is a boolean 
        related to if the state was found.  See the currentState['lastLog'] 
        entry for the reason for failure if the returned success value 
        is False.
        """
        
        if self.currentState['pollingThread'] is None:
            self.currentState['lastLog'] = 'STATE: station monitor thread is not running'
            return False, ''
        else:
            status, value = self.currentState['pollingThread'].getState(dr)
            if not status:
                self.currentState['lastLog'] = 'STATE: unknown DR %s' % dr
                return False, ''
            else:
                return True, value
                
    def getDRQueueState(self, dr):
        """
        Return the queue state of the specified data recorder.  Return a two 
        element tuple of (success, value) where success is a boolean 
        related to if the state was found.  See the currentState['lastLog'] 
        entry for the reason for failure if the returned success value 
        is False.
        """
        
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'QUEUE: data recorder monitoring threads are not running'
            return False, 0
        else:
            try:
                status, value = self.currentState['drThreads'][dr].getQueueState()
                if not status:
                    self.currentState['lastLog'] = 'QUEUE: error getting active copy'
                    return False, 0
                else:
                    if not self.globalInhibit[dr]:
                        return True, value
                    else:
                        return True, 'global paused'
            except KeyError:
                self.currentState['lastLog'] = 'QUEUE: unknown DR %s' % dr
                return False, 0
                
    def getDRQueueSize(self, dr):
        """
        Return the queue size of the specified data recorder.  Return a two 
        element tuple of (success, value) where success is a boolean 
        related to if the size was found.  See the currentState['lastLog'] 
        entry for the reason for failure if the returned success value 
        is False.
        """
        
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'QUEUE: data recorder monitoring threads are not running'
            return False, 0
        else:
            try:
                status, value = self.currentState['drThreads'][dr].getQueueSize()
                if not status:
                    self.currentState['lastLog'] = 'QUEUE: error getting queue size'
                    return False, 0
                else:
                    return True, value
            except KeyError:
                self.currentState['lastLog'] = 'QUEUE: unknown data recorder %s' % dr
                return False, 0
                
    def getDRQueueStats(self):
        """
        Return the queue status (pending, processing, completed, failed) of the
        specified data recorder.  Return a two-element tuple of (success, value)
        where success is a boolean related to if the size was found.  See the
        currentState['lastLog'] entry for the reason for failure if the returned
        success value is False.
        """
        
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'QUEUE: data recorder monitoring threads are not running'
            return False, 0
        else:
            try:
                status, value = self.currentState['drThreads'][dr].getQueueStats()
                if not status:
                    self.currentState['lastLog'] = 'QUEUE: error getting queue stats'
                    return False, 0
                else:
                    return True, value
            except KeyError:
                self.currentState['lastLog'] = 'QUEUE: unknown data recorder %s' % dr
                return False, 0
                
    def getCopyCommand(self, id):
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'QUEUE: data recorder monitoring threads are not running'
            return False, 0
        else:
            found = False
            for dr in self.currentState['drThreads']:
                status, value = self.currentState['drThreads'][dr].getCopyCommand(id)
                if status:
                    found = True
                    break
                    
            if found:
                return True, value
            else:
                self.currentState['lastLog'] = 'QUEUE: unknown command ID'
                return False, 0
                
    def getActiveCopyID(self, dr):
        """
        Return the queue ID of the active copy on the specified data recorder.
        Return a two element tuple of (success, value) where success is a boolean 
        related to if the command was found.  See the currentState['lastLog'] 
        entry for the reason for failure if the returned success value 
        is False.
        """
        
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'ACTIVE: data recorder monitoring threads are not running'
            return False, 0
        else:
            try:
                status, value = self.currentState['drThreads'][dr].getActiveID()
                if not status:
                    self.currentState['lastLog'] = 'ACTIVE: error getting active copy ID'
                    return False, 0
                else:
                    return True, value
            except KeyError:
                self.currentState['lastLog'] = 'ACTIVE: unknown DR %s' % dr
                return False, 0
                
    def getActiveCopyStatus(self, dr):
        """
        Return the status of the active copy on the specified data recorder.  
        Return a two element tuple of (success, value) where success is a boolean 
        related to if the command was found.  See the currentState['lastLog'] 
        entry for the reason for failure if the returned success value 
        is False.
        """
        
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'ACTIVE: data recorder monitoring threads are not running'
            return False, 0
        else:
            try:
                status, value = self.currentState['drThreads'][dr].getActiveStatus()
                if not status:
                    self.currentState['lastLog'] = 'ACTIVE: error getting active copy status'
                    return False, 0
                else:
                    return True, value
            except KeyError:
                self.currentState['lastLog'] = 'ACTIVE: unknown DR %s' % dr
                return False, 0
                
    def getActiveCopyBytes(self, dr):
        """
        Return the number of bytes transferred by the active copy on the 
        specified data recorder.  Return a two element tuple of (success, value) 
        where success is a boolean related to if the size was found.  See the 
        currentState['lastLog'] entry for the reason for failure if the returned 
        success value is False.
        """
        
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'ACTIVE: data recorder monitoring threads are not running'
            return False, 0
        else:
            try:
                status, value = self.currentState['drThreads'][dr].getActiveBytesTransferred()
                if not status:
                    self.currentState['lastLog'] = 'ACTIVE: error getting active copy'
                    return False, 0
                else:
                    return True, value
            except KeyError:
                self.currentState['lastLog'] = 'ACTIVE: unknown DR %s' % dr
                return False, 0
                
    def getActiveCopyProgress(self, dr):
        """
        Return the percentage progress of the active copy on the specified 
        data recorder.  Return a two element tuple of (success, value) where 
        success is a boolean related to if the progress was found.  See the 
        currentState['lastLog'] entry for the reason for failure if the returned 
        success value is False.
        """
        
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'ACTIVE: data recorder monitoring threads are not running'
            return False, 0
        else:
            try:
                status, value = self.currentState['drThreads'][dr].getActiveProgress()
                if not status:
                    self.currentState['lastLog'] = 'ACTIVE: error getting active copy'
                    return False, 0
                else:
                    return True, value
            except KeyError:
                self.currentState['lastLog'] = 'ACTIVE: unknown DR %s' % dr
                return False, 0
                
    def getActiveCopySpeed(self, dr):
        """
        Return the speed the active copy on the specified data recorder.  Return
        a two element tuple of (success, value) where success is a boolean 
        related to if the speed was found.  See the currentState['lastLog'] entry 
        for the reason for failure if the returned success value is False.
        """
        
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'ACTIVE: data recorder monitoring threads are not running'
            return False, 0
        else:
            try:
                status, value = self.currentState['drThreads'][dr].getActiveSpeed()
                if not status:
                    self.currentState['lastLog'] = 'ACTIVE: error getting active copy'
                    return False, 0
                else:
                    return True, value
            except KeyError:
                self.currentState['lastLog'] = 'ACTIVE: unknown DR %s' % dr
                return False, 0
                
    def getActiveCopyRemaining(self, dr):
        """
        Return the estimated time remaining for the active on the specified data 
        recorder.  Return a two element tuple of (success, value) where success 
        is a boolean related to if the time estimate was found.  See the 
        currentState['lastLog'] entry for the reason for failure if the returned 
        success value is False.
        """
        
        if self.currentState['drThreads'] is None:
            self.currentState['lastLog'] = 'ACTIVE: data recorder monitoring threads are not running'
            return False, 0
        else:
            try:
                status, value = self.currentState['drThreads'][dr].getActiveTimeRemaining()
                if not status:
                    self.currentState['lastLog'] = 'ACTIVE: error getting active copy'
                    return False, 0
                else:
                    return True, value
            except KeyError:
                self.currentState['lastLog'] = 'ACTIVE: unknown DR %s' % dr
                return False, 0
                
    def processDRStateChange(self, dr, busyState):
        if self.currentState['drThreads'] is None:
            return False
            
        else:
            if not busyState and not self.globalInhibit[dr]:
                smartFunctionsLogger.debug("Resuming queue on %s with activity inhibit %s, global inhibit %s", dr, busyState, self.globalInhibit[dr])
                self.currentState['drThreads'][dr].resume()
            else:
                smartFunctionsLogger.debug("Pausing queue on %s with activity inhibit %s, global inhibit %s", dr, busyState, self.globalInhibit[dr])
                self.currentState['drThreads'][dr].pause()
                
            return True
