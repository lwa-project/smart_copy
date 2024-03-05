
import os
import re
import sys
import copy
import glob
import time
import uuid
import queue as Queue
import select
import socket
import threading
import traceback
import subprocess
import logging
from io import StringIO
from datetime import datetime

import smtplib
from email.mime.text import MIMEText

from lwa_auth import STORE as LWA_AUTH_STORE

from smartCommon import *

__version__ = "0.6"
__all__ = ['MonitorStation', 'ManageDR', 'MonitorErrorLogs']


smartThreadsLogger = logging.getLogger('__main__')


# Site name to help figure out how many data recorders there are
SITE = socket.gethostname().split('-', 1)[0]

# Serial number generator for the queue entries
sng = SerialNumber()

# Remote copy lock to help ensure that there is only one remove transfer at at time
rcl = threading.Semaphore()

# Error log lock
ell = threading.Semaphore()


def _LogThreadException(cls, exception, logger=None):
    """
    Function to help with logging exceptions within the monitoring threads.
    This will add a ERROR line to the logs and print the full traceback as
    DEBUG.
    """
    
    # Get the logger
    if logger is None:
        logger = logging.getLogger('__main__')
        
    # Extract the traceback and generate the ERROR message
    exc_type, exc_value, exc_traceback = sys.exc_info()
    cls_name = type(cls).__name__
    try:
        cls_name = "%s - %s" % (cls.dr, cls_name)
    except AttributeError:
        pass
    fnc_name = traceback.extract_tb(exc_traceback, 1)[0][2]
    lineno = exc_traceback.tb_lineno
    logger.error("%s: %s failed with: %s at line %i", cls_name, fnc_name, str(exception), lineno)
    
    # Grab the full traceback and save it to a string via StringIO so that we
    # can print it to DEBUG
    fileObject = StringIO()
    traceback.print_tb(exc_traceback, file=fileObject)
    tbString = fileObject.getvalue()
    fileObject.close()
    ## Print the traceback to the logger as a series of DEBUG messages
    for line in tbString.split('\n'):
        logger.debug("%s", line)


class MonitorStation(object):
    def __init__(self, mselog='/home/op1/MCS/sch/mselog.txt', SCCallbackInstance=None):
        self.mselog = mselog
        self.SCCallbackInstance = SCCallbackInstance
        
        # Setup threading
        self.thread = None
        self.alive = threading.Event()
        self.lastError = None
        
        # Setup the data recorder busy list
        self.busy = {}
        nDR = 5 if SITE == 'lwa1' else 4
        for i in range(1, nDR+1):
            self.busy['DR%i' % i] = True
            
        # Setup the MCS command queue state
        self.cmdQueue = LimitedSizeDict(size_limit=64)
            
    def start(self):
        """
        Start the station monitoring thread.
        """
        
        if self.thread is not None:
            self.stop()
            
        for dr in self.busy:
            self.busy[dr] = True
            if self.SCCallbackInstance is not None:
                self.SCCallbackInstance.processDRStateChange(dr, self.busy[dr])
                
        self.thread = threading.Thread(target=self.pollStation, name='pollStation')
        self.thread.setDaemon(1)
        self.alive.set()
        self.thread.start()
        time.sleep(1)
        
        smartThreadsLogger.info('Started station monitoring background thread')
        
    def stop(self):
        """
        Stop the station monitoring thread.
        """
        
        if self.thread is not None:
            self.alive.clear()          #clear alive event for thread
            
            self.thread.join()          #don't wait too long on the thread to finish
            self.thread = None
            self.lastError = None
            
            for dr in self.busy:
                self.busy[dr] = True
                if self.SCCallbackInstance is not None:
                    self.SCCallbackInstance.processDRStateChange(dr, self.busy[dr])
                    
            smartThreadsLogger.info('Stopped station monitoring background thread')
            
    def _parseLogData(self, lines):
        # Make a copy of the current DR states
        newBusyState = copy.deepcopy(self.busy)
            
        # Walk through the lines
        for line in lines:
            ## Parse the line
            try:
                line = line.decode('ascii', errors='ignore')
            except UnicodeDecodeError as e:
                smartThreadsLogger.debug("MonitorStation: failed to decode line '%s': %s", line, str(e))
                continue
            fields = line.split(None, 9)
            try:
                rid = int(fields[5], 10)			# MCS reference ID
                status = int(fields[6], 10)		# MCS command send status
                subsys = fields[7]				# Target subsystem
                cmd = fields[8]				# Command
                data = fields[9].rsplit('|', 1)[0]	# Command arguments/response
            except Exception as e:
                smartThreadsLogger.debug("MonitorStation: error parsing log line '%s': %s", line.rstrip(), str(e))
                continue
                
            ## Ignore non-DR subsystems
            if subsys[:2] != 'DR':
                continue
                
            ## Figure out what to do basedon the command send status
            if status == 2:
                ### Save for later
                self.cmdQueue[rid] = (rid, subsys, cmd, data)
                
            elif status == 3:
                ### The command as been responded to
                if cmd in ('SHT', 'REC', 'SPC'):
                    ### A valid shutdown, record, or spectrometer command
                    newBusyState[subsys] = True
                    
                elif cmd in ('INI', 'STP',):
                    ### A valid INI or stop command
                    newBusyState[subsys] = False
                    
                elif cmd == 'RPT':
                    ### A valid report command has been responded to
                    try:
                        rptType = self.cmdQueue[rid][3]
                        if rptType == 'OP-TYPE':
                            if data[:4] == 'Idle':
                                newBusyState[subsys] = False
                            else:
                                newBusyState[subsys] = True
                        elif rptType == 'SUMMARY':
                            if data[:6] != 'NORMAL':
                                newBusyState[subsys] = True
                    except KeyError:
                        pass
                        
            elif status == 8:
                ## The subsystem is dead
                newBusyState[subsys] = True
                
        return newBusyState
        
    def pollStation(self):
        """
        Poll the mselog.txt file to look for changes in the data recorders states.
        """
        
        # Open the MCS logfile for reading and being watching for changes
        tail = subprocess.Popen(['tail', '-F', self.mselog], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        watch = select.poll()
        watch.register(tail.stdout)
        
        # Go!
        while self.alive.isSet():
            try:
                ## Is there anything to read?
                if watch.poll(1):
                    ### Good, read in all that we can
                    lines = [tail.stdout.readline(),]
                    while watch.poll(1):
                        lines.append( tail.stdout.readline() )
                        
                    ### Parse the lines to figure out the current state of the DRs
                    newState = self._parseLogData(lines)
                    
                    ### Propogate any state changes we find
                    for dr in self.busy:
                        if newState[dr] != self.busy[dr]:
                            smartThreadsLogger.debug('State changed on %s to %s', dr, 'busy' if newState[dr] else 'idle')
                            self.busy[dr] = newState[dr]
                            if self.SCCallbackInstance is not None:
                                self.SCCallbackInstance.processDRStateChange(dr, self.busy[dr])
                                
            except Exception as e:
                _LogThreadException(self, e, logger=smartThreadsLogger)
                
            ## Sleep for a bit to wait on new log entries
            time.sleep(1)
            
        # Clean up and get ready to exit
        watch.unregister(tail.stdout)
        try:
            tail.kill()
        except OSError:
            pass
            
        # Done
        return True
        
    def getState(self, dr):
        """
        Return the current busy state of the specified data recorder.  A 
        two-element tuple of result and value are returned.  If the data 
        recorder is out of range or some other error occurs, 
        (False, errorString) is returned.  Otherwise (True, status) is 
        returned.
        """
        
        try:
            return True, 'busy' if self.busy[dr] else 'idle'
        except KeyError:
            return False, 'Unknown data recorder %s' % str(dr)


class ManageDR(object):
    def __init__(self, dr, config, SCCallbackInstance=None):
        self.dr = dr
        self.config = config
        self.SCCallbackInstance = SCCallbackInstance
        
        self.queue = DiskBackedQueue('.%s.queue' % self.dr, restore=True)
        
        # Setup threading
        self.thread = None
        self.alive = threading.Event()
        self.alive.clear()
        self.lastError = None
        
        # Activity
        self.inhibit = True
        self.active = None
        
        # Results cache
        self.results = LimitedSizeDict(size_limit=512)
        for entry in self.queue.restored:
            host, hostpath, dest, destpath, id, retries, lasttry = entry
            self.results[id] = 'queued for %s:%s -> %s:%s' % (host, hostpath, dest, destpath)
            
    def start(self):
        """
        Start the station monitoring thread.
        """
        
        if self.thread is not None:
            self.stop()
            
        self.thread = threading.Thread(target=self.processQueue, name='processQueue%s' % self.dr)
        self.thread.setDaemon(1)
        self.alive.set()
        self.thread.start()
        time.sleep(1)
        
        smartThreadsLogger.info('Started smart copy processing queue for %s' % self.dr)
        
    def stop(self):
        """
        Stop the station monitoring thread.
        """
        
        if self.thread is not None:
            self.pause()                #stop the current copy
            try:
                rcl.release()
            except threading.ThreadError:
                pass
                
            self.alive.clear()          #clear alive event for thread
            
            self.thread.join()          #don't wait too long on the thread to finish
            self.thread = None
            self.lastError = None
            
            smartThreadsLogger.info('Stopped smart copy processing queue for %s' % self.dr)
            
    def pause(self):
        """
        Pause the current copy and the queue processing.
        """
        
        try:
            self.active.pause()
            self.results[self.active.id] = 'paused for %s:%s -> %s:%s' % (self.active.host, self.active.hostpath, self.active.dest, self.active.destpath)
            
        except AttributeError:
            pass
            
        self.inhibit = True
        
        return True, 0
        
    def resume(self):
        """
        Resume the current copy and the queue processing.
        """
        
        try:
            self.active.resume()
            self.results[self.active.id] = 'active/resumed for %s:%s -> %s:%s' % (self.active.host, self.active.hostpath, self.active.dest, self.active.destpath)
            
        except AttributeError:
            pass
            
        self.inhibit = False
        
        return True, 0
        
    def processQueue(self):
        # Purges run late in the day (18:00 UTC)
        tLastPurge = (int(time.time())/86400)*86400 + 18*3600
        
        while self.alive.isSet():
            tStart = time.time()
            
            try:
                # Determine if we are ready to proceed
                readyToProcess = True
                ## Are we paused for some reason?
                if self.inhibit:
                    readyToProcess &= False
                ## Is there already an item being process?
                if self.active is not None:
                    ## Has the tasked finished?
                    if not self.active.isComplete():
                        ### Nope, no need to start a new one
                        readyToProcess &= False
                    else:
                        ### Yes, save the output
                        self.results[self.active.id] = self.active.status
                        ### Let the queue know that we've done something with it
                        self.queue.task_done()
                        ### Was it a successful copy?
                        if self.active.isSuccessful():
                            ## Yes, save it to the 'completed' log
                            #### Ok, this is a little strange.  We only want to allow spectrometer files 
                            #### that have been copied to a remove destination (leo) to be queued for 
                            #### deletion.  This way we don't have a 'hole' in our archive where data
                            #### gets copied to the cluster, deleted, and never makes it to the archive.
                            if self.active.hostpath.find('DROS/Spec') != -1:
                                if self.active.isRemote():
                                    if self.active.dest.find('leo10g.unm.edu') != -1:
                                        fsize = self.active.getFileSize()
                                        with open('completed_%s.log' % self.dr, 'a') as fh:
                                            fh.write('%.0f %s %s\n' % (time.time(), fsize, self.active.hostpath))
                                            
                            else:
                                fsize = self.active.getFileSize()
                                with open('completed_%s.log' % self.dr, 'a') as fh:
                                    fh.write('%.0f %s %s\n' % (time.time(), fsize, self.active.hostpath))
                                    
                        elif self.active.isFailed():
                            ## No, but let's see we we can save it
                            if self.active.getTryCount() >= self.config['max_retry']:
                                ### No, it's failed too many times.  Save it to the 'error' log
                                fsize = self.active.getFileSize()
                                with ell:
                                    with open('error_%s.log' % self.dr, 'a') as fh:
                                        fh.write('%.0f %s %s -> %s with %s\n' % (time.time(),
                                                                                 fsize,
                                                                                 self.active.hostpath,
                                                                                 self.active.destpath,
                                                                                 self.active.status))
                                        
                            else:
                                ### There's still a chance.  Stick it in again
                                self.queue.put(self.active.getTaskSpecification())
                                
                        ### Check to see if this was a remote copy.  If so, 
                        ### we need to release the lock
                        if self.active.isRemote():
                            rcl.release()
                            
                        ### Reset the active variable now that we've dealt with it
                        self.active = None
                        
                if readyToProcess:
                    # Try to clean things up on the DR
                    if time.time() - tLastPurge > 86400:
                        self.purgeCompleted()
                        tLastPurge = time.time()
                        
                    # It looks like we can try to start another item in the queue running
                    try:
                        ## Pull out the next task
                        task = self.queue.get(False, 5)
                        if task is not None:
                            ### Make sure the task hasn't been canceled
                            if self.results[task[4]] != 'canceled':
                                ### Check if this task has been re-queued because of an error.
                                ### If so, make sure that we haven't tried it again in at 
                                ### least 'wait_retry' hours.
                                if task[5] > 0 and (time.time() - task[6]) < self.config['wait_retry']*3600:
                                    ## Let the queue know that we've done something with it
                                    self.queue.task_done()
                                    
                                    ## Add it back in for later
                                    self.queue.put(task)
                                    time.sleep(5)
                                    continue
                                    
                                ### If the copy appears to be remote, make sure that we can get 
                                ### the network lock.  If we can't, add the task back to the end
                                ### of the queue and skip to the next iteration
                                bw_limit = 0
                                if task[0] != task[2]:
                                    if not rcl.acquire(False):
                                        ## Let the queue know that we've done something with it
                                        self.queue.task_done()
                                        
                                        ## Add it back in for later
                                        self.queue.put(task)
                                        time.sleep(5)
                                        continue
                                    else:
                                        bw_limit = self.config['bw_limit']
                                        
                                ### If we've made it this far we have a copy that is ready to go.  
                                ### Start it up.
                                self.active = InterruptibleCopy(*task, bw_limit=bw_limit)
                                self.results[self.active.id] = 'active/started for %s:%s -> %s:%s' % (task[0], task[1], task[2], task[3])
                                
                            else:
                                ### Yep, it's been canceled
                                self.queue.task_done()
                                
                    except Queue.Empty:
                        self.active = None
                        
            except Exception as e:
                _LogThreadException(self, e, logger=smartThreadsLogger)
                
            time.sleep(5)
            
    def addCopyCommand(self, host, hostpath, dest, destpath):
        """
        Add a copy command to the queue and return the ID.
        """
        
        id = str( sng.get() )
        
        try:
            self.queue.put( (host, hostpath, dest, destpath, id, 0, 0.0) )
            self.results[id] = 'queued for %s:%s -> %s:%s' % (host, hostpath, dest, destpath)
            return True,  id
        except Exception as e:
            return False, str(e)
            
    def cancelCopyCommand(self, id):
        """
        Cancel a queued copy command.
        """
        
        try:
            if self.active is not None:
                if self.active.id == id:
                    self.active.cancel()
            self.results[id] = 'canceled'
            return True, id
        except KeyError:
            return False, 'Unknown copy command ID'
            
    def addDeleteCommand(self, host, hostpath, now=False):
        """
        Add a delete command to the queue and return the ID.
        """
        
        id = str( sng.get() )
        
        try:
            dflag = DELETE_MARKER_NOW if now else DELETE_MARKER_QUEUE
            self.queue.put( (host, hostpath, host, dflag, id, 0, 0.0) )
            self.results[id] = 'queued delete for %s:%s' % (host, hostpath)
            return True,  id
        except Exception as e:
            return False, str(e)
            
    def getCopyCommand(self, id):
        """
        Return the status of the copy command.
        """
        
        try:
            status = self.results[id]
            return True, status
        except KeyError:
            return False, 'Unknown copy command ID'
            
    def getQueueSize(self):
        """
        Return the size of the copy queue.
        """
        
        return True, self.queue.qsize() + (1 if self.active is not None else 0)
        
    def getQueueState(self):
        """
        Return the state of the copy queue.
        """
        
        return True, 'active' if not self.inhibit else 'paused'
        
    def getActiveID(self):
        """
        Return the active copy ID.
        """
        
        if self.active is not None:
            return True, self.active.id
        else:
            return True, 'None'
            
    def getActiveStatus(self):
        """
        Return the status of the active copy.
        """
        
        if self.active is not None:
            try:
                status = self.results[self.active.id]
                return True, status
            except KeyError:
                return False, 'Cannot access active command ID'
        else:
            return True, 'None'
            
    def getActiveBytesTransferred(self):
        """
        Return the number of bytes transferred by the active copy.
        """
        
        if self.active is not None:
            return True, self.active.getBytesTransferred()
        else:
            return True, 'None'
            
    def getActiveProgress(self):
        """
        Return the percentage progress for the active copy.
        """
        
        if self.active is not None:
            return True, self.active.getProgress()
        else:
            return True, 'None'
            
    def getActiveSpeed(self):
        """
        Return the speed of the active copy.
        """
        
        if self.active is not None:
            return True, self.active.getSpeed()
        else:
            return True, 'None'
            
    def getActiveTimeRemaining(self):
        """
        Return the last rsync output line.
        """
        
        if self.active is not None:
            return True, self.active.getTimeRemaining()
        else:
            return True, 'None'
            
    def purgeCompleted(self):
        """
        Purge completed transfers.
        """
        
        # Get the name of the logfile to check
        logname = 'completed_%s.log' % self.dr
        
        # Get how many lines are in the logfile
        try:
            with open('/dev/null', 'w+b') as devnull:
                totalSize = subprocess.check_output(['awk', "{sum+=$2} END {print sum}", logname], stderr=devnull)
            totalSize = totalSize.decode()
            totalSize = int(totalSize, 10)
        except (subprocess.CalledProcessError, ValueError):
            totalSize = 0
            
        # If we have at least 1 TB of files to cleanup, run the cleanup.  Otherwise, wait.
        if totalSize >= self.config['purge_size']*1024**4:
            smartThreadsLogger.info("Attempting to purge %.1f TB from %s", totalSize/1024.0**4, self.dr)
            
            ## Run the cleanup
            ### Load the files to purge
            with open(logname, 'r') as fh:
                lines = fh.read()
            ### Zero out the purge list
            os.unlink(logname)
            
            ### Purge the files, keeping track of what we can't do and what has failed
            entries = lines.split('\n')[:-1]
            retry, failed = [], []
            for entry in entries:
                timestamp, fsize, filename = entry.split(None, 2)
                try:
                    assert(not self.inhibit)
                    with open('/dev/null', 'w+b') as devnull:
                        subprocess.check_output(['ssh', '-t', '-t', 'mcsdr@%s' % self.dr, 'shopt -s huponexit && sudo rm -f %s' % filename], stderr=devnull)
                    smartThreadsLogger.info('Removed %s:%s of size %s', self.dr, filename, fsize)
                except AssertionError:
                    retry.append( (timestamp, fsize, filename) )
                except subprocess.CalledProcessError as e:
                    failed.append( (timestamp, fsize, filename) )
                    smartThreadsLogger.warning('Failed to remove %s:%s of size %s', self.dr, filename, fsize)
                    smartThreadsLogger.debug('%s', str(e))
                    
            ### If there are files that we were unable to transfer, save them for later
            if len(retry) > 0:
                with open(logname, 'w') as fh:
                    for timestamp,fsize,filename in retry:
                        fh.write('%s %s %s\n' % (timestamp, fsize, filename))
                        
                return False
                
            return True
            
        else:
            ## Skip the cleanup
            return False


class MonitorErrorLogs(object):
    def __init__(self, config):
        self.config = config
        
        # Setup threading
        self.thread = None
        self.alive = threading.Event()
        self.alive.clear()
        self.lastError = None
        
        # Activity
        self.active = None
        self.thread = None
        
        # Setup e-mail access
        ## SMTP user and password
        self.FROM = self.config['email']['username']
        self.PASS = self.config['email']['password']
        self.ESRV = self.config['email']['smtp_server']
        
    def start(self):
        """
        Start the station monitoring thread.
        """
        
        if self.thread is not None:
            self.stop()
            
        self.thread = threading.Thread(target=self.monitorLogs, name='monitorLogs')
        self.thread.setDaemon(1)
        self.alive.set()
        self.thread.start()
        time.sleep(1)
        
        smartThreadsLogger.info('Started smart copy error log monitor')
        
    def stop(self):
        """
        Stop the station monitoring thread.
        """
        
        if self.thread is not None:
            self.alive.clear()          #clear alive event for thread
            
            self.thread.join()          #don't wait too long on the thread to finish
            self.thread = None
            self.lastError = None
            
            smartThreadsLogger.info('Stopped smart copy error log monitor')
            
    def monitorLogs(self):
        # Checks run even later in the day (22:00 UTC)
        tLastCheck = (int(time.time())/86400)*86400 + 22*3600
        
        while self.alive.isSet():
            tStart = time.time()
            
            if tStart - tLastCheck >= 86400:
                # Failure complination
                failures = {}
                
                # Error logs
                filenames = glob.glob('error_*.log')
                filenames.sort()
                for filename in filenames:
                    ## DR name
                    dr = os.path.basename(filename)
                    dr = os.path.splitext(dr)[0]
                    dr = dr.rsplit('_', 1)[1]
                    
                    ## Parse to figure out which lines to keep
                    with ell:
                        ### Load
                        with open(filename, 'r') as fh:
                            contents = fh.read()
                            
                        ### Sort
                        to_keep = []
                        for line in contents.split('\n'):
                            if len(line) < 3:
                                continue
                            timestamp, fsize, dataname = line.split(None, 2)
                            timestamp = int(timestamp, 10)
                            fsize = int(fsize, 10)
                            
                            if timestamp >= tStart - 86400 - 3600:
                                to_keep.append(line)
                                
                                if fsize > 0:
                                    try:
                                        failures[dr].append(dataname)
                                    except KeyError:
                                        failures[dr] = [dataname,]
                                        
                        ### Save
                        with open(filename, 'w') as fh:
                            fh.write('\n'.join(to_keep))
                            
                # Report 
                report = ''
                for dr in sorted(list(failures.keys())):
                    report += '%s:\n' % dr
                    for dataname in failures[dr]:
                        report += '  %s\n' % dataname
                        
                # Send the report as an email, if there is anything to send
                if len(report) > 0:
                    ## A copy for the logs
                    for line in report.split('\n'):
                        smartThreadsLogger.debug("%s", line)
                        
                    ## Add on an "email ID" to get around list.unm.edu silliness
                    report = "%s\n\nEmail ID: %s" % (report, str(uuid.uuid4()))
                    
                    ## The message itself
                    ### Who gets it
                    to = ['lwa1ops-l@list.unm.edu',]
                    cc = None
                    
                    ### The report
                    msg = MIMEText(report)
                    msg['Subject'] = 'Recent SmartCopy Failures - %s' % (datetime.utcnow().strftime("%Y/%m/%d"),)
                    msg['From'] = self.FROM
                    msg['To'] = ','.join(to)
                    if cc is not None:
                        cc = list(set(cc))
                        msg['Cc'] = ','.join(cc)
                    msg.add_header('reply-to', 'lwa1ops-l@list.unm.edu')
                    
                    ### The other who gets it
                    rcpt = []
                    rcpt.extend(to)
                    if cc is not None:
                        rcpt.extend(cc)
                        
                    ## Send it off
                    try:
                        server = smtplib.SMTP(self.ESRV, 587)
                        server.starttls()
                        server.login(self.FROM, self.PASS)
                        server.sendmail(self.FROM, rcpt, msg.as_string())
                        server.close()
                    except Exception as e:
                        smartThreadsLogger.error("Could not send error report e-mail: %s", str(e))
                        
                # Update the last check time
                tLastCheck = time.time()
                
            # Sleep
            time.sleep(5)
