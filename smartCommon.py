# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import uuid
try:
    import Queue
except ImportError:
    import queue as Queue
import select
import pickle
import warnings
import threading
import traceback
import subprocess
import logging
try:
    import cStringIO as StringIO
except ImportError:
    from io import StringIO
from socket import gethostname

from collections import OrderedDict

__version__ = '0.2'
__all__ = ['SerialNumber', 'LimitedSizeDict', 'DiskBackedQueue', 
           'InterruptibleCopy', 'DELETE_MARKER_QUEUE', 'DELETE_MARKER_NOW']


smartCommonLogger = logging.getLogger('__main__')


DELETE_MARKER_QUEUE = 'smartcopy_queue_delete_this_file'
DELETE_MARKER_NOW   = 'smartcopy_now_delete_this_file'


IS_UNRELIABLE_LINK = False
if gethostname().split('-', 1)[0] in ('lwasv',):
    IS_UNRELIABLE_LINK = True


class SerialNumber(object):
    """
    Simple class for a serial number generator.
    """
    
    def __init__(self):
        self.sn = 1
        self.lock = threading.Semaphore()
        
    def get(self):
        self.lock.acquire()
        out = self.sn*1
        self.sn += 1
        self.lock.release()
        
        return out
        
    def reset(self):
        self.lock.acquire()
        self.sn = 1
        self.lock.release()
        
        return True


class LimitedSizeDict(OrderedDict):
    """
    From:
     https://stackoverflow.com/questions/2437617/limiting-the-size-of-a-python-dictionary
    """
    
    def __init__(self, *args, **kwds):
        self.size_limit = kwds.pop("size_limit", None)
        OrderedDict.__init__(self, *args, **kwds)
        self._check_size_limit()
        
    def __setitem__(self, key, value):
        OrderedDict.__setitem__(self, key, value)
        self._check_size_limit()
        
    def _check_size_limit(self):
        if self.size_limit is not None:
            while len(self) > self.size_limit:
                self.popitem(last=False)


class DiskBackedQueue(Queue.Queue):
    """
    Sub-class of Queue.Queue that keeps a copy of the FIFO queue on-disk to 
    insure continuity between power outages.
    """
    
    _sep = '<<%>>'
    
    def __init__(self, filename, maxsize=0, restore=True):
        self._filename = filename
        self._lock = threading.RLock()
        Queue.Queue.__init__(self, maxsize=maxsize)
        
        # See if we need to restore from disk
        self.restored = []
        with self._lock:
            if restore:
                if os.path.exists(self._filename):
                    if os.path.getsize(self._filename) > 0:
                        with open(self._filename, 'r') as fh:
                            contents = fh.read()
                        contents = contents.split(self._sep)
                        for i,entry in enumerate(contents):
                            try:
                                entry = bytes(entry, 'ascii')
                            except TypeError:
                                pass
                            try:
                                item = pickle.loads(entry)
                                ## Try to avoid ID collisions across restarts
                                ## NOTE:  This is particullarly clean since
                                ##        it doesn't update the ID in the file
                                host, hostpath, dest, destpath, id, retries, lasttry = item
                                if id < 1024:
                                    id += 1024
                                    item = (host, hostpath, dest, destpath, id, retries, lasttry)
                                Queue.Queue.put(self, item)
                                self.restored.append(item)
                            except Exception as e:
                                warnings.warn("Failed to load entry %i of '%s': %s" \
                                              % (i, os.path.basename(self._filename), str(e)),
                                              RuntimeWarning)
            else:
                try:
                    os.unlink(self._filename)
                except OSError:
                    pass
                    
    def _queue_file_io(self, put=None, task_done=False):
        with self._lock:
            try:
                with open(self._filename, 'r') as fh:
                    contents = fh.read()
                contents = contents.split(self._sep)
            except (OSError, IOError):
                contents = []
                
            if put is not None:
                put = pickle.dumps(put, protocol=0)
                try:
                    put = put.decode('ascii')
                except AttributeError:
                    pass
                contents.insert(0, put)
                with open(self._filename, 'w') as fh:
                    fh.write(self._sep.join(contents))
            elif task_done:
                contents = contents[:-1]
                with open(self._filename, 'w') as fh:
                    fh.write(self._sep.join(contents))
                    
    def put(self, item, block=True, timeout=None):
        with self._lock:
            Queue.Queue.put(self, item, block=block, timeout=timeout)
            self._queue_file_io(put=item)
            
    def put_nowait(self, item):
        with self._lock:
            Queue.Queue.put_nowait(self, item)
            self._queue_file_io(put=item)
            
    def task_done(self):
        with self._lock:
            Queue.Queue.task_done(self)
            self._queue_file_io(task_done=True)


class InterruptibleCopy(object):
    _rsyncRE = re.compile('.*?(?P<transferred>\d) +(?P<progress>\d{1,3}%) +(?P<speed>\d+\.\d+[ kMG]B/s) +(?P<remaining>.*)')
    
    def __init__(self, host, hostpath, dest, destpath, id=None, tries=0, last_try=0.0):
        # Copy setup
        self.host = host
        self.hostpath = hostpath
        self.dest = dest
        self.destpath = destpath
        
        # Identifier
        if id is None:
            id = str( uuid.uuid4() )
        self.id = id
        
        # Retry control
        self.tries = tries
        self.last_try = last_try
        
        # Thread setup
        self.thread = None
        self.process= None
        self.stdout, self.stderr = '', ''
        self.status = ''
        
        # Start the copy or delete running
        if time.time() - self.last_try > 86400.0:
            self.resume()
        else:
            self.status = 'error: too soon to retry'
            
    def __str__(self):
        return "%s = %s:%s -> %s:%s" % (self.id, self.host, self.hostpath, self.dest, self.destpath)
        
    def getTryCount(self):
        """
        Return the number of times this operation has been tried.
        """
        
        return self.tries
        
    def getLastTryTimestamp(self):
        """
        Return the timestamp of the last try.  Returns 0 if the operation has 
        not be previously attempted.
        """
        
        return self.last_try
        
    def getTaskSpecification(self):
        """
        Return the task specification tuple that can be used to re-add the 
        copy to the processing queue.
        """
        
        return (self.host, self.hostpath, self.dest, self.destpath, self.id, self.tries, self.last_try)
        
    def getStatus(self):
        """
        Return the status of the copy.
        """
        
        return self.status
        
    def getFileExists(self):
        """
        Return if the source file exists or not.
        """
        
        if self.host == '':
            cmd = ['du', '-b', self.hostpath]
        else:
            cmd = ["ssh", "-t", "-t", "mcsdr@%s" % self.host.lower()]
            cmd.append('du -b %s' % self.hostpath)
            
        try:
            with open('/dev/null', 'w+b') as devnull:
                output = subprocess.check_output(cmd, stderr=devnull)
            try:
                output = output.decode('ascii')
            except AttributeError:
                pass
            junk = output.split(None, 1)[0]
            exists = True
        except (subprocess.CalledProcessError, IndexError):
            exists = False
            
        return exists
        
    def getFileSize(self):
        """
        Return the filesize to be copied in bytes.
        """
        
        try:
            return self._size
        except AttributeError:
            if self.host == '':
                cmd = ['du', '-b', self.hostpath]
            else:
                cmd = ["ssh", "-t", "-t", "mcsdr@%s" % self.host.lower()]
                cmd.append('du -b %s' % self.hostpath)
                
            try:
                with open('/dev/null', 'w+b') as devnull:
                    output = subprocess.check_output(cmd, stderr=devnull)
                try:
                    output = output.decode('ascii')
                except AttributeError:
                    pass
                self._size = output.split(None, 1)[0]
            except subprocess.CalledProcessError:
                self._size = '0'
                
            return self._size
            
    def getBytesTransferred(self):
        """
        Return the number of bytes transferred.
        """
        
        mtch = self._rsyncRE.search(self.stdout)
        if mtch is not None:
            trans = mtch.group('transferred')
        else:
            trans = "0"
            
        return trans
        
    def getProgress(self):
        """
        Return the percentage progress of the copy.
        """
        
        mtch = self._rsyncRE.search(self.stdout)
        if mtch is not None:
            prog = mtch.group('progress')
        else:
            prog = '0%'
            
        return prog
        
    def getSpeed(self):
        """
        Return the current copy speed or 'paused' if the copy is paused.
        """
        
        if self.isRunning:
            mtch = self._rsyncRE.search(self.stdout)
            if mtch is not None:
                speed = mtch.group('speed')
            else:
                speed = '0.00kB/s'
        else:
            speed = 'paused'
            
        return speed
        
    def getTimeRemaining(self):
        """
        Return the estimated time remaining or 'unknown' if the copy is paused.
        """
        
        if self.isRunning:
            mtch = self._rsyncRE.search(self.stdout)
            if mtch is not None:
                rema = mtch.group('remaining')
            else:
                rema = '99:59:59'
        else:
            rema = 'unknown'
            
        return rema
        
    def isRemote(self):
        """
        Return a Boolean of whether or not the copy is to a remote host.
        """
        
        if self.host == self.dest:
            return False
        else:
            return True
            
    def isRunning(self):
        """
        Return a Boolean of whether or not the copy is currently running.
        """
        
        if self.thread is None:
            return False
        else:
            if self.thread.isAlive():
                return True
            else:
                return False
                
    def isComplete(self):
        """
        Return a Boolean of whether or not the copy has finished.
        """
        
        if self.isRunning():
            return False
        elif self.status[:6] != 'paused':
            return True
        else:
            return False
            
    def isSuccessful(self):
        """
        Return a Boolean of whether or not the copy was successful.
        """
        
        if not self.isComplete():
            return False
        elif self.status[:8] == 'complete':
            return True
        else:
            return False
            
    def isFailed(self):
        """
        Return a Boolean of whether or not the copy failed.
        """
        
        if not self.isComplete():
            return False
        elif self.status[:5] == 'error':
            return True
        else:
            return False
            
    def pause(self):
        """
        Pause the copy.
        """
        
        if self.thread is not None:
            try:
                self.process.kill()
            except OSError:
                pass
            self.thread.join()
            
            self.thread = None
            self.process = None
            self.status = 'paused'
            
            return True
        else:
            return False
            
    def resume(self):
        """
        Resume the copy or delete.
        """
        
        if self.thread is None:
            if self.destpath == DELETE_MARKER_QUEUE:
                smartCommonLogger.debug('Readying delete of %s:%s', self.host, self.hostpath)
                self.status = 'complete'
                return True
                
            if self.destpath == DELETE_MARKER_NOW:
                target = self._runDelete
            else:
                target = self._runCopy
            self.thread = threading.Thread(target=target)
            self.thread.setDaemon(1)
            self.thread.start()
            if self.status != 'paused':
                self.tries += 1
                if self.destpath in (DELETE_MARKER_NOW, DELETE_MARKER_QUEUE):
                    self.tries += 100
                self.last_try = time.time()
            self.status = 'active'
            
            time.sleep(1)
            
            return True
        else:
            return False
            
    def cancel(self):
        """
        Cancel the copy.
        """
        
        if self.isRunning:
            self.pause()
        self.status = 'canceled'
        
        return True
        
    def _getTruncateCommand(self):
        """
        Build up a subprocess-compatible command needed to truncate a file on 
        unreliable links.  This returns None if no truncate command is needed.
        """
        
        if not IS_UNRELIABLE_LINK:
            # Ignore reliable links
            cmd = None
            
        if self.host == '':
            # Locally originating copy
            cmd = ['bash', '-c', "if test -e %s; then truncate -c -s -512K %%s; fi" % self.hostpath]
            
            if self.dest == '':
                # Local destination
                ## Directory/path check
                if os.path.exists(self.destpath) and os.path.isdir(self.destpath):
                    filename = os.path.basename(self.hostpath)
                    cmd[-1] = cmd[-1] % filename
                else:
                    cmd = None
            else:
                # Remote destination
                cmd = None
                
        else:
            # Remotely originating copy
            cmd = ["ssh", "-t", "-t", "mcsdr@%s" % self.host.lower()]
            
            if self.dest == self.host:
                # Source and destination are on the same machine
                cmd.append( 'if test -e %s && test -d %s; then truncate -c -s -512K %s/`basename %s`; else truncate -c -s -512K %s; fi' % (self.hostpath, self.destpath, self.destpath, self.hostpath, self.destpath) )
                check_test = True
            else:
                # Source and destination are on different machines
                cmd = None
                
        return cmd
        
    def _getCopyCommand(self):
        """
        Build up a subprocess-compatible command needed to copy the data.
        """
        
        if self.host == '':
            # Locally originating copy
            cmd = ["rsync",  "-avH",  "--append", "--partial", "--progress", self.hostpath]
            
            if self.dest == '':
                # Local destination
                cmd.append( "%s" % self.destpath )
            else:
                # Remote destination
                ## --append-verify should be ok here
                cmd[cmd.index('--append')] = '--append-verify'
                cmd.append( "%s:%s" % (self.dest, self.destpath) )
                
        else:
            # Remotely originating copy
            cmd = ["ssh", "-t", "-t", "mcsdr@%s" % self.host.lower()]
            
            if self.dest == self.host:
                # Source and destination are on the same machine
                cmd.append( 'shopt -s huponexit && rsync -avH --append --partial --progress %s %s' % (self.hostpath, self.destpath) )
            else:
                # Source and destination are on different machines
                ## --append-verify should be ok here
                cmd.append( 'shopt -s huponexit && rsync -avH --append-verify --partial --progress %s %s:%s' % (self.hostpath, self.dest, self.destpath) )
                
        return cmd
        
    def _runCopy(self):
        """
        Start the copy.
        """
        
        # Get the command to use
        cmd = self._getCopyCommand()
        
        # Deal with unreliable links when copying data
        trunc = self._getTruncateCommand()
        if trunc is not None:
            try:
                trunc_p = subprocess.Popen(trunc)
                trunc_s = trunc_p.wait()
                smartCommonLogger.warning('Truncating destination file with \'%s\', status %i', ' '.join(trunc), trunc_s)
            except Exception as trunc_e:
                smartCommonLogger.error('Error truncating destination file with \'%s\': %s', ' '.join(trunc), str(trunc_e))
                
        # Start up the process and start looking at the stdout
        self.process = subprocess.Popen(cmd, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        watchOut = select.poll()
        watchOut.register(self.process.stdout)
        smartCommonLogger.debug('Launched \'%s\' with PID %i', ' '.join(cmd), self.process.pid)
        
        # Read from stdout while the copy is running so we can get 
        # an idea of what is going on with the progress
        while True:
            ## Is there something to read?
            stdout = ''
            while watchOut.poll(1):
                if self.process.poll() is None:
                    try:
                        new_text = self.process.stdout.read(1)
                        try:
                            new_text = new_text.decode('ascii')
                        except AttributeError:
                            pass
                        stdout += new_text
                    except ValueError:
                        break
                else:
                    break
                    
            ## Did we read something?  If not, sleep
            if stdout == '':
                time.sleep(1)
            else:
                self.stdout = stdout.rstrip()
                
            ## Are we done?
            self.process.poll()
            if self.process.returncode is not None:
                ### Yes, break out of this endless loop
                break
                
        # Pull out anything that might be stuck in the buffers
        self.stdout, self.stderr = self.process.communicate()
        
        smartCommonLogger.debug('PID %i exited with code %i', self.process.pid, self.process.returncode)
        
        if self.process.returncode == 0:
            self.status = 'complete'
        elif self.process.returncode < 0:
            self.status = 'paused'
        else:
            smartCommonLogger.debug('copy failed -> %s', self.stderr.rstrip())
            self.status = 'error: %s' % self.stderr
            
        return self.process.returncode
        
    def _getDeleteCommand(self):
        """
        Build up a subprocess-compatible command needed to delete the data.
        """
        
        if self.host == '':
            # Locally originating delete
            cmd = ["rm" "-f", self.hostpath]
            
        else:
            # Remotely originating delete
            cmd = ["ssh", "-t", "-t", "mcsdr@%s" % self.host.lower()]
            cmd.append( 'shopt -s huponexit && sudo rm -f %s' % self.hostpath )
            
        return cmd
        
    def _runDelete(self):
        """
        Start the delete.
        """
        
        # Get the command to use
        cmd = self._getDeleteCommand()
        
        # Start up the process and start looking at the stdout
        self.process = subprocess.Popen(cmd, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        watchOut = select.poll()
        watchOut.register(self.process.stdout)
        smartCommonLogger.debug('Launched \'%s\' with PID %i', ' '.join(cmd), self.process.pid)
        
        # Read from stdout while the copy is running so we can get 
        # an idea of what is going on with the progress
        while True:
            ## Is there something to read?
            stdout = ''
            while watchOut.poll(1):
                if self.process.poll() is None:
                    try:
                        new_text = self.process.stdout.read(1)
                        try:
                            new_text = new_text.decode('ascii')
                        except AttributeError:
                            pass
                        stdout += new_text
                    except ValueError:
                        break
                else:
                    break
                    
            ## Did we read something?  If not, sleep
            if stdout == '':
                time.sleep(1)
            else:
                self.stdout = stdout.rstrip()
                
            ## Are we done?
            self.process.poll()
            if self.process.returncode is not None:
                ### Yes, break out of this endless loop
                break
                
        # Pull out anything that might be stuck in the buffers
        self.stdout, self.stderr = self.process.communicate()
        
        smartCommonLogger.debug('PID %i exited with code %i', self.process.pid, self.process.returncode)
        
        if self.process.returncode == 0:
            self.status = 'complete'
        elif self.process.returncode < 0:
            self.status = 'paused'
        else:
            smartCommonLogger.debug('delete failed -> %s', self.stderr.rstrip())
            self.status = 'error: %s' % self.stderr
            
        return self.process.returncode
