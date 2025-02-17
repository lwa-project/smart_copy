
import os
import re
import sys
import time
import uuid
import queue as Queue
import select
import pickle
import warnings
import threading
import traceback
import subprocess
import logging
import sqlite3
from io import StringIO
from socket import gethostname

from collections import OrderedDict

__version__ = '0.5'
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
    A thread-safe FIFO queue that persists items to disk using SQLite,
    supporting multiple queues in a single database.
    """
    
    SCHEMA = """
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=FULL;
    PRAGMA busy_timeout=5000;
    
    CREATE TABLE IF NOT EXISTS queue_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        queue_name TEXT NOT NULL,
        host TEXT NOT NULL,
        hostpath TEXT NOT NULL,
        dest TEXT NOT NULL,
        destpath TEXT NOT NULL,
        filesize INTEGER DEFAULT 0,
        command_id INTEGER NOT NULL,
        retry_count INTEGER DEFAULT 0,
        last_try REAL DEFAULT 0.0,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_queue_status ON queue_items(queue_name, status);
    CREATE INDEX IF NOT EXISTS idx_created ON queue_items(created_at);
    """

    # Class-level connection management
    _db_lock = threading.RLock()
    _conn = None
    _cursor = None
    _ref_count: int = 0

    @classmethod
    def _get_connection(cls, db_file):
        """Get or create the shared database connection."""
        with cls._db_lock:
            if cls._conn is None:
                try:
                    # Initialize the connection
                    os.makedirs(os.path.dirname(os.path.abspath(db_file)), exist_ok=True)
                    cls._conn = sqlite3.connect(
                        db_file,
                        isolation_level='IMMEDIATE',
                        check_same_thread=False,
                        timeout=30.0
                    )
                    cls._cursor = cls._conn.cursor()
                    cls._cursor.executescript(cls.SCHEMA)
                    cls._conn.commit()
                except sqlite3.Error as e:
                    smartCommonLogger.error(f"Failed to initialize queue database: {e}")
                    raise
            
            cls._ref_count += 1
            return cls._conn, cls._cursor

    @classmethod
    def _release_connection(cls):
        """Release the shared database connection."""
        with cls._db_lock:
            cls._ref_count -= 1
            if cls._ref_count == 0 and cls._conn is not None:
                try:
                    cls._conn.commit()
                    cls._conn.close()
                    cls._conn = None
                    cls._cursor = None
                except sqlite3.Error:
                    pass

    def __init__(self, filename, queue_name, maxsize=0, restore=True):
        """
        Initialize a queue instance.
        
        Args:
            filename: Shared SQLite database file path
            queue_name: Unique name for this queue (e.g., 'DR1', 'DR2')
            maxsize: Maximum queue size (0 = unlimited)
            restore: Whether to restore pending items on startup
        """
        super().__init__(maxsize)
        
        self._db_file = filename
        self.queue_name = queue_name
        self._lock = threading.RLock()
        
        self.restored_items = []
        
        # Get shared database connection
        self._conn, self._cursor = self._get_connection(filename)
        
        # Verify queue integrity
        self._check_integrity()
        
        # Restore pending items if requested
        if restore:
            self._restore_pending_items()

    def _check_integrity(self):
        """Verify queue integrity and recover if needed."""
        with self._lock:
            try:
                # Check for and recover from interrupted transactions
                self._cursor.execute(
                    """SELECT COUNT(*) FROM queue_items 
                       WHERE queue_name = ? AND status = 'processing'""",
                    (self.queue_name,)
                )
                processing_count = self._cursor.fetchone()[0]
                if processing_count > 0:
                    smartCommonLogger.warning(
                        f"Found {processing_count} interrupted transactions for {self.queue_name}, recovering..."
                    )
                    self._cursor.execute(
                        """UPDATE queue_items SET status = 'pending' 
                           WHERE queue_name = ? AND status = 'processing'""",
                        (self.queue_name,)
                    )
                    self._conn.commit()
                    
            except sqlite3.Error as e:
                smartCommonLogger.error(f"Integrity check failed for {self.queue_name}: {e}")
                raise

    def _restore_pending_items(self):
        """Restore pending items from the database to memory."""
        with self._lock:
            try:
                self._cursor.execute(
                    """SELECT host,hostpath,dest,destpath,command_id,retry_count,last_try FROM queue_items 
                       WHERE queue_name = ? AND status = 'pending' 
                       ORDER BY command_id""",
                    (self.queue_name,)
                )
                rows = self._cursor.fetchall()
                
                for row in rows:
                    try:
                        super().put(*row)
                        self.restored_items.append(*row)
                    except (pickle.UnpicklingError, EOFError) as e:
                        smartCommonLogger.warning(f"Failed to restore queue item in {self.queue_name}: {e}")
                
                smartCommonLogger.info(f"Restored {len(self.restored_items)} items for {self.queue_name}")
                
            except sqlite3.Error as e:
                smartCommonLogger.error(f"Failed to restore items for {self.queue_name}: {e}")
                raise

    def put(self, host, hostpath, dest, destpath, command_id, retry_count=0, last_retry=0.0, block=True, timeout=None):
        """Put an item into the queue."""
        
        with self._lock:
            item = (host, hostpath, dest, destpath, command_id, retry_count, last_retry)
            super().put(item, block, timeout)
            
            try:
                # Start transaction
                self._cursor.execute("BEGIN IMMEDIATE")
                
                # Insert with queue name
                
                self._cursor.execute(
                    """INSERT INTO queue_items (queue_name, host, hostpath, dest, destpath, command_id, retry, last_try) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (self.queue_name, *item)
                )
                
                # Commit transaction
                self._conn.commit()
                
            except sqlite3.Error as e:
                self._conn.rollback()
                smartCommonLogger.error(f"Failed to persist queue item for {self.queue_name}: {e}")
                raise

    def get(self, block=True, timeout=None):
        """Get an item from the queue."""
        with self._lock:
            item = super().get(block, timeout)
            host, hostpath, dest, destpath, command_id, retry_count, last_retry = item
            
            try:
                # Start transaction
                self._cursor.execute("BEGIN IMMEDIATE")
                
                self._cursor.execute(
                    """UPDATE queue_items 
                       SET status = 'processing' 
                       WHERE queue_name = ? AND command_id = ? 
                       AND status = 'pending'""",
                    (self.queue_name, command_id)
                )
                
                # Commit transaction
                self._conn.commit()
                
            except sqlite3.Error as e:
                self._conn.rollback()
                smartCommonLogger.error(f"Failed to update queue item status for {self.queue_name}: {e}")
                raise
                
            return item

    def task_done(self):
        """Mark task as done."""
        with self._lock:
            super().task_done()
            
            try:
                # Start transaction
                self._cursor.execute("BEGIN IMMEDIATE")
                
                # Remove completed items for this queue
                self._cursor.execute(
                    """DELETE FROM queue_items 
                       WHERE queue_name = ? AND status = 'processing'""",
                    (self.queue_name,)
                )
                
                # Commit transaction
                self._conn.commit()
                
            except sqlite3.Error as e:
                self._conn.rollback()
                smartCommonLogger.error(f"Failed to mark queue item as done for {self.queue_name}: {e}")
                raise

    def add_completed(self, host, hostpath, filesize=0):
        """A file as successfully completed."""
        with self._lock:
            item = (host, hostpath, host, hostpath, filesize, 0, time.time(), 'completed')
            
            try:
                # Start transaction
                self._cursor.execute("BEGIN IMMEDIATE")
                
                # Insert with queue name
                
                self._cursor.execute(
                    """INSERT INTO queue_items (queue_name, host, hostpath, dest, destpath, command_id, retry, last_try, status) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (self.queue_name, *item)
                )
                
                # Commit transaction
                self._conn.commit()
                
            except sqlite3.Error as e:
                self._conn.rollback()
                smartCommonLogger.error(f"Failed to add completed file to {self.queue_name}: {e}")
                raise
                
    def get_completed(self):
        """Return a list of completed files as a three-element tuples containing
        host, host path, and file size."""
        with self._lock:
            try:
                self._cursor.execute(
                    """SELECT host, hostpath, filesize
                       FROM queue_items
                       WHERE queue_name = ? AND status = 'completed'
                       ORDER BY hostpath""",
                    (self.queue_name,)
                )
                return self._cursor.fetchall()
                
            except sqlite3.Error as e:
                logger.error(f"Failed to get completed files for {self.queue_name}: {e}")
                raise
                    
    def purge_completed(self):
        """Remove all completed files from the queue."""
        with self._lock:
            try:
                # Start transaction
                self._cursor.execute("BEGIN IMMEDIATE")
                
                # Remove completed items for this queue
                self._cursor.execute(
                    """DELETE FROM queue_items 
                       WHERE queue_name = ? AND status = 'completed'""",
                    (self.queue_name,)
                )
                
                # Commit transaction
                self._conn.commit()
                
            except sqlite3.Error as e:
                self._conn.rollback()
                smartCommonLogger.error(f"Failed purge completed files from {self.queue_name}: {e}")
                raise
                
    def add_failed(self, host, hostpath, reason='', filesize=0):
        """A file as failed."""
        with self._lock:
            item = (host, hostpath, host, reason, filesize, 0, time.time(), 'failed')
            
            try:
                # Start transaction
                self._cursor.execute("BEGIN IMMEDIATE")
                
                # Insert with queue name
                
                self._cursor.execute(
                    """INSERT INTO queue_items (queue_name, host, hostpath, dest, destpath, command_id, retry, last_try, status) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (self.queue_name, *item)
                )
                
                # Commit transaction
                self._conn.commit()
                
            except sqlite3.Error as e:
                self._conn.rollback()
                smartCommonLogger.error(f"Failed to add failed file to {self.queue_name}: {e}")
                raise
                
    def get_failed(self):
        """Return a list of failed files as a four-element tuples containing
        host, host path, failure reason, and file size."""
        with self._lock:
            try:
                self._cursor.execute(
                    """SELECT host, hostpath, destpath, filesize
                       FROM queue_items
                       WHERE queue_name = ? AND status = 'failed'
                       ORDER BY hostpath""",
                    (self.queue_name,)
                )
                return self._cursor.fetchall()
                
            except sqlite3.Error as e:
                logger.error(f"Failed to get failed files for {self.queue_name}: {e}")
                raise
                
    def purge_failed(self):
        """Remove all failed files from the queue."""
        with self._lock:
            try:
                # Start transaction
                self._cursor.execute("BEGIN IMMEDIATE")
                
                # Remove completed items for this queue
                self._cursor.execute(
                    """DELETE FROM queue_items 
                       WHERE queue_name = ? AND status = 'failed'""",
                    (self.queue_name,)
                )
                
                # Commit transaction
                self._conn.commit()
                
            except sqlite3.Error as e:
                self._conn.rollback()
                smartCommonLogger.error(f"Failed to purge failed files from {self.queue_name}: {e}")
                raise
                
    def get_queue_stats(self):
        """Get statistics about this queue."""
        with self._lock:
            try:
                stats = {
                    'pending': 0,
                    'processing': 0,
                    'completed': 0,
                    'failed': 0
                }
                
                self._cursor.execute(
                    """SELECT status, COUNT(*) FROM queue_items 
                       WHERE queue_name = ? GROUP BY status""",
                    (self.queue_name,)
                )
                for status, count in self._cursor.fetchall():
                    stats[status] = count
                    
                return stats
                
            except sqlite3.Error as e:
                smartCommonLogger.error(f"Failed to get queue statistics for {self.queue_name}: {e}")
                raise

    def __del__(self):
        """Release the shared connection."""
        self._release_connection()


class InterruptibleCopy(object):
    _rsyncRE = re.compile('.*?(?P<transferred>\d) +(?P<progress>\d{1,3}%) +(?P<speed>\d+\.\d+[ kMG]B/s) +(?P<remaining>.*)')
    
    def __init__(self, host, hostpath, dest, destpath, id=None, tries=0, last_try=0.0, bw_limit=0.0):
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
        
        # Bandwidth limiting in MB/s for remote copies
        self.bw_limit = bw_limit
        
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
            output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
            output = output.decode()
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
                output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
                output = output.decode()
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
                rcmd = "rsync -avH --append-verify --partial --progress"
                if self.bw_limit > 0:
                    rcmd += (" --bwlimit=%.2fm" % self.bw_limit)
                cmd.append( 'shopt -s huponexit && %s %s %s:%s' % (rcmd, self.hostpath, self.dest, self.destpath) )
                
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
                        new_text = new_text.decode()
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
            smartCommonLogger.debug('copy failed -> %s', self.stderr.decode().rstrip())
            self.status = 'error: %s' % self.stderr.decode()
            
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
                        new_text = new_text.decode()
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
            self.status = 'error: %s' % self.stderr.decode()
            
        return self.process.returncode
