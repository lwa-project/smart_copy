# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import uuid
import select
import threading
import traceback
import subprocess
import logging
try:
	import cStringIO as StringIO
except ImportError:
	import StringIO

from collections import OrderedDict

__version__ = '0.1'
__revision__ = '$Rev$'
__all__ = ['SerialNumber', 'LimitedSizeDict', 'InterruptibleCopy', 
           'DELETE_MARKER_QUEUE', 'DELETE_MARKER_NOW', 
           '__version__', '__revision__', '__all__']


smartCommonLogger = logging.getLogger('__main__')


DELETE_MARKER_QUEUE = 'smartcopy_queue_delete_this_file'
DELETE_MARKER_NOW   = 'smartcopy_now_delete_this_file'


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


class InterruptibleCopy(object):
	_rsyncRE = re.compile('.*?(?P<transferred>\d) +(?P<progress>\d{1,3}%) +(?P<speed>\d+\.\d+[ kMG]B/s) +(?P<remaining>.*)')
	
	def __init__(self, host, hostpath, dest, destpath, id=None):
		# Copy setup
		self.host = host
		self.hostpath = hostpath
		self.dest = dest
		self.destpath = destpath
		
		# Identifier
		if id is None:
			id = str( uuid.uuid4() )
		self.id = id
		
		# Thread setup
		self.thread = None
		self.process= None
		self.stdout, self.stderr = '', ''
		self.status = ''
		
		# Start the copy or delete running
		self.resume()
		
	def __str__(self):
		return "%s = %s:%s -> %s:%s" % (self.id, self.host, self.hostpath, self.dest, self.destpath)
		
	def getStatus(self):
		"""
		Return the status of the copy.
		"""
		
		return self.status
		
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
				output = subprocess.check_output(cmd)
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
			self.process.kill()
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
				cmd.append( "%s:%s" % (self.dest, self.destpath) )
				
		else:
			# Remotely originating copy
			cmd = ["ssh", "-t", "-t", "mcsdr@%s" % self.host.lower()]
			
			if self.dest == self.host:
				# Source and destination are on the same machine
				cmd.append( 'shopt -s huponexit && rsync -avH --append --partial --progress %s %s' % (self.hostpath, self.destpath) )
			else:
				# Source and destination are on different machines
				cmd.append( 'shopt -s huponexit && rsync -avH --append --partial --progress %s %s:%s' % (self.hostpath, self.dest, self.destpath) )
				
		return cmd
		
	def _runCopy(self):
		"""
		Start the copy.
		"""
		
		# Get the command to use
		cmd = self._getCopyCommand()
		
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
						stdout += self.process.stdout.read(1)
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
			cmd.append( 'shopt -s huponexit && rm -f %s' % self.hostpath )
			
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
						stdout += self.process.stdout.read(1)
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
