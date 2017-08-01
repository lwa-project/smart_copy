# -*- coding: utf-8 -*-
import os
import re
import sys
import copy
import time
import uuid
import Queue
import select
import socket
import threading
import traceback
import subprocess
import logging
try:
	import cStringIO as StringIO
except ImportError:
	import StringIO

from smartCommon import *

__version__ = "0.1"
__revision__ = "$Rev$"
__all__ = ['MonitorStation', 'ManageDR', '__version__', '__revision__', '__all__']


smartThreadsLogger = logging.getLogger('__main__')


# Serial number generator for the queue entries
sng = SerialNumber()

# Remote copy lock to help ensure that there is only one remove transfer at at time
rcl = threading.Semaphore()


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
		for i in xrange(1, 5+1):
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
				exc_type, exc_value, exc_traceback = sys.exc_info()
				smartThreadsLogger.error("MonitorStation: pollStation failed with: %s at line %i", str(e), traceback.tb_lineno(exc_traceback))
				
				## Grab the full traceback and save it to a string via StringIO
				fileObject = StringIO.StringIO()
				traceback.print_tb(exc_traceback, file=fileObject)
				tbString = fileObject.getvalue()
				fileObject.close()
				## Print the traceback to the logger as a series of DEBUG messages
				for line in tbString.split('\n'):
					smartThreadsLogger.debug("%s", line)
					
			## Sleep for a bit to wait on new log entries
			time.sleep(1)
			
		# Clean up and get ready to exit
		watch.unregister(tail.stdout)
		tail.kill()
		
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
	def __init__(self, dr, SCCallbackInstance=None):
		self.dr = dr
		self.SCCallbackInstance = SCCallbackInstance
		
		self.queue = Queue.Queue()
		
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
										fh = open('completed_%s.log' % self.dr, 'a')
										fh.write('%s %s\n' % (fsize, self.active.hostpath))
										fh.close()
							else:
								fsize = self.active.getFileSize()
								fh = open('completed_%s.log' % self.dr, 'a')
								fh.write('%s %s\n' % (fsize, self.active.hostpath))
								fh.close()
						elif self.active.isFailed():
							## No, save it to the 'error' log
							fsize = self.active.getFileSize()
							fh = open('error_%s.log' % self.dr, 'a')
							fh.write('%s %s\n' % (fsize, self.active.hostpath))
							fh.close()
						### Check to see if this was a remote copy.  If so, 
						### we need to release the lock
						if self.active.isRemote():
							rcl.release()
							
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
							if self.results[task[-1]] != 'canceled':
								### If the copy appears to be remote, make sure that we can get 
								### the network lock.  If we can't, add the task back to the end
								### of the queue and skip to the next iteration
								if task[0] != task[2]:
									if not rcl.acquire(False):
										## Let the queue know that we've done something with it
										self.queue.task_done()
										
										## Add it back in for later
										self.queue.put(task)
										time.sleep(5)
										continue
										
								### If we've made it this far we have a copy that is ready to go.  
								### Start it up.
								self.active = InterruptibleCopy(*task)
								self.results[self.active.id] = 'active/started for %s:%s -> %s:%s' % (task[0], task[1], task[2], task[3])
								
								### Let the queue know that we've done something with it
								self.queue.task_done()
								
					except Queue.Empty:
						self.active = None
						
			except Exception as e:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				smartThreadsLogger.error("ManageDR: processQueue failed with: %s at line %i", str(e), traceback.tb_lineno(exc_traceback))
				
				## Grab the full traceback and save it to a string via StringIO
				fileObject = StringIO.StringIO()
				traceback.print_tb(exc_traceback, file=fileObject)
				tbString = fileObject.getvalue()
				fileObject.close()
				## Print the traceback to the logger as a series of DEBUG messages
				for line in tbString.split('\n'):
					smartThreadsLogger.debug("%s", line)
					
			time.sleep(5)
			
	def addCopyCommand(self, host, hostpath, dest, destpath):
		"""
		Add a copy command to the queue and return the ID.
		"""
		
		id = str( sng.get() )
		
		try:
			self.queue.put( (host, hostpath, dest, destpath, id) )
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
		
		return True, self.queue.qsize()
		
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
			output = subprocess.check_output(['awk', "'{print $1}'", logname])
			totalSize = sum([int(v, 10) for v in output.split('\n')[:-1]])
		except subprocess.CalledProcessError:
			totalSize = 0
			
		# If we have at least 1 TB of files to cleanup, run the cleanup.  Otherwise, wait.
		if totalSize >= 1024**4:
			## Run the cleanup
			### Load the files to purge
			fh = open(logname, 'r')
			lines = fh.read()
			fh.close()
			### Zero out the purge list
			os.unlink(logname)
			
			### Purge the files, keeping track of what we can't do and what has failed
			entries = lines.split('\n')[:-1]
			retry, failed = [], []
			for entry in entries:
				fsize, filename = entry.split(None, 1)
				try:
					assert(not self.inhibit)
					subprocess.check_output(['ssh', 'mcsdr@%s' % self.dr, 'rm -f %s' % filename])
					smartThreadsLogger.info('Removed %s:%s of size %s', self.dr, filename, fsize)
				except AssertionError:
					retry.append( (fsize, filename) )
				except subprocess.CalledProcessError:
					failed.append( (fsize, filename) )
					smartThreadsLogger.warning('Failed to remove %s:%s of size %s', self.dr, filename, fsize)
					
			### If there are files that we were unable to transfer, save them for later
			if len(retry) > 0:
				fh = open(logfile, 'w')
				for fsize,filename in retry:
					fh.write('%s %s\n' % (fsize, filename))
				fh.close()
				
				return False
				
			return True
			
		else:
			## Skip the cleanup
			return False
			