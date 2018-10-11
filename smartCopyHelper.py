#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import zmq
import math
import time
import getopt
import socket
import subprocess
from datetime import datetime

from zeroconf import Zeroconf

from lsl.common import mcs, metabundle
try:
	from lsl.common import metabundleADP
	adpReady = True
except ImportError:
	adpReady = False


SITE = socket.gethostname().split('-', 1)[0]


def usage(exitCode=None):
	print """smartCopyHelper.py - Parse a metadata file and queue the data copy for later

Usage: smartCopyHelper.py [OPTIONS] metadata [metadata [...]] ucfName

Options:
-h, --help         Display this help information
-v, --version      Display version information
-m, --metadata     Include the metadata with the copy
-o, --obsevations  Comma separated list of obseration numbers to transfer
                   (one based; default = -1 = tranfer all obserations)
-q, --query        Query the smart copy server

Note:
  For -q/--query calls, valid MIB entries are:
    OBSSTATUS_DR# - whether or not DR# is recording data
    
    QUEUE_SIZE_DR# - size of the copy queue on DR#
    QUEUE_STATUS_DR# - status of the copy queue on DR#
    QUEUE_ENTRY_# - details of a copy command entry
    
    ACTIVE_ID_DR# - active copy command queue ID on DR#
    ACTIVE_STATUS_DR# - active copy command status/command on DR#
    ACTIVE_BYTES_DR# - active copy bytes transferred on DR#
    ACTIVE_PROGRESS_DR# - active copy progress on DR#
    ACTIVE_SPEED_DR#- active copy speed on DR#
    ACTIVE_REMAINING_DR# - active copy time remaining on DR#
"""

	if exitCode is not None:
		sys.exit(exitCode)
	else:
		return True


def parseOptions(args):
	"""
	Parse the command line options and return a dictionary of the configuration
	"""

	config = {}
	# Default parameters
	config['version'] = False
	config['metadata'] = False
	config['obsID'] = -1
	config['query'] = False
	config['args'] = []
	
	# Read in and process the command line flags
	try:
		opts, args = getopt.getopt(args, "hvmo:q", ["help", "version", "metadata", "observations=", "query"])
	except getopt.GetoptError, err:
		# Print help information and exit:
		print str(err) # will print something like "option -a not recognized"
		usage(exitCode=2)
		
	# Work through opts
	for opt, value in opts:
		if opt in ('-h', '--help'):
			usage(exitCode=0)
		elif opt in ('-v', '--version'):
			config['version'] = True
		elif opt in ('-m', '--metadata'):
			config['metadata'] = True
		elif opt in ('-o', '--observations'):
			config['obsID'] = [int(v,10)-1 for v in value.split(',')]
		elif opt in ('-q', '--query'):
			config['query'] = True
		else:
			assert False
	
	# Add in arguments
	config['args'] = args
	
	# Validate
	if config['query'] and len(config['args']) != 1:
		raise RuntimeError("Only one argument is allowed for query operations")
	if not config['query'] and not config['version'] and len(config['args']) < 2:
		raise RuntimeError("Must specify both a metadata file and a UCF username for the copy")
		
	# Return configuration
	return config


def parseMetadata(tarname):
	"""
	Given a filename for a metadata tarball, parse the file and return a
	five-element tuple of:
	  * filetag
	  * DRSU barcode
	  * beam
	  * date
	  * if the data is spectrometer or not
	"""
	
	try:
		parser = metabundle
		parser.getSessionSpec(tarname)
	except Exception as e:
		if adpReady:
			parser = metabundleADP
			parser.getSessionSpec(tarname)
		else:
			raise e
			
	project = parser.getSessionDefinition(tarname)
	isSpec = False
	if project.sessions[0].observations[0].mode not in ('TBW', 'TBN'):
		if project.sessions[0].spcSetup[0] != 0 and project.sessions[0].spcSetup[1] != 0:
			isSpec = True
			
	meta = parser.getSessionMetaData(tarname)
	tags = [meta[id]['tag'] for id in sorted(meta.keys())]
	barcodes = [meta[id]['barcode'] for id in sorted(meta.keys())]
	meta = parser.getSessionSpec(tarname)
	beam = meta['drxBeam']
	date = mcs.mjdmpm2datetime(int(meta['MJD']), int(meta['MPM']))
	datestr = date.strftime("%y%m%d")
	
	return tags, barcodes, beam, date, isSpec


def getDRSUPath(beam, barcode):
	"""
	Given a beam (data recorder) number and a DRSU barcode, convert the 
	barcode to a path on the data recorder.  Returns None if the DRSU cannot
	be found.
	"""
	
	path = None
	
	try:
		p = subprocess.Popen(['ssh', "mcsdr@dr%s" % beam, 'mount', '-l', '-t', 'ext4'], cwd='/home/op1/MCS/tp', stdin=None, stdout=subprocess.PIPE)
		p.wait()
		
		for line in iter(p.stdout.readline, ''):
			line = line.split()
			try:
				if line[6]==("['%s']" % (barcode)):
					path = line[2]
			except:
				pass
				
	except:
		pass
		
	return path


# Maximum number of bytes to receive from MCS
MCS_RCV_BYTES = 16*1024


def getTime():
	"""
	Return a two-element tuple of the current MJD and MPM.
	"""
	
	# determine current time
	dt = datetime.utcnow()
	year        = dt.year             
	month       = dt.month      
	day         = dt.day    
	hour        = dt.hour
	minute      = dt.minute
	second      = dt.second     
	millisecond = dt.microsecond / 1000
	
	# compute MJD         
	# adapted from http://paste.lisp.org/display/73536
	# can check result using http://www.csgnetwork.com/julianmodifdateconv.html
	a = (14 - month) // 12
	y = year + 4800 - a          
	m = month + (12 * a) - 3                    
	p = day + (((153 * m) + 2) // 5) + (365 * y)   
	q = (y // 4) - (y // 100) + (y // 400) - 32045
	mjd = int(math.floor( (p+q) - 2400000.5))  
	
	# compute MPM
	mpm = int(math.floor( (hour*3600 + minute*60 + second)*1000 + millisecond ))
	
	return (mjd, mpm)


def buildPayload(source, cmd, data=None, refSocket=None):
	if refSocket is None:
		ref = 1
	else:
		refSocket.send(b"next_ref")
		ref = int(refSocket.recv(), 10)
		
	mjd, mpm = getTime()
	
	payload = ''
	payload += 'SCM'
	payload += source
	payload += cmd
	payload += '%9i' % ref
	if data is None:
		payload += '%4i' % 0
	else:
		payload += '%4i' % len(data)
	payload += '%6i' % mjd
	payload += '%9i' % mpm
	payload += ' '
	if data is not None:
		payload += data
		
	return payload


def parsePayload(payload):
	dataLen = int(payload[18:22], 10)
	cmdStatus = payload[38]
	subStatus = payload[39:46]
	data      = payload[46:46+dataLen-7]
	
	return cmdStatus, subStatus, data


def main(args):
	# Parse the command line
	config = parseOptions(args)
	
	# Connect to the smart copy command server
	zeroconf = Zeroconf()
	tPoll = time.time()
	zinfo = zeroconf.get_service_info("_sccs._udp.local.", "Smart copy server._sccs._udp.local.")
	while time.time() - tPoll <= 10.0 and zinfo is None:
		time.sleep(1)
		zinfo = zeroconf.get_service_info("_sccs._udp.local.", "Smart copy server._sccs._udp.local.")
	if zinfo is None:
		raise RuntimeError("Cannot find the smart copy command server")
		
	if config['version']:
		## Smart copy command server info
		print zinfo
		
	else:
		outHost = socket.inet_ntoa(zinfo.address)
		outPort = zinfo.port
		try:
			inHost = socket.gethostname().split('-')[1].upper()
		except IndexError:
			inHost = socket.gethostname().upper()[:3]
		while len(inHost) < 3:
			inHost += "_"
		inPort = int(zinfo.properties['MESSAGEOUTPORT'], 10)
		refPort = int(zinfo.properties['MESSAGEREFPORT'], 10)
		
		context = zmq.Context()
		sockRef = context.socket(zmq.REQ)
		sockRef.connect("tcp://%s:%i" % (outHost, refPort))
		
		infs = []
		cmds = []
		if config['query']:
			infs.append( "Querying '%s'" % config['args'][0] )
			cmds.append( buildPayload(inHost, 'RPT', data=config['args'][0], refSocket=sockRef) )
			
		else:
			filenames = config['args'][:-1]
			destUser = config['args'][-1]
			
			# Validate the UCF username
			try:
				output = subprocess.check_output(['ssh', 'mcsdr@lwaucf1', 'ls /data/network/recent_data/%s' % destUser])
			except subprocess.CalledProcessError:
				raise RuntimeError("Invalid UCF username/path: %s" % destUser)
				
			# Process the input files
			for filename in filenames:
				## Parse the metadata
				try:
					filetags, barcodes, beam, date, isSpec = parseMetadata(filename)
				except KeyError:
					print "WARNING: could not parse '%s', skipping" % os.path.basename(filename)
					continue
					
				## Go!
				inHost = "DR%i" % beam
				_drPathCache = {}
				_done = []
				for oid,(filetag,barcode) in enumerate(zip(filetags, barcodes)):
					### Make sure we have a valid tag
					if filetag in ('', 'UNK'):
						print "WARNING: invalid filetag '%s' for observation %i of '%s', skipping" % (filetag, oid+1, filename)
						continue
						
					### See if we should transfer this file
					if config['obsID'] != -1:
						if oid not in config['obsID']:
							continue
							
					### See if we have already transferred this file
					if filetag in _done:
						continue
						
					### Get the path on the DR
					try:
						drPath = _drPathCache[(beam,barcode)]
					except KeyError:
						_drPathCache[(beam,barcode)] = getDRSUPath(beam, barcode)
						drPath = _drPathCache[(beam,barcode)]
						
					### Make the copy
					if drPath is None:
						print "WARNING: could not find path for DRSU '%s' on DR%i, skipping" % (barcode, beam)
						continue
						
					srcPath= "%s:%s/DROS/%s/%s" % (inHost, drPath, 'Spec' if isSpec else 'Rec', filetag)
					destPath = "%s:/mnt/network/recent_data/%s" % (inHost, destUser)
					
					try:
						host, hostpath = srcPath.split(':', 1)
					except ValueError:
						host, hostpath = '', srcPath
					if host == '':
						host = inHost
						hostpath = os.path.abspath(hostpath)
						
					try:
						dest ,destpath = destPath.split(':', 1)
					except ValueError:
						dest, destpath = '', destPath
					if dest == '':
						dest = inHost
						destpath = os.path.abspath(destpath)
						
					infs.append( "Queuing copy for %s:%s to %s:%s" % (host, hostpath, dest, destpath) )
					cmds.append( buildPayload(inHost, "SCP", data="%s:%s->%s:%s" % (host, hostpath, dest, destpath), refSocket=sockRef) )
					
					if config['metadata']:
						mtdPath = os.path.abspath(filename)
						destPath = "%s:/data/network/recent_data/%s" % ('lwaucf1', destUser)
						
						try:
							dest ,destpath = destPath.split(':', 1)
						except ValueError:
							dest, destpath = '', destPath
						if dest == '':
							dest = 'lwaucf1'
							destpath = os.path.abspath(destpath)
							
						infs.append( "Copying metadata %s to %s:%s" % (filename, dest, destpath) )
						cmds.append( ["rsync", "-e ssh", "-avH", mtdPath, "mcsdr@%s:%s" % (dest, destpath)] )
						
					### Update the done list
					_done.append( filetag )
		try:
			sockOut = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sockOut.settimeout(5)
			sockIn  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sockIn.bind(("0.0.0.0", inPort))
			sockIn.settimeout(5)
			
			for inf,cmd in zip(infs,cmds):
				print inf
				
				if inf[:4] != 'Copy':
					## Standard SmartCopy commands
					sockOut.sendto(cmd, (outHost, outPort))
					data, address = sockIn.recvfrom(MCS_RCV_BYTES)
					
					cStatus, sStatus, info = parsePayload(data)
					info = info.split('\n')
					if len(info) == 1:
						print cStatus, sStatus, info[0]
					else:
						print cStatus, sStatus
						for line in info:
							print "  %s" % line
							
				else:
					## Custom metadata scp command
					try:
						output = subprocess.check_output(cmd)
						print "  Done"
					except subprocess.CalledProcessError:
						print "  WARNING: failed to copy metadata, skipping"
					
			sockIn.close()
			sockOut.close()
		except socket.error as e:
			raise RuntimeError(str(e))
			
		sockRef.close()
		context.term()
		
	zeroconf.close()
	time.sleep(0.1)


if __name__ == "__main__":
	main(sys.argv[1:])
	
