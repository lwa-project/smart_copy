# SmartCopy Scripts Guide

This document describes the user-facing scripts provided by SmartCopy for
managing data copies, deletions, and queue operations at LWA stations. For
protocol-level details and system internals, see the SmartCopy ICD.

Some scripts are available on the DRs as well as the MCS task processor,
while others are intended to be run on the MCS task processor (where the
SmartCopy server runs). The per-script notes below indicate where each
script is expected to be used.

## Copy Operations

### smartCopy.py

*Available on: MCS task processor, DRs*

Queue one or more file copies for later processing.

```
smartCopy.py [-v] source [source ...] destination
```

| Argument | Description |
|----------|-------------|
| `source` | One or more filenames to copy (host:path format) |
| `destination` | Destination to copy to (host:path format) |
| `-v, --version` | Display version information |

Colons in file paths must be escaped with a backslash.

**Example:**
```bash
# Copy a file from DR1 to a local destination
smartCopy.py DR1:/data/file.dat /data/local/

# Copy multiple files to a remote destination
smartCopy.py DR1:/data/file1.dat DR1:/data/file2.dat leo:/data/archive/
```

### smartCopyHelper.py

*Available on: MCS task processor*

Parse metadata tarballs and queue copies of the associated observation data
to a UCF user directory.

```
smartCopyHelper.py [-v] [-o OBS] [-m] filename [filename ...] ucfuser
```

| Argument | Description |
|----------|-------------|
| `filename` | One or more metadata tarballs to examine |
| `ucfuser` | UCF destination username or path |
| `-o, --observations` | Comma-separated list of observation numbers to transfer (1-based; -1 = all). Default: `-1` |
| `-m, --metadata` | Include the metadata tarball with the copy |
| `-v, --version` | Display version information |


*Note: `-o, --observations` is only relevant for TBS observations.*

**Example:**
```bash
# Copy all observations from a metadata tarball
smartCopyHelper.py /data/metadata/obs123.tgz jsmith

# Copy specific observations with metadata
smartCopyHelper.py -m -o 1,3 /data/metadata/obs123.tgz jsmith
```

### smartCopyLeo.py

*Available on: MCS task processor, DRs*

Parse metadata tarballs and queue copies to the remote archive.

```
smartCopyLeo.py [-v] [-o OBS] [-m] [-a PROJECTS] filename [filename ...]
```

| Argument | Description |
|----------|-------------|
| `filename` | One or more metadata tarballs to examine |
| `-o, --observations` | Comma-separated list of observation numbers to transfer (1-based; -1 = all). Default: `-1` |
| `-m, --metadata` | Include the metadata tarball with the copy |
| `-a, --allowed-projects` | Comma-separated list of projects to copy non-spectrometer data for |
| `-v, --version` | Display version information |

*Note: `-o, --observations` is only relevant for TBS observations.*

**Example:**
```bash
# Copy all observations to the remote archive
smartCopyLeo.py /data/metadata/obs123.tgz

# Copy only spectrometer data plus specific projects
smartCopyLeo.py -a DD002,LC001 /data/metadata/obs123.tgz
```

## Delete Operations

### smartDelete.py

*Available on: MCS task processor, DRs*

Queue one or more file deletions.

```
smartDelete.py [-v] [-n] filename [filename ...]
```

| Argument | Description |
|----------|-------------|
| `filename` | One or more filenames to delete (host:path format) |
| `-n, --now` | Execute the deletion as soon as it reaches the front of the queue, rather than deferring to the next purge cycle |
| `-v, --version` | Display version information |

**Example:**
```bash
# Queue files for deletion at next purge
smartDelete.py DR1:/data/old_file.dat

# Delete immediately when the command is processed
smartDelete.py -n DR1:/data/old_file.dat
```

### smartDeleteHelper.py

*Available on: MCS task processor*

Parse metadata tarballs and queue deletion of the associated observation data.

```
smartDeleteHelper.py [-v] [-o OBS] [-n] filename [filename ...]
```

| Argument | Description |
|----------|-------------|
| `filename` | One or more metadata tarballs to examine |
| `-o, --observations` | Comma-separated list of observation numbers to delete (1-based; -1 = all). Default: `-1` |
| `-n, --now` | Execute the deletion as soon as it reaches the front of the queue |
| `-v, --version` | Display version information |

**Example:**
```bash
# Queue all observation data for deletion
smartDeleteHelper.py /data/metadata/obs123.tgz

# Immediately delete specific observations
smartDeleteHelper.py -n -o 2,4 /data/metadata/obs123.tgz
```

## Queue Control

### smartCopyPause.py

*Available on: MCS task processor*

Pause copy queue processing on one or more data recorders. Pausing sets the
global inhibit flag, preventing the station monitor from auto-resuming the
queue.

```
smartCopyPause.py [-v] [-a] [DR ...]
```

| Argument | Description |
|----------|-------------|
| `DR` | One or more data recorder names (e.g., `DR1`, `DR2`) |
| `-a, --all` | Pause copies on all DRs |
| `-v, --version` | Display version information |

**Example:**
```bash
# Pause a specific DR
smartCopyPause.py DR1

# Pause all DRs
smartCopyPause.py -a
```

### smartCopyResume.py

*Available on: MCS task processor*

Resume copy queue processing on one or more data recorders. Resuming clears
the global inhibit flag, allowing the station monitor to manage the queue.

```
smartCopyResume.py [-v] [-a] [DR ...]
```

| Argument | Description |
|----------|-------------|
| `DR` | One or more data recorder names (e.g., `DR1`, `DR2`) |
| `-a, --all` | Resume copies on all DRs |
| `-v, --version` | Display version information |

**Example:**
```bash
# Resume a specific DR
smartCopyResume.py DR1

# Resume all DRs
smartCopyResume.py -a
```

## Status Queries

### smartQuery.py

*Available on: MCS task processor*

Query the SmartCopy server for system and queue status information.

```
smartQuery.py [-v] query [query ...]
```

| Argument | Description |
|----------|-------------|
| `query` | One or more MIB entry names to query |
| `-v, --version` | Display version information |

Valid MIB entries:

| Entry | Description |
|-------|-------------|
| `SUMMARY` | System status (e.g., `NORMAL`) |
| `INFO` | Informational message |
| `LASTLOG` | Last log entry |
| `SUBSYSTEM` | Subsystem identifier (`SCM`) |
| `SERIALNO` | Serial number |
| `VERSION` | Software version |
| `OBSSTATUS_DR{n}` | Whether DR{n} is recording data |
| `QUEUE_SIZE_DR{n}` | Number of items in DR{n}'s copy queue |
| `QUEUE_STATUS_DR{n}` | Status of DR{n}'s copy queue |
| `QUEUE_STATS_DR{n}` | Queue statistics for DR{n} |
| `QUEUE_ENTRY_#` | Details of a specific copy command |
| `ACTIVE_ID_DR{n}` | Active copy command ID on DR{n} |
| `ACTIVE_STATUS_DR{n}` | Active copy status on DR{n} |
| `ACTIVE_BYTES_DR{n}` | Bytes transferred by active copy on DR{n} |
| `ACTIVE_PROGRESS_DR{n}` | Progress of active copy on DR{n} |
| `ACTIVE_SPEED_DR{n}` | Transfer speed of active copy on DR{n} |
| `ACTIVE_REMAINING_DR{n}` | Estimated time remaining on DR{n} |

**Example:**
```bash
# Check system status
smartQuery.py SUMMARY

# Check queue sizes on all DRs
smartQuery.py QUEUE_SIZE_DR1 QUEUE_SIZE_DR2 QUEUE_SIZE_DR3 QUEUE_SIZE_DR4 QUEUE_SIZE_DR5

# Monitor an active copy
smartQuery.py ACTIVE_PROGRESS_DR1 ACTIVE_SPEED_DR1 ACTIVE_REMAINING_DR1
```
