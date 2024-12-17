/**
 * Copyright (C) 2000-2024 Atomikos <info@atomikos.com>
 *
 * LICENSE CONDITIONS
 *
 * See http://www.atomikos.com/Main/WhichLicenseApplies for details.
 */

package com.atomikos.recovery.fs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectStreamException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.atomikos.icatch.config.Configuration;
import com.atomikos.icatch.provider.ConfigProperties;
import com.atomikos.logging.Logger;
import com.atomikos.logging.LoggerFactory;
import com.atomikos.persistence.imp.LogFileLock;
import com.atomikos.recovery.LogException;
import com.atomikos.recovery.LogReadException;
import com.atomikos.recovery.LogWriteException;
import com.atomikos.recovery.PendingTransactionRecord;
import com.atomikos.util.VersionedFile;

public class FileSystemRepository implements Repository {

	private static final Logger LOGGER = LoggerFactory.createLogger(FileSystemRepository.class);
	private VersionedFile file;
        
        // make volatile to allow, for the DISK_FORCE_BUNDING_ENABLED=true case, the null-check in initChannelIfNecessary
        // to work properly without the need to acquire 
	private volatile FileChannel rwChannel = null;
	private LogFileLock lock_;
        
        // Enable/disable disk-force-bundling mode optimization.
        // Ffor heavy concurrent load this can significantly improve transaction throughput by bundling the disk force
        // of several transactions. Depending on the settings of the tuning options below this might in turn increase
        // individual transaction latency - test and tune, your mileage may vary.
        public static final String DISK_FORCE_BUNDLING_ENABLED_PROPERTY = "com.atomikos.icatch.disk_force_bunding_enabled";
        private boolean diskForceBundlingEnabled = false;
        // The maximum number of items to collect in a single batch.
        // Note that it makes no sense to make this number larger than the maximum number of concurrent threads, if you
        // do this and make the max_wait_time larger than 0 you will actually decrease commit throughput and increase latency.
        public static final String DISK_FORCE_BUNDLING_MAX_BUNDLE_SIZE_PROPERTY = "com.atomikos.icatch.disk_force_bunding_max_bundle_size";
        private int diskForceBundlingMaxBundleSize = 20;
        // set this to non-zero to reduce load on the disk and tune your load profile
        public static final String DISK_FORCE_BUNDLING_MAX_BUNDLING_WAIT_TIME_MS_PROPERTY = "com.atomikos.icatch.disk_force_bunding_max_bundling_wait_time_ms";
        private long diskForceBundlingMaxBundlingWaitTimeMs = 0;
        // size limit of the disk-force-bunding queue
        public static final String DISK_FORCE_BUNDLING_QUEUE_MAX_SIZE_PROPERTY = "com.atomikos.icatch.disk_force_bunding_max_queue_size";
        private int diskForceBundlingMaxQueueSize = 1000;
        
        // the queue to put ByteBuffer write requests into
        private final BlockingQueue<BufferHolder> diskForceBundlingQueue = new ArrayBlockingQueue<>(diskForceBundlingMaxQueueSize);
        
        // This read/write lock is used a bit atypically, the read-locks are used for the write & force operations,
        // the write lock for the renewal/checkpointing operations of the rwChannel.
        private final ReadWriteLock diskForceBundlingRwChannelLock = new ReentrantReadWriteLock();

	@Override
	public void init() throws LogException {
		ConfigProperties configProperties = Configuration.getConfigProperties();
		String baseDir = configProperties.getLogBaseDir();
		String baseName = configProperties.getLogBaseName();
		LOGGER.logDebug("baseDir " + baseDir);
		LOGGER.logDebug("baseName " + baseName);
                
                diskForceBundlingEnabled = Boolean.parseBoolean(configProperties.getProperty(DISK_FORCE_BUNDLING_ENABLED_PROPERTY, "false"));
                diskForceBundlingMaxBundleSize = Integer.parseInt(configProperties.getProperty(DISK_FORCE_BUNDLING_MAX_BUNDLE_SIZE_PROPERTY, "20"));
                diskForceBundlingMaxBundlingWaitTimeMs = Long.parseLong(configProperties.getProperty(DISK_FORCE_BUNDLING_MAX_BUNDLING_WAIT_TIME_MS_PROPERTY, "0"));
                diskForceBundlingMaxQueueSize = Integer.parseInt(configProperties.getProperty(DISK_FORCE_BUNDLING_QUEUE_MAX_SIZE_PROPERTY, "1000"));
                
		LOGGER.logDebug("diskForceBundlingEnabled " + diskForceBundlingEnabled);
		LOGGER.logDebug("diskForceBundlingMaxBundleSize " + diskForceBundlingMaxBundleSize);
		LOGGER.logDebug("diskForceBundlingMaxBundlingWaitTimeMs " + diskForceBundlingMaxBundlingWaitTimeMs);
		LOGGER.logDebug("diskForceBundlingMaxQueueSize " + diskForceBundlingMaxQueueSize);
                
		lock_ = new LogFileLock(baseDir, baseName);
		LOGGER.logDebug("LogFileLock " + lock_);
		lock_.acquireLock();
		file = new VersionedFile(baseDir, baseName, ".log");
                
                // if disk-force-bundling is enabled, start the bundling thread
                if (diskForceBundlingEnabled) {
                    Thread t = new Thread(new Runnable() {
                        public void run() {
                            int totalCount = 0;
                            while (true) {
                                try {
                                    // start with one due to the first poll
                                    int count = 1;
                                    List<BufferHolder> holdersProcessed = new ArrayList<>(diskForceBundlingMaxBundleSize);
                                    BufferHolder bh = diskForceBundlingQueue.take();//poll(1, TimeUnit.MILLISECONDS);
                                    while (count < diskForceBundlingMaxBundleSize && bh != null) {
                                        count++;
                                        writeToChannel(bh.buff);
                                        holdersProcessed.add(bh);
                                        if (diskForceBundlingMaxBundlingWaitTimeMs <= 0) {
                                            // performance tests have shown this to be faster than poll(0, TimeUnit.MILLISECONDS),
                                            // at least on Windows (say, +10-15%), under heavy load
                                            bh = diskForceBundlingQueue.poll();
                                        }
                                        else {
                                            bh = diskForceBundlingQueue.poll(diskForceBundlingMaxBundlingWaitTimeMs, TimeUnit.MILLISECONDS);
                                        }
                                    }
                                    // the last one might be non-null but the batch-count already reached - don't forget to process that too...
                                    if (bh != null) {
                                        writeToChannel(bh.buff);
                                        holdersProcessed.add(bh);
                                    }
                                    writeForceChannel(false);
                                    for (BufferHolder bhp : holdersProcessed) {
                                        bhp.latch.countDown();
                                    }
                                    if (LOGGER.isTraceEnabled()) {
                                        totalCount = totalCount + holdersProcessed.size();
                                        LOGGER.logTrace("TotalCount: " + totalCount + ", last bundle size: " + holdersProcessed.size());
                                    }
                                }
                                catch (InterruptedException e) {
                                    LOGGER.logError("InterruptedException Problem in disk-force-bundling thread! Trying to continue.", e);
                                    // set-back interrupted flag
                                    Thread.currentThread().interrupt();
                                }
                                catch (IOException e) {
                                    LOGGER.logError("IOException Problem in disk-force-bundling thread! Trying to continue.", e);
                                }
                            }
                        }
                    }, "Disk-Force-Bundle-Thread");
                    t.setPriority(10);
                    t.setDaemon(true);
                    t.start();
                    LOGGER.logInfo("Started Disk-Force-Bundle Thread");
                }
                else {
                    LOGGER.logDebug("Running in classic (Non-Disk-Force-Bundle) mode");
                }
	}
        
	@Override
	public CountDownLatch put(String id, PendingTransactionRecord pendingTransactionRecord)
			throws IllegalArgumentException, LogWriteException {

		try {
			initChannelIfNecessary();
			return write(pendingTransactionRecord, true);
		} catch (IOException e) {
			throw new LogWriteException(e);
		}
	}

	private void initChannelIfNecessary()
			throws FileNotFoundException {
            if (diskForceBundlingEnabled) {
                if (rwChannel == null) {
                    try {
                        diskForceBundlingRwChannelLock.writeLock().lock();
                        rwChannel = file.openNewVersionForNioWriting();
                    }
                    finally {
                        diskForceBundlingRwChannelLock.writeLock().unlock();
                    }
                }
            }
            else {
                synchronized (this) {
                    if (rwChannel == null) {
			rwChannel = file.openNewVersionForNioWriting();
                    }
                }
            }
	}
	private CountDownLatch write(PendingTransactionRecord pendingTransactionRecord,
			boolean flushImmediately) throws IOException {
		String str = pendingTransactionRecord.toRecord();
		byte[] buffer = str.getBytes();
		ByteBuffer buff = ByteBuffer.wrap(buffer);
		return writeToFile(buff, flushImmediately);
	}

	private CountDownLatch writeToFile(ByteBuffer buff, boolean force)
			throws IOException {
            
            if (diskForceBundlingEnabled) {
                if (force) {
                    BufferHolder bh = new BufferHolder();
                    bh.buff = buff;
                    // directly offer without timeout, it is unlikely that the queue becomes full (other mechanisms will become stuck first,
                    // i. e. threads hanging on the latch.await and timing out there)
                    diskForceBundlingQueue.offer(bh);
                    return bh.latch;
                }
                else {
                    writeToChannel(buff);
                    return null;
                }
            }
            else {
                synchronized (this) {
                    rwChannel.write(buff);
                    if (force) {
                        rwChannel.force(false);
                    }
                    return null;
                }
            }
	}

	@Override
	public PendingTransactionRecord get(String coordinatorId) throws LogReadException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<PendingTransactionRecord> findAllCommittingCoordinatorLogEntries() throws LogReadException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<PendingTransactionRecord> getAllCoordinatorLogEntries() throws LogReadException {
		FileInputStream fis = null;
		try {
			fis = file.openLastValidVersionForReading();
		} catch (FileNotFoundException firstStart) {
			// the file could not be opened for reading;
			// merely return the default empty vector
		} 
		if (fis != null) {
			return readFromInputStream(fis);
		}
		//else
		return Collections.emptyList();
	}

	public static Collection<PendingTransactionRecord> readFromInputStream(
			InputStream in) throws LogReadException {
		Map<String, PendingTransactionRecord> coordinatorLogEntries = new HashMap<String, PendingTransactionRecord>();
		BufferedReader br = null;
		try {
			InputStreamReader isr = new InputStreamReader(in);
			br = new BufferedReader(isr);
			coordinatorLogEntries = readContent(br);
		} catch (Exception e) {
			LOGGER.logFatal("Error in recover", e);
			throw new LogReadException(e);
		} finally {
			closeSilently(br);
		}
		return coordinatorLogEntries.values();
	}
	
	static Map<String, PendingTransactionRecord> readContent(BufferedReader br)
			throws IOException {

		Map<String, PendingTransactionRecord> coordinatorLogEntries = new HashMap<String, PendingTransactionRecord>();
		String line = null;
		try {

			while ((line = br.readLine()) != null) {
				if (line.startsWith("{\"id\"")) {
					String msg = "Detected old log file format - please terminate all transactions under the old release first!";
					throw new IOException(msg);
				}
				PendingTransactionRecord coordinatorLogEntry = PendingTransactionRecord.fromRecord(line);
				coordinatorLogEntries.put(coordinatorLogEntry.id,coordinatorLogEntry);
			}

		} catch (java.io.EOFException unexpectedEOF) {
			LOGGER.logTrace(
					"Unexpected EOF - logfile not closed properly last time?",
					unexpectedEOF);
			// merely return what was read so far...
		} catch (StreamCorruptedException unexpectedEOF) {
			LOGGER.logTrace(
					"Unexpected EOF - logfile not closed properly last time?",
					unexpectedEOF);
			// merely return what was read so far...
		} catch (ObjectStreamException unexpectedEOF) {
			LOGGER.logTrace(
					"Unexpected EOF - logfile not closed properly last time?",
					unexpectedEOF);
			// merely return what was read so far...
		} catch (IllegalArgumentException couldNotParseLastRecord) {
			LOGGER.logTrace(
					"Unexpected record format - logfile not closed properly last time?",
					couldNotParseLastRecord);
			// merely return what was read so far...
		} catch (RuntimeException unexpectedEOF) {
			LOGGER.logWarning("Unexpected EOF - logfile not closed properly last time? " +line +" "
					+ unexpectedEOF);
		}
		return coordinatorLogEntries;
	}
	private static void closeSilently(BufferedReader fis) {
		try {
			if (fis != null)
				fis.close();
		} catch (IOException io) {
			LOGGER.logWarning("Fail to close logfile after reading - ignoring");
		}
	}
	
	@Override
	public void writeCheckpoint(Collection<PendingTransactionRecord> checkpointContent) throws LogWriteException {

		try {
                    if (diskForceBundlingEnabled) {
                        try {
                            diskForceBundlingRwChannelLock.writeLock().lock();
                            closeOutput();
                            rwChannel = file.openNewVersionForNioWriting();
                            for (PendingTransactionRecord coordinatorLogEntry : checkpointContent) {
                                    write(coordinatorLogEntry, false);
                            }
                            rwChannel.force(false);
                            file.discardBackupVersion();
                        }
                        finally {
                            diskForceBundlingRwChannelLock.writeLock().unlock();
                        }
                    }
                    else {
                        closeOutput();
                        
                        rwChannel = file.openNewVersionForNioWriting();
                        for (PendingTransactionRecord coordinatorLogEntry : checkpointContent) {
                                write(coordinatorLogEntry, false);
                        }
                        rwChannel.force(false);
                        file.discardBackupVersion();
                    }
		} catch (FileNotFoundException firstStart) {
			// the file could not be opened for reading;
			// merely return the default empty vector
		} catch (Exception e) {
			LOGGER.logFatal("Failed to write checkpoint", e);
			throw new LogWriteException(e);
		}
	}
	
	protected void closeOutput() throws IllegalStateException {
		try {
			if (file != null) {
				file.close();
			}
		} catch (IOException e) {
			throw new IllegalStateException("Error closing previous output", e);
		}
	}
	
	@Override
	public void close() {
            if (diskForceBundlingEnabled) {
		try {
                    diskForceBundlingRwChannelLock.writeLock().lock();
		    closeOutput();
		} catch (Exception e) {
			LOGGER.logWarning("Error closing file - ignoring", e);
		} finally {
                        diskForceBundlingRwChannelLock.writeLock().unlock();
			lock_.releaseLock();
		}
            }
            else {
                try {
			closeOutput();
		} catch (Exception e) {
			LOGGER.logWarning("Error closing file - ignoring", e);
		} finally {
			lock_.releaseLock();
		}
            }
	}
        
        /** Helper method to write to channel with proper locking for disk-force-budling
         * 
         * @param buff
         * @throws IOException 
         */
        private void writeToChannel(ByteBuffer buff) throws IOException {
            try {
                diskForceBundlingRwChannelLock.readLock().lock();
                rwChannel.write(buff);
            }
            finally {
                diskForceBundlingRwChannelLock.readLock().unlock();
            }
        }
        
        /** Helper method to disk-force channel with proper locking for disk-force-budling
         * 
         * @param forceMeta
         * @throws IOException 
         */
        private void writeForceChannel(boolean forceMeta) throws IOException {
            try {
                diskForceBundlingRwChannelLock.readLock().lock();
                rwChannel.force(forceMeta);
            }
            finally {
                diskForceBundlingRwChannelLock.readLock().unlock();
            }
        }
        
        /** Simple helper class to transfer ByteBuffer and countdown latch
         * from original/transaction thread to disk-force thread
         */
        private static class BufferHolder {
            private ByteBuffer buff;
            // coordination latch, once it's down the original/transaction thread
            // knows the buffer has been disk-forced
            private final CountDownLatch latch = new CountDownLatch(1);
        }
}
