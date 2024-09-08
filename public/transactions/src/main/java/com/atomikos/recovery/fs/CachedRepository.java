/**
 * Copyright (C) 2000-2024 Atomikos <info@atomikos.com>
 *
 * LICENSE CONDITIONS
 *
 * See http://www.atomikos.com/Main/WhichLicenseApplies for details.
 */

package com.atomikos.recovery.fs;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.atomikos.icatch.config.Configuration;
import com.atomikos.icatch.provider.ConfigProperties;
import com.atomikos.logging.Logger;
import com.atomikos.logging.LoggerFactory;
import com.atomikos.recovery.LogException;
import com.atomikos.recovery.LogReadException;
import com.atomikos.recovery.LogWriteException;
import com.atomikos.recovery.PendingTransactionRecord;
import com.atomikos.recovery.TxState;

public class CachedRepository  implements Repository {

	private static final Logger LOGGER = LoggerFactory.createLogger(CachedRepository.class);
	private boolean corrupt = false; 
	private final InMemoryRepository inMemoryCoordinatorLogEntryRepository;

	private final Repository backupCoordinatorLogEntryRepository;

	private volatile long numberOfPutsSinceLastCheckpoint = 0;
	private long checkpointInterval;
	private long forgetOrphanedLogEntriesDelay;
        
        public static final String DISK_FORCE_BUNDLING_MAX_COORDINATION_WAIT_TIME_MS = "com.atomikos.icatch.disk_force_bunding_max_coordination_wait_time_ms";
        // zero means wait as long as it takes
        private long diskForceBundlingMaxCoordinationWaitTimeMs = 0;
        
	public CachedRepository(
			InMemoryRepository inMemoryCoordinatorLogEntryRepository,
			Repository backupCoordinatorLogEntryRepository) {
		this.inMemoryCoordinatorLogEntryRepository = inMemoryCoordinatorLogEntryRepository;
		this.backupCoordinatorLogEntryRepository = backupCoordinatorLogEntryRepository;
	}

	@Override
	public void init() {
		//populate inMemoryCoordinatorLogEntryRepository with backup data
		
		ConfigProperties configProperties =	Configuration.getConfigProperties();
		checkpointInterval = configProperties.getCheckpointInterval();
		forgetOrphanedLogEntriesDelay = configProperties.getForgetOrphanedLogEntriesDelay();
                
                diskForceBundlingMaxCoordinationWaitTimeMs = Long.parseLong(configProperties.getProperty(DISK_FORCE_BUNDLING_MAX_COORDINATION_WAIT_TIME_MS, "0"));
		LOGGER.logDebug("diskForceBundlingMaxCoordinationWaitTimeMs " + diskForceBundlingMaxCoordinationWaitTimeMs);
		
		try {
			Collection<PendingTransactionRecord> coordinatorLogEntries = backupCoordinatorLogEntryRepository.getAllCoordinatorLogEntries();
			for (PendingTransactionRecord coordinatorLogEntry : coordinatorLogEntries) {
				inMemoryCoordinatorLogEntryRepository.put(coordinatorLogEntry.id, coordinatorLogEntry);
			}
			
			performCheckpoint();
		} catch (LogException e) {
			LOGGER.logFatal("Corrupted transaction log cache - restart JVM", e);
			corrupt = true;
		}
		
	}

	@Override
	public CountDownLatch put(String id, PendingTransactionRecord coordinatorLogEntry)
			throws IllegalArgumentException, LogWriteException {
		
		try {
                    CountDownLatch cdl;
                    synchronized (this) {
			if(needsCheckpoint()){
				performCheckpoint();
			}
			cdl = backupCoordinatorLogEntryRepository.put(id, coordinatorLogEntry);
			inMemoryCoordinatorLogEntryRepository.put(id, coordinatorLogEntry);
			numberOfPutsSinceLastCheckpoint++;
                    }
                    // If there is a latch returned, we are running in disk-force-bundling mode, so wait for the disk-force-bundling thread
                    // to signal disk-force has occured. The waiting is done outside of the synchronized block, otherwise no bundling would be
                    // possible in the first place.
                    if (cdl != null) {
                        if (diskForceBundlingMaxCoordinationWaitTimeMs > 0) {
                            boolean completed = cdl.await(diskForceBundlingMaxCoordinationWaitTimeMs, TimeUnit.MILLISECONDS);
                            if (!completed) {
                                LOGGER.logWarning("Disk force coordination time expired without completion for " + id + ", throwing exception. Another try will be done via checkpoint mechanism.");
                                throw new IllegalStateException("Disk force coordination time expired without completion for " + id + ", throwing exception. Another try will be done via checkpoint mechanism.");
                            }
                        }
                        else {
                            cdl.await();
                        }
                    }
		} catch (Exception e) {
                    LOGGER.logDebug("Issue occurred during write put, trying checkpoint.", e);
                    performCheckpoint();
		}
                return null;
	}

	private synchronized void performCheckpoint() throws LogWriteException {
		try {
			Collection<PendingTransactionRecord> coordinatorLogEntries =	purgeExpiredCoordinatorLogEntriesInStateAborting();
			backupCoordinatorLogEntryRepository.writeCheckpoint(coordinatorLogEntries);
			inMemoryCoordinatorLogEntryRepository.writeCheckpoint(coordinatorLogEntries);
			numberOfPutsSinceLastCheckpoint=0;
			corrupt = false;
		} catch (LogWriteException corrupted) {
			LOGGER.logWarning("Failed to write checkpoint - will try again later", corrupted);
			corrupt = true;
			throw corrupted;	
		} catch (Exception corrupted) {
			LOGGER.logWarning("Failed to write checkpoint - will try again later", corrupted);
			corrupt = true;
			throw new LogWriteException(corrupted);
		}
	}

	private Collection<PendingTransactionRecord> purgeExpiredCoordinatorLogEntriesInStateAborting() {
		Set<PendingTransactionRecord> ret = new HashSet<PendingTransactionRecord>();
		long now = System.currentTimeMillis();
		Collection<PendingTransactionRecord> coordinatorLogEntries = inMemoryCoordinatorLogEntryRepository.getAllCoordinatorLogEntries();
		for (PendingTransactionRecord coordinatorLogEntry : coordinatorLogEntries) {
			if (!canBeForgotten(now, coordinatorLogEntry)){
				ret.add(coordinatorLogEntry);
			}
		}
		return ret;
	}

	protected boolean canBeForgotten(long now,
			PendingTransactionRecord coordinatorLogEntry) {
		boolean ret = false;
		if ((coordinatorLogEntry.expires+forgetOrphanedLogEntriesDelay) < now) {
			TxState entryState = coordinatorLogEntry.state;
			if (!entryState.isHeuristic()) {
				//can happen for commits or aborts where the 'terminated' does not make it to the log after the resource aborted/committed
				LOGGER.logWarning("Purging orphaned entry from log: " + coordinatorLogEntry);
				ret = true;
			}
			
		}
		return ret;
	}

	private boolean needsCheckpoint() {
		return numberOfPutsSinceLastCheckpoint>=checkpointInterval || corrupt;
	}

	@Override
	public PendingTransactionRecord get(String coordinatorId) throws LogReadException  {
		return inMemoryCoordinatorLogEntryRepository.get(coordinatorId);
	}


	@Override
	public Collection<PendingTransactionRecord> findAllCommittingCoordinatorLogEntries() throws LogReadException {
		return inMemoryCoordinatorLogEntryRepository.findAllCommittingCoordinatorLogEntries();
	}

	

	@Override
	public void close() {
		backupCoordinatorLogEntryRepository.close();
		inMemoryCoordinatorLogEntryRepository.close();
	}

	@Override
	public Collection<PendingTransactionRecord> getAllCoordinatorLogEntries() {
		return inMemoryCoordinatorLogEntryRepository.getAllCoordinatorLogEntries();
	}

	@Override
	public void writeCheckpoint(
			Collection<PendingTransactionRecord> checkpointContent) {
		throw new UnsupportedOperationException();
	}
}
