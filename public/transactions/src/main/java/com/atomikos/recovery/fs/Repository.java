/**
 * Copyright (C) 2000-2024 Atomikos <info@atomikos.com>
 *
 * LICENSE CONDITIONS
 *
 * See http://www.atomikos.com/Main/WhichLicenseApplies for details.
 */

package com.atomikos.recovery.fs;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import com.atomikos.recovery.LogException;
import com.atomikos.recovery.LogReadException;
import com.atomikos.recovery.LogWriteException;
import com.atomikos.recovery.PendingTransactionRecord;

public interface Repository {

	void init() throws LogException;

        // Returns the disk-force-bundling coordination latch for the case that disk-force-bundling is enabled.
        // The latch is necessary upstream because of the CachedRepository wrapping the FileSystemRepository and the CachedRepository
        // doing it's own synchronization.
	CountDownLatch put(String id,PendingTransactionRecord pendingTransactionRecord) throws LogWriteException;
	
	PendingTransactionRecord get(String coordinatorId) throws LogReadException;

	Collection<PendingTransactionRecord> findAllCommittingCoordinatorLogEntries() throws LogReadException;
	
	Collection<PendingTransactionRecord> getAllCoordinatorLogEntries() throws LogReadException;

	void writeCheckpoint(Collection<PendingTransactionRecord> checkpointContent) throws LogWriteException;
	
	void close();
}
