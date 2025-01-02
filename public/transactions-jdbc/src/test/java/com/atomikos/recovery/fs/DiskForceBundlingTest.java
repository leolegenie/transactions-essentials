/**
 * Copyright (C) 2000-2024 Atomikos <info@atomikos.com>
 *
 * LICENSE CONDITIONS
 *
 * See http://www.atomikos.com/Main/WhichLicenseApplies for details.
 */
package com.atomikos.recovery.fs;

import com.atomikos.icatch.config.Configuration;
import static com.atomikos.icatch.config.Configuration.getConfigProperties;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.atomikos.logging.Logger;
import com.atomikos.logging.LoggerFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import static junit.framework.TestCase.assertTrue;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** This class is intended to check that the disk force bundling optimization does not cause
 * unexpected issues and by running enabled vs disabled also to compare performance. It also causes
 * semi-random 'functional' errors to also test the rollbacks.
 * 
 * The tests run against an H2 in memory DB.
 * 
 * The default settings run the test quite quickly, increase number of threads, transactions per thread
 * and other parameters to get more varied results.
 * 
 */
public class DiskForceBundlingTest {

    private static final Logger LOGGER = LoggerFactory.createLogger(DiskForceBundlingTest.class);

    private static int NB_THREADS = 10;
    private static int NB_TRANSACTIONS_PER_THREAD = 100;
    private static String DISK_FORCE_BUNDING_ENABLED = "true";
    // roughly make that many inserts 'fail' (leading to rollbacks)
    private static float FAILURE_RATIO = 0.1f;
    
    private UserTransaction userTransaction;
    private DataSource ds;

    @Before
    public void setUp() throws Exception {
        getConfigProperties().setProperty("com.atomikos.icatch.max_actives", String.valueOf(Integer.MAX_VALUE));
        getConfigProperties().setProperty("com.atomikos.icatch.checkpoint_interval", "20000");
        getConfigProperties().setProperty("com.atomikos.icatch.registered", "true");
        getConfigProperties().setProperty("com.atomikos.icatch.default_jta_timeout", "30000");
        //getConfigProperties().setProperty("com.atomikos.icatch.max_timeout", "30000");
        //getConfigProperties().setProperty("com.atomikos.icatch.default_jta_timeout","30000");
        getConfigProperties().setProperty(FileSystemRepository.DISK_FORCE_BUNDLING_ENABLED_PROPERTY, DISK_FORCE_BUNDING_ENABLED);
//        getConfigProperties().setProperty(FileSystemRepository.DISK_FORCE_BUNDLING_MAX_BUNDLE_SIZE_PROPERTY,"40");
//        getConfigProperties().setProperty(FileSystemRepository.DISK_FORCE_BUNDLING_MAX_BUNDLING_WAIT_TIME_MS_PROPERTY,"5");
//        getConfigProperties().setProperty(CachedRepository.DISK_FORCE_BUNDLING_MAX_COORDINATION_WAIT_TIME_MS,"0");

        userTransaction = new com.atomikos.icatch.jta.UserTransactionImp();
        AtomikosDataSourceBean ds = new AtomikosDataSourceBean();
        ds.setUniqueResourceName("H2-DUMMY-DB");

        JdbcDataSource ds2 = new JdbcDataSource();
        ds2.setURL("jdbc:h2:mem:mydatabase;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE");//"jdbc:h2:˜/test");
        ds2.setUser("sa");
        ds2.setPassword("sa");

        ds.setXaDataSource(ds2);
        ds.setConcurrentConnectionValidation(true);
        ds.setBorrowConnectionTimeout(5);

        ds.setPoolSize(NB_THREADS);
        ds.setTestQuery("SELECT 1 from dual");
        ds.init();

        prepareDB(ds);

        this.ds = ds;
    }

    @After
    public void tearDown() throws Exception {
        Configuration.shutdown(true);
        ((AtomikosDataSourceBean) ds).close();
    }

    @Test
    public void testFunctionality() throws Exception {
        PerftestRunnable[] rs = new PerftestRunnable[NB_THREADS];

        Thread[] threads = new Thread[NB_THREADS];
        for (int i = 0; i < threads.length; i++) {
            PerftestRunnable r = new PerftestRunnable(ds, userTransaction, i);
            threads[i] = new Thread(r, "Perfthread-" + i);
            threads[i].setDaemon(true);
            rs[i] = r;
        }

        long start = System.currentTimeMillis();
        for (int j = 0; j < threads.length; j++) {
            threads[j].start();
            LOGGER.logInfo("Started thread " + j);
        }
        int totalSuccessful = 0;
        int totalFailed = 0;
        int totalRolledback = 0;
        for (int j = 0; j < threads.length; j++) {
            threads[j].join();
            int successful = rs[j].successfulTransactions.get();
            int failed = rs[j].failedBackTransactions.get();
            int rolledback = rs[j].rolledBackTransactions.get();
            totalSuccessful = totalSuccessful + successful;
            totalFailed = totalFailed + failed;
            totalRolledback = totalRolledback + rolledback;
            LOGGER.logInfo("Successful: " + threads[j] + " - " + successful);
            LOGGER.logInfo("Failed:     " + threads[j] + " - " + failed);
            LOGGER.logInfo("Rolledback: " + threads[j] + " - " + rolledback);
        }
        LOGGER.logInfo("tx / sec: " + (NB_THREADS * NB_TRANSACTIONS_PER_THREAD) * 1000 / ((System.currentTimeMillis() - start)));
        int inserted = insertedCount();
        LOGGER.logInfo("total inserted records count: " + inserted);
        LOGGER.logInfo("total successful commits count: " + totalSuccessful);
        LOGGER.logInfo("total failed operations count: " + totalFailed);
        LOGGER.logInfo("total successful rolledback count: " + totalRolledback);

        // the total number of records found in the DB should be equal to the total number of successful commits
        assertTrue("Inserted not equals to successfully XA-comitted!", inserted == totalSuccessful);
        
        // all fails should have a corresponding successful rollback under normal circumstances (probably not in case the
        // resource or system misbehaves (e. g. cannot write to disk temporarily or so))
        assertTrue("Failed not equals to successfully XA-rolledback!", totalFailed == totalRolledback);
    }

    protected void prepareDB(DataSource ds) throws SQLException {
        try (
                Connection conn = ds.getConnection(); Statement s = conn.createStatement();) {
            s.executeUpdate("drop table if exists accounts");
            System.err.println("Creating Accounts table...");
            s.executeUpdate("create table accounts ( "
                    + " account INTEGER, owner VARCHAR(300), balance BIGINT )");
        }
    }

    protected int insertedCount() throws SQLException {
        Connection c = null;
        PreparedStatement s = null;
        ResultSet rs = null;
        try {
            c = ds.getConnection();
            s = c.prepareStatement("select count(*), account from Accounts group by account");
            rs = s.executeQuery();
            int totalChanged = 0;
            while (rs.next()) {
                int count = rs.getInt(1);
                totalChanged = totalChanged + count;
                LOGGER.logDebug(rs.getInt(2) + " - " + count);
            }
            return totalChanged;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (s != null) {
                s.close();
            }
            if (c != null) {
                c.close();
            }
        }
    }

    private static class PerftestRunnable implements Runnable {

        private AtomicInteger successfulTransactions = new AtomicInteger(0);
        private AtomicInteger failedBackTransactions = new AtomicInteger(0);
        private AtomicInteger rolledBackTransactions = new AtomicInteger(0);
        private UserTransaction userTransaction;
        private DataSource ds;
        private int id;

        private PerftestRunnable(DataSource ds, UserTransaction userTransaction, int id) {
            this.ds = ds;
            this.userTransaction = userTransaction;
            this.id = id;
        }

        public void run() {
            for (int count = 0; count < NB_TRANSACTIONS_PER_THREAD; count++) {
                try {
                    userTransaction.begin();
                    performSQL(id, count);
                    //Thread.currentThread().sleep(100);
                    userTransaction.commit();
                    successfulTransactions.incrementAndGet();
                } catch (Exception e) {
                    failedBackTransactions.incrementAndGet();
                    if (!(e instanceof CustomException)) {
                        LOGGER.logError("Error", e);
                    }
                    try {
                        userTransaction.rollback();
                        rolledBackTransactions.incrementAndGet();
                    } catch (Exception e1) {
                        LOGGER.logError("Could not rollback transaction!", e);
                    }
                }
                if (count % 200 == 0) {
                    LOGGER.logDebug("Count: " + count + ", success: " + successfulTransactions.get()
                            + ", failed: " + failedBackTransactions.get() + ", rolledback: " + rolledBackTransactions.get());
                }
            }
            LOGGER.logInfo("Total: " + NB_TRANSACTIONS_PER_THREAD + ", success: " + successfulTransactions.get()
                    + ", failed: " + failedBackTransactions.get() + ", rolledback: " + rolledBackTransactions.get());
        }

        protected void performSQL(int id, int iteration) throws SQLException {
            Random rand = new Random();

            Connection c = null;
            PreparedStatement s = null;
            try {
                c = ds.getConnection();
                s = c.prepareStatement("insert into Accounts values ( ?, ?, ? )");
                s.setInt(1, id);
                s.setString(2, "owner" + id);
                int x = rand.nextInt(NB_TRANSACTIONS_PER_THREAD);
                s.setInt(3, x);
                s.executeUpdate();
                // make some transactions fail
                if (x > (NB_TRANSACTIONS_PER_THREAD * FAILURE_RATIO)) {
                    throw new CustomException();
                }
            } catch (SQLException e) {
                if (!(e instanceof CustomException)) {
                    LOGGER.logError("SQL could not be executed", e);
                }
                throw e;
            } finally {
                if (s != null) {
                    s.close();
                }
                if (c != null) {
                    c.close();
                }
            }
        }
    }

    // just a marker exception
    private static class CustomException extends SQLException {
    }
}
