/**
 * 
 */
package org.voltdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.voltdb.catalog.*;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.InitiateTaskMessage;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.*;

import edu.brown.BaseTestCase;
import edu.mit.dtxn.Dtxn;
import edu.mit.dtxn.Dtxn.FragmentResponse;
import edu.mit.hstore.HStoreCoordinatorNode;
import edu.mit.hstore.HStoreMessenger;

/**
 * @author pavlo
 *
 */
public class TestExecutionSite extends BaseTestCase {

    private static final int NUM_PARTITONS = 10;
    private static final int PARTITION_ID = 1;
    private static final long CLIENT_HANDLE = 1001;
    private static final long LAST_SAFE_TXN = -1;
    private static final BackendTarget BACKEND_TARGET = BackendTarget.HSQLDB_BACKEND;

    private static final String TARGET_PROCEDURE = "GetAccessData";
    private static final Object TARGET_PARAMS[] = new Object[] { new Long(1), new Long(1) };
    
    private static ExecutionSite site;
    
    private final Random rand = new Random(); 
    
    private class MockCallback implements RpcCallback<Dtxn.FragmentResponse> {
        @Override
        public void run(FragmentResponse parameter) {
            // Nothing!
        }
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITONS);
        
        if (site == null) {
            PartitionEstimator p_estimator = new PartitionEstimator(catalog_db);
            Site catalog_site = CollectionUtil.getFirst(CatalogUtil.getCluster(catalog).getSites());
            site = new ExecutionSite(PARTITION_ID, catalog, BACKEND_TARGET, p_estimator, null);
            
            Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();
            executors.put(PARTITION_ID, site);
            HStoreCoordinatorNode hstore = new HStoreCoordinatorNode(catalog_site, executors, p_estimator);
            site.setHStoreCoordinatorNode(hstore);
            HStoreMessenger messenger = new HStoreMessenger(executors, catalog_site);
            site.setHStoreMessenger(messenger);
            
        }
    }
    
    protected class BlockingObserver extends EventObserver {
        public final LinkedBlockingDeque<ClientResponse> lock = new LinkedBlockingDeque<ClientResponse>(1);
        
        @Override
        public void update(Observable o, Object arg) {
            assert(arg != null);
            ClientResponse response = (ClientResponse)arg;
            Logger.getRootLogger().info("BlockingObserver got update for txn #" + response.getTransactionId());
            lock.offer(response);
        }
        
        public ClientResponse poll() throws Exception {
            return (lock.poll(10, TimeUnit.SECONDS));
        }
    }
    
    /**
     * testGetExecutionEngine
     */
//    public void testGetExecutionEngine() {
//        assertNotNull(site.getExecutionEngine());
//    }
    
    /**
     * testGetProcedure
     */
    public void testGetProcedure() {
        VoltProcedure volt_proc = site.getProcedure(TARGET_PROCEDURE);
        assertNotNull(volt_proc);
        assertFalse(site.proc_pool.get(TARGET_PROCEDURE).contains(volt_proc));
        assertTrue(site.all_procs.get(TARGET_PROCEDURE).contains(volt_proc));
    }
    
    /**
     * testCreateVoltProcedures
     */
    public void testCreateVoltProcedures() {
        Procedure catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        int orig_count = site.all_procs.get(TARGET_PROCEDURE).size();
        int new_proc_count = 4;
        int count = 0;
        site.createVoltProcedures(catalog_proc, new_proc_count);
        for (VoltProcedure volt_proc : site.all_procs.get(TARGET_PROCEDURE)) {
            assertTrue(volt_proc.isInitialized());
            assertTrue(site.all_procs.get(TARGET_PROCEDURE).contains(volt_proc));
            count++;
        } // WHILE
        assertEquals(orig_count+new_proc_count, count);
    }
    
    /**
     * testRunClientResponse
     */
//    public void testRunClientResponse() throws Exception {
//        Thread thread = new Thread(site);
//        thread.start();
//        
//        VoltProcedure volt_proc = site.getProcedure(TARGET_PROCEDURE);
//        assertNotNull(volt_proc);
//        assertFalse(site.proc_pool.get(TARGET_PROCEDURE).contains(volt_proc));
//        
//        // For now just check whether our VoltProcedure goes back in the pool
//        site.callback.update(null, new Pair<VoltProcedure, ClientResponse>(volt_proc, new ClientResponseImpl()));
//        
//        int tries = 5;
//        boolean found = false;
//        while (tries-- > 0) {
//            if (site.proc_pool.get(TARGET_PROCEDURE).contains(volt_proc)) {
//                found = true;
//                break;
//            }
//            Thread.sleep(1000);
//        } // WHILE
//        assertTrue(found);
//        
//        thread.interrupt();
//    }
    
    /**
     * testRunInitiateTaskMessage
     */
    public void testRunInitiateTaskMessage() throws Exception {
        Thread thread = new Thread(site);
        thread.start();
        
        //
        // Peek in the VoltProcedure pool and register a callback
        // Use this callback to attach to the VoltProcedure and get the ClientResponse
        //
        VoltProcedure volt_proc = site.proc_pool.get(TARGET_PROCEDURE).peek();
        assertNotNull(volt_proc);
        BlockingObserver observer = new BlockingObserver();
        volt_proc.registerCallback(observer);
        
        //
        // Create an InitiateTaskMessage and shove that to the ExecutionSite
        //
        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName(TARGET_PROCEDURE);
        invocation.setParams(TARGET_PARAMS);
        invocation.setClientHandle(10001);
        
        Long txn_id = new Long(rand.nextInt());
        InitiateTaskMessage init_task = new InitiateTaskMessage(PARTITION_ID, PARTITION_ID, txn_id, CLIENT_HANDLE, true, true, invocation, LAST_SAFE_TXN); 
        site.doWork(init_task, new MockCallback());
        
        int tries = 1000;
        boolean found = false;
        while (tries-- > 0) {
            VoltProcedure running_volt_proc = site.getRunningVoltProcedure(txn_id);
            if (running_volt_proc != null) {
                assert(volt_proc == running_volt_proc);
                assertEquals(TARGET_PROCEDURE, running_volt_proc.getProcedureName());
                found = true;
                break;
            }
            Thread.sleep(1);
        } // WHILE
        assertTrue(found);
        
        //
        // Now check whether we got the ClientResponse
        //
        ClientResponse response = observer.poll();
        assertNotNull(response);
        assertEquals(1, response.getResults().length);
        Logger.getRootLogger().info("Finished checking transaction information");
        
        thread.interrupt();
        thread.join();
    }
    
    /**
     * testMultipleTransactions
     */
//    public void testMultipleTransactions() throws Exception {
//        final Thread thread = new Thread(site);
//        thread.setPriority(Thread.MIN_PRIORITY);
//        thread.start();
//        
//        //
//        // Fire up a bunch of transactions and make sure that they don't get the same VoltProcedure
//        //
//        final int num_xacts = 4;
//        final InitiateTaskMessage tasks[] = new InitiateTaskMessage[num_xacts];
//        final BlockingObserver observers[] = new BlockingObserver[num_xacts];
//        final VoltProcedure volt_procs[] = new VoltProcedure[num_xacts];
//        final long xact_ids[] = new long[num_xacts];
//        final boolean found[] = new boolean[num_xacts];
//        final boolean started[] = new boolean[num_xacts];
//        Thread check_threads[] = new Thread[num_xacts];
//        for (int i = 0; i < num_xacts; i++) {
//            xact_ids[i] = rand.nextLong();
//            found[i] = false;
//            started[i] = false;
//            
//            // Peek in the VoltProcedure pool and register a callback
//            int ii = 0;
//            for (VoltProcedure volt_proc : site.proc_pool.get(TARGET_PROCEDURE)) {
//                volt_procs[i] = volt_proc;
//                if (ii++ == i) break; 
//            } // FOR
//            assertNotNull(volt_procs[i]);
//            observers[i] = new BlockingObserver();
//            volt_procs[i].registerCallback(observers[i]);
//        
//            // Create an InitiateTaskMessage and shove that to the ExecutionSite
//            StoredProcedureInvocation invocation = new StoredProcedureInvocation();
//            invocation.setProcName(TARGET_PROCEDURE);
//            invocation.setParams(TARGET_PARAMS);
//            tasks[i] = new InitiateTaskMessage(PARTITION_ID, PARTITION_ID, xact_ids[i], CLIENT_HANDLE, true, true, invocation, LAST_SAFE_TXN);
//            
//            final int idx = i;
//            check_threads[i] = new Thread() {
//                public void run() {
//                    int tries = 1000;
//                    boolean done = false;
//                    started[idx] = true;
//                    site.doWork(tasks[idx]);
//                    while (tries-- > 0 && !done) {
//                        // Try to grab the running VoltProcedure handle
//                        VoltProcedure running_volt_proc = site.getRunningVoltProcedure(xact_ids[idx]);
//                        if (!found[idx] && running_volt_proc != null) {
//                            assert(volt_procs[idx] == running_volt_proc) : "Expected to get VoltProcedure " + volt_procs[idx] + " but got " + running_volt_proc;
//                            assertEquals(TARGET_PROCEDURE, running_volt_proc.getProcedureName());
//                            Logger.getRootLogger().info("Got running VoltProcedure handle for txn #" + xact_ids[idx] + " [" + idx + "]");
//                            found[idx] = true;
//                        }
//                        done = done && found[idx];
//                        if (!done) {
//                            try {
//                                Thread.sleep(1);
//                            } catch (InterruptedException ex) {
//                                return;
//                            }
//                        }
//                    } // WHILE
//                    return;
//                }
//            };
//        } // WHILE
//        for (Thread t : check_threads) {
//            t.start();
//            Thread.sleep(10);
//        }
//        for (Thread t : check_threads) t.join();
//        
//        
//        //
//        // Check to make sure that we got what we were looking for 
//        //
//        for (int i = 0; i < num_xacts; i++) {
//            assert(found[i]) : "Failed to running VoltProcedure for txn #" + xact_ids[i] + " [" + i + "]";
//            
//            // Make sure that it didn't execute with the same VoltProcedure
//            for (int ii = 0; ii < num_xacts; ii++) {
//                if (i == ii) continue;
//                assertTrue(volt_procs[i] != volt_procs[ii]);
//            } // FOR
//        
//            // Now check whether we got the ClientResponse
//            ClientResponse response = observers[i].poll();
//            assertNotNull(response);
//            assertEquals(1, response.getResults().length);
//        } // FOR
//        
//        thread.interrupt();
//        thread.join();
//    }
}
