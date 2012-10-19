package edu.brown.markov;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableUtil;

public class MarkovEstimate implements Poolable, DynamicTransactionEstimate {
    private static final Logger LOG = Logger.getLogger(MarkovEstimate.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final CatalogContext catalogContext;
    
    // ----------------------------------------------------------------------------
    // GLOBAL DATA
    // ----------------------------------------------------------------------------
    
    protected float confidence = MarkovUtil.NULL_MARKER;
    protected float singlepartition;
    protected float abort;
    protected float greatest_abort = MarkovUtil.NULL_MARKER;

    // MarkovPathEstimator data 
    protected final List<MarkovVertex> path = new ArrayList<MarkovVertex>();
    protected final PartitionSet touched_partitions = new PartitionSet();
    protected final PartitionSet read_partitions = new PartitionSet();
    protected final PartitionSet write_partitions = new PartitionSet();

    private MarkovVertex vertex;
    private int batch;
    private long time;
    private boolean initializing = true;
    private boolean valid = true;
    
    // ----------------------------------------------------------------------------
    // PARTITION-SPECIFIC DATA
    // ----------------------------------------------------------------------------
    
    /**
     * The number of Statements executed at each partition
     */
    private final int touched[];
    
    // Probabilities
    private final float finished[];
    private final float read[];
    private final float write[];
    
    // ----------------------------------------------------------------------------
    // TRANSIENT DATA
    // ----------------------------------------------------------------------------
    
    // Cached
    protected PartitionSet finished_partitionset;
    protected PartitionSet touched_partitionset;
    protected PartitionSet most_touched_partitionset;
    protected PartitionSet read_partitionset;
    protected PartitionSet write_partitionset;
    
    private List<CountedStatement> query_estimate;
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTORS + INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public MarkovEstimate(CatalogContext catalogContext) {
        this.catalogContext = catalogContext;
        
        this.touched = new int[this.catalogContext.numberOfPartitions];
        this.finished = new float[this.catalogContext.numberOfPartitions];
        this.read = new float[this.catalogContext.numberOfPartitions];
        this.write = new float[this.catalogContext.numberOfPartitions];
        this.finish(); // initialize!
        this.initializing = false;
    }
    
    /**
     * Given an empty estimate object and the current Vertex, we fill in the
     * relevant information for the transaction coordinator to use.
     * @param estimate the Estimate object which will be filled in
     * @param v the Vertex we are currently at in the MarkovGraph
     */
    public MarkovEstimate init(MarkovVertex v, int batch) {
        assert(v != null);
        assert(this.initializing == false);
        assert(this.vertex == null) : "Trying to initialize the same object twice!";
        
        this.confidence = 1.0f;
        this.batch = batch;
        this.vertex = v;
        this.time = v.getExecutionTime();
        
        return (this);
    }
    
    protected void populateProbabilities() {
        assert(this.vertex != null);
        boolean is_singlepartition = (this.touched_partitions.size() == 1);
        float untouched_finish = 1.0f;
        float inverse_prob = 1.0f - this.confidence;
        for (int p = 0; p < this.catalogContext.numberOfPartitions; p++) {
            float finished_prob = this.vertex.getFinishProbability(p);
            if (this.touched_partitions.contains(p) == false) {
                this.setReadOnlyProbability(p, this.vertex.getReadOnlyProbability(p));
                this.setWriteProbability(p, this.vertex.getWriteProbability(p));
                if (is_singlepartition) untouched_finish = Math.min(untouched_finish, finished_prob);
            }
            if (this.isReadOnlyProbabilitySet(p) == false) {
                this.setReadOnlyProbability(p, this.vertex.getReadOnlyProbability(p));
            }
            if (this.isWriteProbabilitySet(p) == false) {
                this.setWriteProbability(p, this.vertex.getWriteProbability(p));
                // this.setWriteProbability(p, inverse_prob);
            }
            this.setFinishProbability(p, finished_prob);    
        } // FOR
        
        // Single-Partition Probability
        if (is_singlepartition) {
            if (trace.get())
                LOG.trace(String.format("Only one partition was touched %s. Setting single-partition probability to ???",
                          this.touched_partitions)); 
            this.setSinglePartitionProbability(untouched_finish);
        } else {
            this.setSinglePartitionProbability(1.0f - untouched_finish);
        }
        
        // Abort Probability
        // Only use the abort probability if we have seen at least ABORT_MIN_TXNS
        if (this.vertex.getTotalHits() >= MarkovGraph.MIN_HITS_FOR_NO_ABORT) {
            if (this.greatest_abort == MarkovUtil.NULL_MARKER) this.greatest_abort = 0.0f;
            this.setAbortProbability(this.greatest_abort);
        } else {
            this.setAbortProbability(1.0f);
        }
    }
    
    @Override
    public final boolean isInitialized() {
        return (this.vertex != null); //  && this.path.isEmpty() == false);
    }
    
    @Override
    public void finish() {
        if (this.initializing == false) {
            if (debug.get()) LOG.debug(String.format("Cleaning up MarkovEstimate [hashCode=%d]", this.hashCode()));
            this.vertex = null;
        }
        for (int i = 0; i < this.touched.length; i++) {
            this.touched[i] = 0;
            this.finished[i] = MarkovUtil.NULL_MARKER;
            this.read[i] = MarkovUtil.NULL_MARKER;
            this.write[i] = MarkovUtil.NULL_MARKER;
        } // FOR
        this.confidence = MarkovUtil.NULL_MARKER;
        this.singlepartition = MarkovUtil.NULL_MARKER;
        this.abort = MarkovUtil.NULL_MARKER;
        this.greatest_abort = MarkovUtil.NULL_MARKER;
        this.path.clear();
        
        this.touched_partitions.clear();
        this.read_partitions.clear();
        this.write_partitions.clear();
        
        if (this.finished_partitionset != null) this.finished_partitionset.clear();
        if (this.touched_partitionset != null) this.touched_partitionset.clear();
        if (this.most_touched_partitionset != null) this.most_touched_partitionset.clear();
        if (this.read_partitionset != null) this.read_partitionset.clear();
        if (this.write_partitionset != null) this.write_partitionset.clear();
        if (this.query_estimate != null) this.query_estimate.clear();
        this.valid = true;
    }
    
    /**
     * Returns true if this estimate is valid and can be used by the runtime system
     * @return
     */
    public boolean isValid() {
        if (this.vertex == null) {
            if (debug.get()) LOG.warn("MarkovGraph vertex is null");
            return (false);
        }
        return (this.valid);
        
//        for (int i = 0; i < this.touched.length; i++) {
//            if (this.finished[i] == MarkovUtil.NULL_MARKER) {
//                if (debug.get()) LOG.warn("finished[" + i + "] is null");
//                return (false);
//            } else if (this.read[i] == MarkovUtil.NULL_MARKER) {
//                if (debug.get()) LOG.warn("read[" + i + "] is null");
//                return (false);
//            } else if (this.write[i] == MarkovUtil.NULL_MARKER) {
//                if (debug.get()) LOG.warn("write[" + i + "] is null");
//                return (false);
//            }
//        } // FOR
//        if (this.singlepartition == MarkovUtil.NULL_MARKER) return (false);
//        if (this.userabort == MarkovUtil.NULL_MARKER) return (false);
//        return (true);
    }
    
    @Override
    public boolean hasQueryEstimate() {
        return (this.path.isEmpty() == false);
    }
    
    @Override
    public List<CountedStatement> getEstimatedQueries(int partition) {
        if (this.query_estimate == null) {
            this.query_estimate = new ArrayList<CountedStatement>();
        }
        if (this.query_estimate.isEmpty()) {
            for (MarkovVertex v : this.path) {
                PartitionSet partitions = v.getPartitions();
                if (partitions.contains(partition)) {
                    this.query_estimate.add(v.getCountedStatement());
                }
            } // FOR
        }
        return (this.query_estimate);
    }
    
    public List<MarkovVertex> getMarkovPath() {
//        assert(this.path.isEmpty() == false) :
//            "Trying to access MarkovPath before it was set";
        return (this.path);
    }
    
    /**
     * The last vertex in this batch
     * @return
     */
    public MarkovVertex getVertex() {
        return (this.vertex);
    }
    
    /**
     * Return that BatchId for this Estimate
     * @return
     */
    public int getBatchId() {
        return (this.batch);
    }

    /**
     * Increment an internal counter of the number of Statements
     * that are going to be executed at each partition
     * @param partition
     */
    protected void incrementTouchedCounter(int partition) {
        this.touched[partition]++;
    }
    
    protected void incrementTouchedCounter(PartitionSet partitions) {
        for (int p = 0; p < this.touched.length; p++) {
            if (partitions.contains(p)) this.touched[p]++;
        } // FOR
    }
    
    // ----------------------------------------------------------------------------
    // CONFIDENCE COEFFICIENT
    // ----------------------------------------------------------------------------
    
    public boolean isConfidenceCoefficientSet() {
        return (this.confidence != MarkovUtil.NULL_MARKER);
    }
    public float getConfidenceCoefficient() {
        return (this.confidence);
    }
    public void setConfidenceCoefficient(float probability) {
        this.confidence = probability;
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // SINGLE-PARTITIONED PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addSinglePartitionProbability(float probability) {
        this.singlepartition = probability + (this.singlepartition == MarkovUtil.NULL_MARKER ? 0 : this.singlepartition);
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    @Override
    public void setSinglePartitionProbability(float probability) {
        this.singlepartition = probability;
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    @Override
    public float getSinglePartitionProbability() {
        return (this.singlepartition);
    }
    @Override
    public boolean isSinglePartitionProbabilitySet() {
        return (this.singlepartition != MarkovUtil.NULL_MARKER);
    }

    
    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addReadOnlyProbability(int partition, float probability) {
        this.read[partition] = probability + (this.read[partition] == MarkovUtil.NULL_MARKER ? 0 : this.read[partition]); 
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    @Override
    public void setReadOnlyProbability(int partition, float probability) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.read.length) : "Invalid Partition: " + partition;
        this.read[partition] = probability;
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    @Override
    public float getReadOnlyProbability(int partition) {
        return (this.read[partition]);
    }
    @Override
    public boolean isReadOnlyProbabilitySet(int partition) {
        return (this.read[partition] != MarkovUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addWriteProbability(int partition, float probability) {
        this.write[partition] = probability + (this.write[partition] == MarkovUtil.NULL_MARKER ? 0 : this.write[partition]);
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    @Override
    public void setWriteProbability(int partition, float probability) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.write.length) : "Invalid Partition: " + partition;
        this.write[partition] = probability;
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    @Override
    public float getWriteProbability(int partition) {
        return (this.write[partition]);
    }
    @Override
    public boolean isWriteProbabilitySet(int partition) {
        return (this.write[partition] != MarkovUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // DONE PROBABILITY
    // ----------------------------------------------------------------------------

    @Override
    public void addFinishProbability(int partition, float probability) {
        this.finished[partition] = probability + (this.finished[partition] == MarkovUtil.NULL_MARKER ? 0 : this.finished[partition]);
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    @Override
    public void setFinishProbability(int partition, float probability) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.finished.length) : "Invalid Partition: " + partition;
        this.finished[partition] = probability;
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    @Override
    public float getFinishProbability(int partition) {
        return (this.finished[partition]);
    }
    @Override
    public boolean isFinishProbabilitySet(int partition) {
        return (this.finished[partition] != MarkovUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addAbortProbability(float probability) {
        this.abort = probability + (this.abort == MarkovUtil.NULL_MARKER ? 0 : this.abort); 
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    @Override
    public void setAbortProbability(float probability) {
        this.abort = probability;
        this.valid = this.valid && (probability != MarkovUtil.NULL_MARKER);
    }
    @Override
    public float getAbortProbability() {
        return (this.abort);
    }
    @Override
    public boolean isAbortProbabilitySet() {
        return (this.abort != MarkovUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // Convenience methods using EstimationThresholds object
    // ----------------------------------------------------------------------------
    
    private boolean checkProbabilityAllPartitions(float probs[], float threshold) {
        for (int partition = 0; partition < probs.length; partition++) {
            if (probs[partition] < threshold) return (false);
        } // FOR
        return (true);
    }
    
    @Override
    public boolean isSinglePartitioned(EstimationThresholds t) {
        return (this.getTouchedPartitions(t).size() <= 1);
    }
    @Override
    public boolean isAbortable(EstimationThresholds t) {
        return (this.abort >= t.getAbortThreshold());
    }
    @Override
    public boolean isReadOnlyPartition(EstimationThresholds t, int partition) {
        return (this.read[partition] >= t.getReadThreshold());
    }
    @Override
    public boolean isReadOnlyAllPartitions(EstimationThresholds t) {
        return (this.checkProbabilityAllPartitions(this.read, t.getReadThreshold()));
    }
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean isWritePartition(EstimationThresholds t, int partition) {
        return (this.write[partition] >= t.getWriteThreshold());
    }
    
    // ----------------------------------------------------------------------------
    // FINISHED PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean isFinishPartition(EstimationThresholds t, int partition) {
        return (this.finished[partition] >= t.getFinishedThreshold());
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public boolean isTargetPartition(EstimationThresholds t, int partition) {
        return ((1 - this.finished[partition]) >= t.getFinishedThreshold());
    }
    public int getTouchedCounter(int partition) {
        return (this.touched[partition]);
    }

    public long getExecutionTime() {
        return time;
    }
    
    private void getPartitions(PartitionSet partitions, float values[], float limit, boolean inverse) {
        partitions.clear();
        if (inverse) {
            for (int i = 0; i < values.length; i++) {
                if ((1 - values[i]) >= limit)
                    partitions.add(this.catalogContext.getAllPartitionIdArray()[i]);
            } // FOR
        } else {
            for (int i = 0; i < values.length; i++) {
                if (values[i] >= limit)
                    partitions.add(this.catalogContext.getAllPartitionIdArray()[i]);
            } // FOR
        }
    }

    @Override
    public PartitionSet getReadOnlyPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.read_partitionset == null) this.read_partitionset = new PartitionSet();
        this.getPartitions(this.read_partitionset, this.read, (float)t.getReadThreshold(), false);
        return (this.read_partitionset);
    }
    @Override
    public PartitionSet getWritePartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.write_partitionset == null) this.write_partitionset = new PartitionSet();
        this.getPartitions(this.write_partitionset, this.write, (float)t.getWriteThreshold(), false);
        return (this.write_partitionset);
    }
    @Override
    public PartitionSet getFinishPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.finished_partitionset == null) this.finished_partitionset = new PartitionSet();
        this.getPartitions(this.finished_partitionset, this.finished, (float)t.getFinishedThreshold(), false);
        return (this.finished_partitionset);
    }
    @Override
    public PartitionSet getTouchedPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.touched_partitionset == null) this.touched_partitionset = new PartitionSet();
        this.getPartitions(this.touched_partitionset, this.finished, t.getFinishedThreshold(), true);
        return (this.touched_partitionset);
    }
    
    public PartitionSet getMostTouchedPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.touched_partitionset == null) this.touched_partitionset = new PartitionSet();
        this.getPartitions(this.touched_partitionset, this.finished, t.getFinishedThreshold(), true);
        
        if (this.most_touched_partitionset == null) this.most_touched_partitionset = new PartitionSet();
        int max_ctr = 0;
        for (Integer p : this.touched_partitionset) {
            if (this.touched[p.intValue()] > 0 && max_ctr <= this.touched[p.intValue()]) {
                if (max_ctr == this.touched[p.intValue()]) this.most_touched_partitionset.add(p);
                else {
                    this.most_touched_partitionset.clear();
                    this.most_touched_partitionset.add(p);
                    max_ctr = this.touched[p.intValue()];
                }
            }
        } // FOR
        return (this.most_touched_partitionset);
    }
    
    @Override
    public String toString() {
        final String f = "%-6.02f"; 
        
        Map<String, Object> m0 = new LinkedHashMap<String, Object>();
        m0.put("BatchEstimate", (this.batch == MarkovUtil.INITIAL_ESTIMATE_BATCH ? "<INITIAL>" : "#" + this.batch));
        m0.put("HashCode", this.hashCode());
        m0.put("Valid", this.valid);
        m0.put("Vertex", this.vertex);
        m0.put("Confidence", this.confidence);
        m0.put("Single-P", (this.singlepartition != MarkovUtil.NULL_MARKER ? String.format(f, this.singlepartition) : "-"));
        m0.put("User Abort", (this.abort != MarkovUtil.NULL_MARKER ? String.format(f, this.abort) : "-"));
        
        String header[] = {
            "",
            "ReadO",
            "Write",
            "Finished",
            "TouchCtr",
        };
        Object rows[][] = new Object[this.touched.length][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new String[] {
                String.format("Partition #%02d", i),
                (this.read[i] != MarkovUtil.NULL_MARKER ? String.format(f, this.read[i]) : "-"),
                (this.write[i] != MarkovUtil.NULL_MARKER ? String.format(f, this.write[i]) : "-"),
                (this.finished[i] != MarkovUtil.NULL_MARKER ? String.format(f, this.finished[i]) : "-"),
                Integer.toString(this.touched[i]),
            };
        } // FOR
        Map<String, String> m1 = TableUtil.tableMap(header, rows);

        return (StringUtil.formatMapsBoxed(m0, m1));
    }
}
