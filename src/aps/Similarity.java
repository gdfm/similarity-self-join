package aps;

import static java.lang.Double.compare;
import static java.lang.Math.min;
import static java.lang.Math.max;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import aps.io.GenericKey;
import aps.io.GenericValue;
import aps.io.HalfPair;
import aps.io.IndexItem;
import aps.io.IndexItemArrayWritable;
import aps.io.VectorComponent;
import aps.io.VectorComponentArrayWritable;
import aps.io.VectorPair;
import aps.util.FileUtils;

public class Similarity extends Configured implements Tool {

    public static final String PARAM_APS_MAXKEY = "aps.maxKey";
    public static final String PARAM_APS_STRIPES = "aps.stripes";
    public static final String PARAM_APS_THRESHOLD = "aps.threshold";
    public static final String PARAM_APS_REDUCER_PER_STRIPE = "aps.red_per_stripe";
    public static final int REPORTER_INTERVAL = 1000;

    private static final String PRUNED = "pruned";
    private static final int INDEX_INTERVAL = 2;
    private static final float DEFAULT_THRESHOLD = 0.7f;
    private static final int DEFAULT_FACTOR = 2;
    private static final int DEFAULT_SPREAD = 5;

    private static final String thresholdOptName = "threshold";
    private static final String maxwiOptName = "maxwi";
    private static final String factorOptName = "factor";
    private static final String stripesOptName = "stripes";
    private static final String spreadOptName = "spread";
    private static final String maxVectorIDOptName = "maxid";

    private static final Log LOG = LogFactory.getLog(Similarity.class);

    public static class IndexerMapper extends MapReduceBase implements
            Mapper<IntWritable, VectorComponentArrayWritable, LongWritable, IndexItem> {

        private float threshold;
        private MultipleOutputs mos;
        private Map<Long, Double> maxWi = new HashMap<Long, Double>();

        private final LongWritable outKey = new LongWritable();
        private final IndexItem outValue = new IndexItem();

        @Override
        public void configure(JobConf conf) {
            threshold = conf.getFloat(PARAM_APS_THRESHOLD, DEFAULT_THRESHOLD);
            mos = new MultipleOutputs(conf);
            // open the maxWeight_i file in the DistributedCache
            boolean succeded = FileUtils.readMaxWiFile(conf, maxWi);
            if (!succeded)
                throw new AssertionError("Could not read maxWi file");
        }

        @Override
        public void map(IntWritable vectorID, VectorComponentArrayWritable value,
                OutputCollector<LongWritable, IndexItem> output, Reporter reporter) throws IOException {

            VectorComponent[] vcarray = value.toVectorComponentArray();
            int prunedNumber = computeBound(vcarray);
            // int prunedNumber = computeBoundImproved(vcarray);
            // int prunedNumber = computeBoundImproved2(vcarray);
            double runningSum = 0, runningMax = 0;
            // save the pruned part of the vector
            if (prunedNumber > 0) {
                VectorComponentArrayWritable vcaw = new VectorComponentArrayWritable(Arrays.copyOf(vcarray,
                        prunedNumber));
                mos.getCollector(PRUNED, reporter).collect(vectorID, vcaw);
            }

            for (int i = 0; i < vcarray.length; i++) {
                // index the remaining part
                if (i >= prunedNumber) {
                    outKey.set(vcarray[i].getID());
                    outValue.set(vectorID.get(), vcarray[i].getWeight());
                    outValue.setMaxIndexed(vcarray[prunedNumber].getID());
                    outValue.setVectorMaxWeight(value.maxWeight());
                    outValue.setVectorLength(value.length());
                    outValue.setVectorSum(value.sumWeights());
                    outValue.setPositionalMaxWeight(runningMax);
                    outValue.setPositionalSum(runningSum);
                    output.collect(outKey, outValue);
                }
                // update running stats
                runningSum += vcarray[i].getWeight();
                runningMax = max(runningMax, vcarray[i].getWeight());
            }
            // sanity checks
            assert compare(value.maxWeight(), runningMax) == 0;
            assert compare(value.sumWeights(), runningSum) == 0;
        }

        private int computeBound(VectorComponent[] vcarray) {
            // find the maximum number of indexable components using the bounds
            double idxBound = 0;
            int prunedNumber = 0;
            for (VectorComponent vc : vcarray) {
                double mwi = maxWi.get(vc.getID());
                idxBound += mwi * vc.getWeight();
                if (compare(idxBound, threshold) >= 0)
                    return prunedNumber; // we are done
                prunedNumber++;
            }
            return prunedNumber;
        }

        private int computeBoundImproved(VectorComponent[] vcarray) {
            ArrayList<VectorComponent> pivot = new ArrayList<VectorComponent>(vcarray.length);
            int prunedNumber = 0;
            for (VectorComponent vc : vcarray) {
                // add a new component to the pivot (componentID, maxWi(componentID))
                pivot.add(new VectorComponent(vc.getID(), maxWi.get(vc.getID())));
                // build a new normalized pivot proportional to vcarray
                VectorComponent[] normalizedPivot = pivot.toArray(new VectorComponent[pivot.size()]);
                normalizedPivot = VectorComponentArrayWritable.componentWiseProduct(normalizedPivot, vcarray);
                normalizedPivot = VectorComponentArrayWritable.normaliseInPlace(normalizedPivot);
                double score = VectorComponentArrayWritable.dotProduct(normalizedPivot, vcarray);
                if (compare(score, threshold) >= 0)
                    return prunedNumber;
                prunedNumber++;
            }
            return prunedNumber;
        }

        private int computeBoundImproved2(VectorComponent[] vcarray) {
            ArrayList<VectorComponent> pivot = new ArrayList<VectorComponent>(vcarray.length);
            int prunedNumber = 0;
            double score = 0;
            for (VectorComponent vc : vcarray) {
                // add a new component to the pivot (componentID, maxWi(componentID))
                pivot.add(new VectorComponent(vc.getID(), maxWi.get(vc.getID())));
                // build a new normalized pivot proportional to vcarray
                VectorComponent[] normalizedPivot = pivot.toArray(new VectorComponent[pivot.size()]);
                normalizedPivot = VectorComponentArrayWritable.componentWiseProduct(normalizedPivot, vcarray);
                normalizedPivot = VectorComponentArrayWritable.normaliseInPlace(normalizedPivot);
                score += normalizedPivot[normalizedPivot.length - 1].getWeight()
                        * vcarray[vcarray.length - 1].getWeight();
                if (compare(score, threshold) >= 0)
                    return prunedNumber;
                prunedNumber++;
            }
            return prunedNumber;
        }

        @Override
        public void close() throws IOException {
            mos.close();
        }
    }

    public static class IndexerReducer extends MapReduceBase implements
            Reducer<LongWritable, IndexItem, LongWritable, IndexItemArrayWritable> {

        @Override
        public void reduce(LongWritable key, Iterator<IndexItem> values,
                OutputCollector<LongWritable, IndexItemArrayWritable> output, Reporter reporter) throws IOException {
            List<IndexItem> list = new LinkedList<IndexItem>();
            while (values.hasNext()) {
                list.add(new IndexItem(values.next())); // deep copy
            }
            if (list.size() > 1) { // do not output single valued posting lists
                IndexItem buffer[] = list.toArray(new IndexItem[list.size()]);
                // sort the posting list in order to generate pairs in order
                Arrays.sort(buffer); // TODO use Hadoop secondary sort
                output.collect(key, new IndexItemArrayWritable(buffer));
            }
        }

    }

    public static class SimilarityMapperIndex extends MapReduceBase implements
            Mapper<LongWritable, IndexItemArrayWritable, GenericKey, GenericValue> {

        private final GenericKey outKey = new GenericKey();
        private final GenericValue outValue = new GenericValue();
        private final HalfPair payload = new HalfPair();
        private float threshold;

        @Override
        public void map(LongWritable key, IndexItemArrayWritable value,
                OutputCollector<GenericKey, GenericValue> output, Reporter reporter) throws IOException {
            IndexItem[] postingList = value.toIndexItemArray();
            for (int i = 1; i < postingList.length; i++) {
                for (int j = 0; j < i; j++) {
                    IndexItem x = postingList[i];
                    IndexItem y = postingList[j];
                    // |y| >= t / maxweight(x) && |x| >= t / maxweight(y)
                    if (compare(x.vectorLength(), Math.ceil(threshold / y.vectorMaxWeight())) >= 0
                            && compare(y.vectorLength(), Math.ceil(threshold / x.vectorMaxWeight())) >= 0
                            // tight upper bound on similarity score
                            && compare(min(x.vectorMaxWeight() * y.vectorSum(), y.vectorMaxWeight() * x.vectorSum()),
                                    threshold) >= 0) {
                        // positional filter
                        // && compare(
                        // min(x.positionalMaxWeight() * y.positionalSum(),
                        // y.positionalMaxWeight() * x.positionalSum())
                        // + x.getWeight() * y.getWeight(), threshold) >= 0)
                        if (j % REPORTER_INTERVAL == 0)
                            reporter.progress();
                        int lpv = IndexItem.getLeastPrunedVectorID(x, y);
                        int mpv = IndexItem.getMostPrunedVectorID(x, y);
                        float psim = (float) (x.getWeight() * y.getWeight());
                        outKey.set(lpv, mpv);
                        payload.set(mpv, psim);
                        outValue.set(payload);
                        output.collect(outKey, outValue);
                        reporter.incrCounter(APS.ADDEND, 1);
                    }
                }
            }
        }

        @Override
        public void configure(JobConf conf) {
            this.threshold = conf.getFloat(PARAM_APS_THRESHOLD, DEFAULT_THRESHOLD);
        }
    }

    public static class SimilarityMapperInput extends MapReduceBase implements
            Mapper<IntWritable, VectorComponentArrayWritable, GenericKey, GenericValue> {

        private int nstripes;
        private final GenericKey outKey = new GenericKey();
        private final GenericValue outValue = new GenericValue();

        @Override
        public void map(IntWritable vectorID, VectorComponentArrayWritable value,
                OutputCollector<GenericKey, GenericValue> output, Reporter reporter) throws IOException {
            // vectors sort before pairs using a secondary key < MINIMUM_ID
            for (int i = 1; i <= nstripes; i++) {
                outKey.set(vectorID.get(), Preprocesser.MINIMUM_ID - i);
                outValue.set(value);
                output.collect(outKey, outValue);
            }
        }

        @Override
        public void configure(JobConf conf) {
            nstripes = conf.getInt(PARAM_APS_STRIPES, 1);
        }
    }

    public static class SimilarityCombiner extends MapReduceBase implements
            Reducer<GenericKey, GenericValue, GenericKey, GenericValue> {

        private final HalfPair payload = new HalfPair();
        private final GenericValue outValue = new GenericValue();

        @Override
        public void reduce(GenericKey key, Iterator<GenericValue> values,
                OutputCollector<GenericKey, GenericValue> output, Reporter reporter) throws IOException {
            if (key.getSecondary() < Preprocesser.MINIMUM_ID) { // vector
                output.collect(key, values.next());
                if (values.hasNext())
                    assert false : "Vectors should not get grouped by combiner: " + key;
            } else { // addend
                reporter.progress();
                int counter = 0;
                float sim = 0;
                HalfPair hp = null;
                while (values.hasNext()) {
                    hp = (HalfPair) values.next().get();
                    sim += hp.getSimilarity();
                    if (counter++ % REPORTER_INTERVAL == 0)
                        reporter.progress();
                }
                if (hp != null) {
                    payload.set(hp.getID(), sim);
                    outValue.set(payload);
                    output.collect(key, outValue);
                } else {
                    assert false : "There is nothing to combine!";
                }
            }
        }
    }

    public static class SimilarityReducer extends MapReduceBase implements
            Reducer<GenericKey, GenericValue, VectorPair, FloatWritable> {

        private float threshold;
        boolean haspruned = false;
        private final Map<Integer, VectorComponentArrayWritable> pruned = new HashMap<Integer, VectorComponentArrayWritable>();
        private final VectorPair outKey = new VectorPair();
        private final FloatWritable outValue = new FloatWritable();

        @Override
        public void reduce(GenericKey key, Iterator<GenericValue> values,
                OutputCollector<VectorPair, FloatWritable> output, Reporter reporter) throws IOException {

            int vectorID = key.getPrimary();
            assert (key.getSecondary() == -1);
            // the vector is the first value
            VectorComponentArrayWritable vector = (VectorComponentArrayWritable) values.next().get();
            // half pairs are sorted such that all equal pairs are consecutive
            if (values.hasNext()) {
                reporter.incrCounter(APS.COMBINED, 1);
                HalfPair hp1 = (HalfPair) values.next().get();
                float similarity = hp1.getSimilarity();
                HalfPair hp2;
                int counter = 0;
                while (values.hasNext()) {
                    reporter.incrCounter(APS.COMBINED, 1);
                    if (counter++ % REPORTER_INTERVAL == 0)
                        reporter.progress();
                    hp2 = (HalfPair) values.next().get();
                    if (hp1.equals(hp2)) {
                        similarity += hp2.getSimilarity();
                    } else {
                        // output
                        outputHelper(hp1, vectorID, vector, similarity, output, reporter);
                        // start new stripe
                        hp1 = hp2;
                        similarity = hp1.getSimilarity();
                    }
                }
                // output the last one
                outputHelper(hp1, vectorID, vector, similarity, output, reporter);
            }
        }

        private boolean outputHelper(HalfPair hp, int vectorID, VectorComponentArrayWritable vector, float similarity,
                OutputCollector<VectorPair, FloatWritable> output, Reporter reporter) throws IOException {
            reporter.incrCounter(APS.EVALUATED, 1);
            reporter.progress();
            if (haspruned) {
                VectorComponentArrayWritable remainder = pruned.get(hp.getID());
                if (remainder != null) {
                    // cheap upper bound dot(x,y) <= min(|x|,|y|) * maxweight(x) * maxweight(y)
                    // double dotProdBound = min(remainder.length(), vector.length()) * remainder.getMaxWeight()
                    // * vector.getMaxWeight();
                    // if (compare(similarity + dotProdBound, threshold) >= 0)
                    similarity += VectorComponentArrayWritable.dotProduct(vector, remainder);

                } else {
                    LOG.warn("No remainder found for vector " + hp.getID());
                }
            }
            if (compare(similarity, threshold) >= 0) {
                int firstID = VectorPair.canonicalFirst(vectorID, hp.getID());
                int secondID = VectorPair.canonicalSecond(vectorID, hp.getID());
                outKey.set(firstID, secondID);
                outValue.set(similarity);
                output.collect(outKey, outValue);
                reporter.incrCounter(APS.SIMILAR, 1);
                return true;
            }
            return false;
        }

        @Override
        public void configure(JobConf conf) {
            this.threshold = conf.getFloat(PARAM_APS_THRESHOLD, DEFAULT_THRESHOLD);
            int reducerID = conf.getInt("mapred.task.partition", -1);
            int max = conf.getInt(PARAM_APS_MAXKEY, 0);
            int nstripes = conf.getInt(PARAM_APS_STRIPES, 1);
            int spread = conf.getInt(PARAM_APS_REDUCER_PER_STRIPE, 1);
            if (reducerID < 0 || max == 0) {
                LOG.error("Could not find stripe ID, reverting to whole rest file loading");
                LOG.debug("reducer = " + reducerID + "\t max = " + max + "\t nstripes = " + nstripes);
                // open the pruned part file in the DistrubutedCache
                haspruned = FileUtils.readRestFile(conf, pruned);
            } else {
                int stripe = GenericKey.StripePartitioner.findStripe(reducerID, spread);
                int from = GenericKey.StripePartitioner.minKeyInStripe(stripe, nstripes, max);
                int to = from + GenericKey.StripePartitioner.numKeysInStripe(stripe, nstripes, max);
                // read from 'from' included, to 'to' excluded
                LOG.info("Reducer " + reducerID + " loading stripe " + stripe + " of " + nstripes + " (" + from + ","
                        + (to - 1) + ")");
                haspruned = FileUtils.readRestFile(conf, pruned, from, to);
            }
            if (!haspruned)
                LOG.warn("No pruned file provided in DistributedCache");
            else
                LOG.info("Read " + pruned.size() + " entries from pruned file");
        }
    }

    private OptionSet parseOptions(OptionParser p, String[] args) throws IOException {
        OptionSet options = null;
        try {
            options = p.parse(args);
            if (options.has("?") || !options.has(maxwiOptName) || options.nonOptionArguments().size() < 2) {
                System.err.println("Usage:\nSimilarity <input_paths> <output_path> [options]");
                System.err.println("Please specify " + maxwiOptName + " option\n");
                p.printHelpOn(System.err);
                System.exit(0);
            }
        } catch (OptionException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return options;
    }

    @Override
    public int run(String[] args) throws IOException {
        OptionParser p = new OptionParser();
        OptionSpec<String> maxwiOpt = p.accepts(maxwiOptName, "location of maxWi map file (HDFS) REQUIRED")
                .withRequiredArg().ofType(String.class);
        OptionSpec<Float> thresholdOpt = p.accepts(thresholdOptName, "similarity threshold").withRequiredArg()
                .ofType(Float.class).defaultsTo(DEFAULT_THRESHOLD);
        OptionSpec<Integer> stripesOpt = p.accepts(stripesOptName, "number of stripes to divide the similarity matrix")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);
        OptionSpec<Integer> spreadOpt = p.accepts(spreadOptName, "number of reducers per stripe").withRequiredArg()
                .ofType(Integer.class).defaultsTo(DEFAULT_SPREAD);
        OptionSpec<Integer> factorOpt = p.accepts(factorOptName, "number of mappers per reducer").withRequiredArg()
                .ofType(Integer.class).defaultsTo(DEFAULT_FACTOR);
        OptionSpec<Integer> maxVectorIDOpt = p.accepts(maxVectorIDOptName, "maximum vector ID").withRequiredArg()
                .ofType(Integer.class);
        p.acceptsAll(Arrays.asList("h", "?"), "show help");

        OptionSet options = parseOptions(p, args);

        // to distinguish indexes built in successive runs
        DateFormat df = new SimpleDateFormat("yyyyMMdd-HHmmss");
        Date date = new Date();

        float threshold = options.valueOf(thresholdOpt); // threshold
        if (threshold < 0 || threshold >= 1) {
            System.err.println(thresholdOptName + " should be between 0 and 1");
            System.exit(1);
        }

        int numStripes = options.valueOf(stripesOpt); // number of stripes
        if (numStripes < 1) {
            System.err.println(stripesOptName + " should be > 0");
            System.exit(1);
        }

        // MapReduce parameters
        int spread = options.valueOf(spreadOpt); // how many reducers per stripe
        if (spread < 1) {
            System.err.println(spreadOptName + " should be > 0");
            System.exit(1);
        }

        int factor = options.valueOf(factorOpt); // how many mappers per reducer
        if (factor < 1) {
            System.err.println(factorOptName + " should be > 0");
            System.exit(1);
        }

        int maxKey = 0;
        if (options.has(maxVectorIDOpt)) {
            maxKey = options.valueOf(maxVectorIDOpt); // maximum value of the vector ID
            if (maxKey < 1) {
                System.err.println(maxVectorIDOptName + " should be > 0");
                System.exit(1);
            }
        }

        int numReducers = GenericKey.StripePartitioner.numReducers(numStripes, spread);
        int numMappers = numReducers * factor;
        int numBuckets = numMappers;

        // pick the file with max weights from command line
        String maxWiDir = options.valueOf(maxwiOpt);
        List<String> nonOptArgs = options.nonOptionArguments();

        LOG.info("Threshold set to " + threshold);
        LOG.info(String.format("Buckets: %1$-10s Factor: %2$-10s Stripes: %3$-10s Spread: %4$-10s Reducers: %5$-10s",
                numBuckets, factor, numStripes, spread, numReducers));

        // start building the jobs
        JobConf conf1 = new JobConf(getConf(), Similarity.class);
        conf1.setFloat(PARAM_APS_THRESHOLD, threshold);
        conf1.setInt(PARAM_APS_STRIPES, numStripes);
        DistributedCache.addCacheFile(URI.create(maxWiDir), conf1);

        Path inputPath = new Path(nonOptArgs.get(0));
        Path indexPath = new Path(nonOptArgs.get(0) + "-index-" + threshold + "-s" + numStripes + "_" + df.format(date));
        // index filtering pruned nested directory
        Path indexOnlyPath = new Path(indexPath, "part*");
        Path outputPath = new Path(nonOptArgs.get(1) + "-" + threshold + "-s" + numStripes);
        FileInputFormat.setInputPaths(conf1, inputPath);
        FileOutputFormat.setOutputPath(conf1, indexPath);

        conf1.setInputFormat(SequenceFileInputFormat.class);
        conf1.setOutputFormat(SequenceFileOutputFormat.class);
        conf1.setMapOutputKeyClass(LongWritable.class);
        conf1.setMapOutputValueClass(IndexItem.class);
        conf1.setOutputKeyClass(LongWritable.class);
        conf1.setOutputValueClass(IndexItemArrayWritable.class);
        conf1.setMapperClass(IndexerMapper.class);
        conf1.setReducerClass(IndexerReducer.class);

        // assuming input is sorted according to the key (vectorID) so that the
        // part files are locally sorted
        MultipleOutputs.addNamedOutput(conf1, PRUNED, SequenceFileOutputFormat.class, IntWritable.class,
                VectorComponentArrayWritable.class);

        // remove the stuff we added from the job name
        conf1.set("mapred.job.name", "APS-" + indexPath.getName().substring(0, indexPath.getName().length() - 16));
        conf1.setNumTasksToExecutePerJvm(-1); // JVM reuse
        conf1.setSpeculativeExecution(false);
        conf1.setCompressMapOutput(true);
        // hash the posting lists in different buckets to distribute the load
        conf1.setNumReduceTasks(numBuckets);

        RunningJob job1 = JobClient.runJob(conf1);

        // part 2
        JobConf conf2 = new JobConf(getConf(), Similarity.class);

        if (numStripes > 0)
            FileUtils.mergeRestFile(conf2, indexPath, PRUNED, INDEX_INTERVAL);

        MultipleInputs.addInputPath(conf2, indexOnlyPath, SequenceFileInputFormat.class, SimilarityMapperIndex.class);
        MultipleInputs.addInputPath(conf2, inputPath, SequenceFileInputFormat.class, SimilarityMapperInput.class);
        FileOutputFormat.setOutputPath(conf2, outputPath);
        conf2.setCombinerClass(SimilarityCombiner.class);
        conf2.setReducerClass(SimilarityReducer.class);
        conf2.setPartitionerClass(GenericKey.StripePartitioner.class);
        conf2.setOutputKeyComparatorClass(GenericKey.Comparator.class);
        conf2.setOutputValueGroupingComparator(GenericKey.PrimaryComparator.class);
        conf2.setMapOutputKeyClass(GenericKey.class);
        conf2.setMapOutputValueClass(GenericValue.class);
        conf2.setOutputKeyClass(VectorPair.class);
        conf2.setOutputValueClass(NullWritable.class);

        Counter numDocs = job1.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS");
        maxKey = maxKey > 0 ? maxKey : (int) numDocs.getValue();
        LOG.info("Setting max key value in input to " + maxKey);
        conf2.setInt(PARAM_APS_MAXKEY, maxKey);

        conf2.setInt(PARAM_APS_STRIPES, numStripes);
        conf2.setFloat(PARAM_APS_THRESHOLD, threshold);
        conf2.setInt(PARAM_APS_REDUCER_PER_STRIPE, spread);
        conf2.set("mapred.job.name", "APS-" + outputPath.getName());

        conf2.setNumTasksToExecutePerJvm(-1); // JVM reuse
        conf2.setSpeculativeExecution(false);
        conf2.setCompressMapOutput(true);
        conf2.setNumReduceTasks(numReducers);

        JobClient.runJob(conf2);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Similarity(), args);
        System.exit(exitCode);
    }

    public static enum APS {
        ADDEND, COMBINED, EVALUATED, SIMILAR,
    }
}
