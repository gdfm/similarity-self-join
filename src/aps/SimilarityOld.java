package aps;

import static java.lang.Double.compare;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
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

public class SimilarityOld extends Configured implements Tool {

    private static final String PARAM_APS_VERSION = "aps.version";
    private static final String PARAM_APS_THRESHOLD = "aps.threshold";
    private static final String PRUNED = "pruned";
    private static final float DEFAULT_THRESHOLD = 0.7f;
    private static final int INDEX_INTERVAL = 2;
    public static final int REPORTER_INTERVAL = 1000;
    private static final Log LOG = LogFactory.getLog(SimilarityOld.class);

    public static class IndexerMapper extends MapReduceBase implements
            Mapper<IntWritable, VectorComponentArrayWritable, LongWritable, IndexItem> {

        private JobConf conf;
        private double threshold;
        private int version;
        private MultipleOutputs mos;
        private Map<Long, Double> maxWi = new HashMap<Long, Double>();

        private final LongWritable outKey = new LongWritable();
        private final IndexItem outValue = new IndexItem();

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
            threshold = conf.getFloat(PARAM_APS_THRESHOLD, DEFAULT_THRESHOLD);
            version = conf.getInt(PARAM_APS_VERSION, 0);
            mos = new MultipleOutputs(conf);
            // open the maxWeight_i file in the DistributedCache
            if (version > 0) {
                boolean succeded = FileUtils.readMaxWiFile(conf, maxWi);
                if (!succeded)
                    throw new AssertionError("Could not read maxWi file");
            }
        }

        @Override
        public void map(IntWritable vectorID, VectorComponentArrayWritable value,
                OutputCollector<LongWritable, IndexItem> output, Reporter reporter) throws IOException {

            double bound = 0, mwi;
            long maxIndexed = 0;
            VectorComponentArrayWritable vcaw;
            VectorComponent[] vcarray;
            int prunedNumber;

            switch (version) {

            case 0:
                for (VectorComponent vc : value.toVectorComponentArray()) {
                    outKey.set(vc.getID());
                    outValue.set(vectorID.get(), vc.getWeight());
                    output.collect(outKey, outValue);
                }
                break;

            case 1:
                for (VectorComponent vc : value.toVectorComponentArray()) {
                    mwi = maxWi.get(vc.getID());
                    bound += mwi * vc.getWeight();
                    if (compare(bound, threshold) >= 0) { // index this part
                        maxIndexed = Math.max(maxIndexed, vc.getID());
                        outKey.set(vc.getID());
                        outValue.set(vectorID.get(), vc.getWeight());
                        output.collect(outKey, outValue);
                    }
                }
                value.setMaxIndexed(maxIndexed);
                mos.getCollector(PRUNED, reporter).collect(vectorID, value);
                break;

            case 2:
                vcarray = value.toVectorComponentArray();
                prunedNumber = 0;
                for (VectorComponent vc : vcarray) {
                    mwi = maxWi.get(vc.getID());
                    bound += mwi * vc.getWeight();
                    outKey.set(vc.getID());
                    outValue.set(vectorID.get(), vc.getWeight());
                    if (compare(bound, threshold) < 0) {
                        outValue.flag();
                        prunedNumber++;
                    }
                    // index everything anyway
                    output.collect(outKey, outValue);
                }
                // save the pruned part of the vector
                // LOG.info("Number of pruned components for vector " + vectorID + ": " + prunedNumber);
                if (prunedNumber > 0) {
                    vcaw = new VectorComponentArrayWritable(Arrays.copyOf(vcarray, prunedNumber));
                    // LOG.info("Collecting pruned part: " + vectorID + "\t" + vcaw);
                    mos.getCollector(PRUNED, reporter).collect(vectorID, vcaw);
                }
                break;

            case 3:
                // find the maximum indexable component using the bound
                vcarray = value.toVectorComponentArray();
                prunedNumber = 0;
                for (VectorComponent vc : vcarray) {
                    mwi = maxWi.get(vc.getID());
                    bound += mwi * vc.getWeight();
                    if (compare(bound, threshold) < 0) {
                        prunedNumber++;
                    } else {
                        maxIndexed = vc.getID();
                        break;
                    }
                }
                // save the pruned part of the vector
                if (prunedNumber > 0) {
                    vcaw = new VectorComponentArrayWritable(Arrays.copyOf(vcarray, prunedNumber));
                    mos.getCollector(PRUNED, reporter).collect(vectorID, vcaw);
                }
                // index the remaining part
                for (int i = prunedNumber; i < vcarray.length; i++) {
                    outKey.set(vcarray[i].getID());
                    outValue.set(vectorID.get(), vcarray[i].getWeight());
                    outValue.setMaxIndexed(maxIndexed);
                    output.collect(outKey, outValue);
                }
                break;

            default:
                throw new AssertionError("Unexpected version " + version);
            } // switch
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
                Arrays.sort(buffer); // TODO USE HADOOP'S SECONDARY SORT
                output.collect(key, new IndexItemArrayWritable(buffer));
            }
        }

    }

    public static class SimilarityMapper extends MapReduceBase implements
            Mapper<LongWritable, IndexItemArrayWritable, VectorPair, FloatWritable> {

        private JobConf conf;
        private int version;

        private final VectorPair outKey = new VectorPair();
        private final FloatWritable outValue = new FloatWritable();

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
            version = conf.getInt(PARAM_APS_VERSION, 0);
        }

        @Override
        public void map(LongWritable key, IndexItemArrayWritable value,
                OutputCollector<VectorPair, FloatWritable> output, Reporter reporter) throws IOException {
            IndexItem[] postingList = value.toIndexItemArray();

            switch (version) {
            case 0:
            case 1:
                for (int i = 1; i < postingList.length; i++) {
                    IndexItem x = postingList[i];
                    double weight = x.getWeight();
                    for (int j = 0; j < i; j++) {
                        if (j % REPORTER_INTERVAL == 0)
                            reporter.progress();
                        IndexItem y = postingList[j];
                        float similarity = (float) (weight * y.getWeight());
                        outKey.set(x.getID(), y.getID());
                        outValue.set(similarity);
                        output.collect(outKey, outValue);
                        reporter.incrCounter(Similarity.APS.ADDEND, 1);
                    }
                }
                break;

            case 2:
                for (int i = 1; i < postingList.length; i++) {
                    double weight = postingList[i].getWeight();
                    for (int j = 0; j < i; j++) {
                        if (j % REPORTER_INTERVAL == 0)
                            reporter.progress();
                        if (!postingList[i].isFlagged() || !postingList[j].isFlagged()) {
                            float similarity = (float) (weight * postingList[j].getWeight());
                            outKey.set(postingList[i].getID(), postingList[j].getID());
                            outValue.set(similarity);
                            output.collect(outKey, outValue);
                            reporter.incrCounter(Similarity.APS.ADDEND, 1);
                        }
                    }
                }
                break;

            default:
                throw new AssertionError("Unexpected version " + version);
            }
        }
    }

    public static class SimilarityCombiner extends MapReduceBase implements
            Reducer<VectorPair, FloatWritable, VectorPair, FloatWritable> {

        private final FloatWritable outValue = new FloatWritable();

        @Override
        public void reduce(VectorPair pair, Iterator<FloatWritable> values,
                OutputCollector<VectorPair, FloatWritable> output, Reporter reporter) throws IOException {
            reporter.progress();
            float similarity = 0;
            while (values.hasNext())
                similarity += values.next().get();
            outValue.set(similarity);
            output.collect(pair, outValue);
        }
    }

    public static class SimilarityReducer extends MapReduceBase implements
            Reducer<VectorPair, FloatWritable, VectorPair, FloatWritable> {

        private JobConf conf;
        private double threshold;
        private int version;
        private boolean hasPruned = false;
        private final Map<Integer, VectorComponentArrayWritable> pruned = new HashMap<Integer, VectorComponentArrayWritable>();
        private MapFile.Reader reader;
        private final IntWritable readerKey = new IntWritable();
        private final FloatWritable outValue = new FloatWritable();

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
            threshold = conf.getFloat(PARAM_APS_THRESHOLD, DEFAULT_THRESHOLD);
            version = conf.getInt(PARAM_APS_VERSION, 0);
            // open the pruned part file in the DistrubutedCache
            if (version > 0) {
                if (version == 1) {
                    try {
                        reader = FileUtils.getLocalMapFileReader(conf);
                        hasPruned = true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    hasPruned = FileUtils.readRestFile(conf, pruned);
                }
                if (!hasPruned)
                    LOG.warn("No pruned file provided in DistributedCache");
                else
                    LOG.info("Read " + pruned.size() + " entries from pruned file");
            }
        }

        @Override
        public void reduce(VectorPair pair, Iterator<FloatWritable> values,
                OutputCollector<VectorPair, FloatWritable> output, Reporter reporter) throws IOException {
            float similarity = 0;
            int addendNumber = 0;
            while (values.hasNext()) {
                if (addendNumber++ % REPORTER_INTERVAL == 0)
                    reporter.progress();
                similarity += values.next().get();
            }
            reporter.incrCounter(Similarity.APS.COMBINED, addendNumber);
            reporter.incrCounter(Similarity.APS.EVALUATED, 1);
            reporter.progress();
            int version = conf.getInt(PARAM_APS_VERSION, 0);
            VectorComponentArrayWritable v1, v2;
            switch (version) {
            case 0:
                break;

            case 1:
                if (hasPruned && reader != null) {
                    v1 = new VectorComponentArrayWritable();
                    readerKey.set(pair.getFirst());
                    reader.get(readerKey, v1);
                    v2 = new VectorComponentArrayWritable();
                    readerKey.set(pair.getSecond());
                    reader.get(readerKey, v2);
                    reporter.progress();
                    if (v1 != null && v2 != null) {
                        long minDim = Math.min(v1.getMaxIndexed(), v2.getMaxIndexed());
                        similarity += VectorComponentArrayWritable.dotProduct(v1.toVectorComponentArray(),
                                v2.toVectorComponentArray(), minDim);
                    } else {
                        LOG.info("Unable to find pruned components for pair (" + pair.getFirst() + ","
                                + pair.getSecond() + ")");
                    }
                }
                break;

            case 2:
                if (hasPruned) {
                    v1 = pruned.get(pair.getFirst());
                    v2 = pruned.get(pair.getSecond());
                    reporter.progress();
                    if (v1 != null && v2 != null) {
                        similarity += VectorComponentArrayWritable.dotProduct(v1, v2);
                    } else {
                        LOG.info("Unable to find pruned components for pair (" + pair.getFirst() + ","
                                + pair.getSecond() + ")");
                    }
                }
                break;

            default:
                throw new AssertionError("Unexpected version " + version);
            }

            if (compare(similarity, threshold) >= 0) {
                outValue.set(similarity);
                output.collect(pair, outValue);
                reporter.incrCounter(Similarity.APS.SIMILAR, 1);
                reporter.progress();
            }
        }
    }

    public static class SimilarityMapperV3Index extends MapReduceBase implements
            Mapper<LongWritable, IndexItemArrayWritable, GenericKey, GenericValue> {

        private final GenericKey outKey = new GenericKey();
        private final GenericValue outValue = new GenericValue();
        private final HalfPair payload = new HalfPair();

        @Override
        public void map(LongWritable key, IndexItemArrayWritable value,
                OutputCollector<GenericKey, GenericValue> output, Reporter reporter) throws IOException {
            IndexItem[] postingList = value.toIndexItemArray();
            for (int i = 1; i < postingList.length; i++) {
                for (int j = 0; j < i; j++) {
                    if (j % REPORTER_INTERVAL == 0)
                        reporter.progress();
                    int lpv = IndexItem.getLeastPrunedVectorID(postingList[i], postingList[j]);
                    int mpv = IndexItem.getMostPrunedVectorID(postingList[i], postingList[j]);
                    float psim = (float) (postingList[i].getWeight() * postingList[j].getWeight());
                    outKey.set(lpv, mpv);
                    payload.set(mpv, psim);
                    outValue.set(payload);
                    output.collect(outKey, outValue);
                    reporter.incrCounter(Similarity.APS.ADDEND, 1);
                }
            }
        }
    }

    public static class SimilarityMapperV3Input extends MapReduceBase implements
            Mapper<IntWritable, VectorComponentArrayWritable, GenericKey, GenericValue> {

        private final GenericKey outKey = new GenericKey();
        private final GenericValue outValue = new GenericValue();

        @Override
        public void map(IntWritable vectorID, VectorComponentArrayWritable value,
                OutputCollector<GenericKey, GenericValue> output, Reporter reporter) throws IOException {
            reporter.progress();
            // vectors sort before pairs using a secondary key < MIN_ID
            outKey.set(vectorID.get(), Preprocesser.MINIMUM_ID - 1);
            outValue.set(value);
            output.collect(outKey, outValue);
        }
    }

    public static class SimilarityCombinerV3 extends MapReduceBase implements
            Reducer<GenericKey, GenericValue, GenericKey, GenericValue> {

        private final HalfPair payload = new HalfPair();
        private final GenericValue outValue = new GenericValue();

        @Override
        public void reduce(GenericKey key, Iterator<GenericValue> values,
                OutputCollector<GenericKey, GenericValue> output, Reporter reporter) throws IOException {
            if (key.getSecondary() < Preprocesser.MINIMUM_ID) { // vector
                output.collect(key, values.next());
                if (values.hasNext())
                    throw new AssertionError("Vectors should not get grouped by combiner: " + key);
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
                    throw new AssertionError("There is nothing to combine!");
                }
            }
        }
    }

    public static class SimilarityReducerV3 extends MapReduceBase implements
            Reducer<GenericKey, GenericValue, VectorPair, FloatWritable> {

        private double threshold;
        private JobConf conf;
        boolean hasPruned = false;
        private final Map<Integer, VectorComponentArrayWritable> pruned = new HashMap<Integer, VectorComponentArrayWritable>();
        private final VectorPair outKey = new VectorPair();
        private final FloatWritable outValue = new FloatWritable();

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
            this.threshold = conf.getFloat(PARAM_APS_THRESHOLD, DEFAULT_THRESHOLD);
            // open the pruned part file in the DistrubutedCache
            hasPruned = FileUtils.readRestFile(conf, pruned);
            if (!hasPruned)
                LOG.warn("No pruned file provided in DistributedCache");
            else
                LOG.info("Read " + pruned.size() + " entries from pruned file");
        }

        @Override
        public void reduce(GenericKey key, Iterator<GenericValue> values,
                OutputCollector<VectorPair, FloatWritable> output, Reporter reporter) throws IOException {

            int vectorID = key.getPrimary();
            // LOG.info("VectorID: " + vectorID + " - Secondary Key = " + key.getSecondary());
            assert (key.getSecondary() == -1);
            // the vector is the first value
            VectorComponentArrayWritable vector = (VectorComponentArrayWritable) values.next().get();
            // vector pairs are sorted such that all equal pairs are consecutive
            if (values.hasNext()) {
                reporter.incrCounter(Similarity.APS.COMBINED, 1);
                HalfPair hp1 = (HalfPair) values.next().get();
                // LOG.info("Fist pair: " + vp);
                float similarity = hp1.getSimilarity();
                // assert (vectorID == vp.getLeastPrunedVectorID());
                // assert (vectorID != vp.getMostPrunedVectorID());
                HalfPair hp2;
                int counter = 0;
                while (values.hasNext()) {
                    reporter.incrCounter(Similarity.APS.COMBINED, 1);
                    if (counter++ % REPORTER_INTERVAL == 0)
                        reporter.progress();
                    hp2 = (HalfPair) values.next().get();
                    if (hp1.equals(hp2)) {
                        // LOG.info("Next pair in stripe : " + vp2);
                        similarity += hp2.getSimilarity();
                        // LOG.info("It is equal, summing similarities - New similarity = " + similarity);
                    } else {
                        // output
                        outputHelper(hp1, vectorID, vector, similarity, output, reporter);
                        // start new stripe
                        hp1 = hp2;
                        // LOG.info("New stripe start: " + vp);
                        similarity = hp1.getSimilarity();
                    }
                }
                // output the last one
                outputHelper(hp1, vectorID, vector, similarity, output, reporter);
            }
        }

        private boolean outputHelper(HalfPair hp, int vectorID, VectorComponentArrayWritable vector, float similarity,
                OutputCollector<VectorPair, FloatWritable> output, Reporter reporter) throws IOException {

            reporter.incrCounter(Similarity.APS.EVALUATED, 1);
            reporter.progress();
            if (hasPruned) {
                VectorComponentArrayWritable rest = pruned.get(hp.getID());
                if (rest != null)
                    similarity += VectorComponentArrayWritable.dotProduct(vector.toVectorComponentArray(),
                            rest.toVectorComponentArray());
            }
            if (compare(similarity, threshold) >= 0) {
                int firstID = VectorPair.canonicalFirst(vectorID, hp.getID());
                int secondID = VectorPair.canonicalSecond(vectorID, hp.getID());
                outKey.set(firstID, secondID);
                outValue.set(similarity);
                output.collect(outKey, outValue);
                reporter.incrCounter(Similarity.APS.SIMILAR, 1);
                return true;
            }
            return false;
        }
    }

    @Override
    public int run(String[] args) throws IOException {
        // MapReduce parameters
        int factor = 2; // how many mappers per reducer
        int numReducers = 28;
        int numMappers = numReducers * factor;
        int numBuckets = numMappers;
        // int numBuckets = 1;

        // to distinguish indexes built in successive runs
        DateFormat df = new SimpleDateFormat("yyyyMMdd-HHmmss");
        Date date = new Date();

        JobConf conf1 = new JobConf(getConf(), SimilarityOld.class);
        if (conf1 == null) {
            return -1;
        }

        if (args.length < 4) {
            System.err.println("Usage:\nSimilarity <input_paths> <output_path> <threshold> <version_number> [maxWi]");
            System.exit(1);
        }

        // pick the threshold from the command line
        float threshold = Float.parseFloat(args[2]);
        if (threshold < 0 || threshold >= 1) {
            System.err.println("<threshold> should be between 0 and 1");
            System.exit(1);
        }
        conf1.setFloat(PARAM_APS_THRESHOLD, threshold);

        // pick the version from command line
        int version = Integer.parseInt(args[3]);
        if (version < 0 || version > 3) {
            System.err.println("<version_number> can only be 0, 1, 2, 3");
            System.exit(1);
        }
        conf1.setInt(PARAM_APS_VERSION, version);

        // for advanced versions, the file with max weights must be provided
        if (version > 0) {
            if (args.length < 5) {
                System.err
                        .println("Usage:\nSimilarity <input_paths> <output_path> <threshold> <version_number> [maxWi]");
                System.err.println("For version " + version + " please provide maxWi file");
                System.exit(1);
            }
            String maxWiDir = args[4];
            DistributedCache.addCacheFile(URI.create(maxWiDir), conf1);
        }

        LOG.info("Running version " + version);

        Path inputPath = new Path(args[0]);
        Path indexPath = new Path(args[0] + "-index-" + threshold + "-v" + version + "_" + df.format(date));
        // index filtering pruned nested directory
        Path indexOnlyPath = new Path(indexPath, "part*");
        Path outputPath = new Path(args[1] + "-" + threshold + "-v" + version);

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

        // assuming input is sorted according to the key (vectorID) so that the part files are locally sorted
        MultipleOutputs.addNamedOutput(conf1, PRUNED, SequenceFileOutputFormat.class, IntWritable.class,
                VectorComponentArrayWritable.class);

        conf1.set("mapred.job.name", "APS-" + indexPath.getName().substring(0, indexPath.getName().length() - 16));
        conf1.setNumTasksToExecutePerJvm(-1); // JVM reuse
        conf1.setSpeculativeExecution(false);

        // for advanced versions, hash the posting lists in different buckets to distribute the load
        if (version > 0)
            conf1.setNumReduceTasks(numBuckets);

        JobClient.runJob(conf1);

        // part 2
        JobConf conf2 = new JobConf(getConf(), SimilarityOld.class);
        if (conf2 == null) {
            return -1;
        }

        conf2.setInt(PARAM_APS_VERSION, version);
        conf2.setFloat(PARAM_APS_THRESHOLD, threshold);

        if (version > 0)
            FileUtils.mergeRestFile(conf2, indexPath, PRUNED, INDEX_INTERVAL);

        /*
         * If version == 3 we need to send the input along with the pairs. We need to employ a custom partitioner to
         * direct the vectors from the input together with the pairs. The partitioner will use the vectorID as the key,
         * for pairs we use the vectorID from getLeastIndexedID. We need also a custom comparator to sort the vectors
         * before the pairs (secondary sort). Finally we need MultipleInputs to add the inputs (original input + index).
         * The input just uses a wrapper mapper SimilarityMapperV3Input, the index uses SimilarityMapperV3Index.
         * SimilarityReducerV3 needs to deal with a GenericWritable that wraps Vector and VectorPair
         */

        if (version == 3) {
            MultipleInputs.addInputPath(conf2, indexOnlyPath, SequenceFileInputFormat.class,
                    SimilarityMapperV3Index.class);
            MultipleInputs.addInputPath(conf2, inputPath, SequenceFileInputFormat.class, SimilarityMapperV3Input.class);
            FileOutputFormat.setOutputPath(conf2, outputPath);
            conf2.setCombinerClass(SimilarityCombinerV3.class);
            conf2.setReducerClass(SimilarityReducerV3.class);

            // partition and group by primary key, sort by primary and secondary
            conf2.setPartitionerClass(GenericKey.PrimaryPartitioner.class);
            conf2.setOutputKeyComparatorClass(GenericKey.Comparator.class);
            conf2.setOutputValueGroupingComparator(GenericKey.PrimaryComparator.class);

            conf2.setMapOutputKeyClass(GenericKey.class);
            conf2.setMapOutputValueClass(GenericValue.class);
        } else {
            FileInputFormat.setInputPaths(conf2, indexOnlyPath);
            FileOutputFormat.setOutputPath(conf2, outputPath);

            conf2.setInputFormat(SequenceFileInputFormat.class);
            conf2.setOutputFormat(SequenceFileOutputFormat.class);

            conf2.setMapperClass(SimilarityMapper.class);
            conf2.setCombinerClass(SimilarityCombiner.class);
            conf2.setReducerClass(SimilarityReducer.class);

            conf2.setMapOutputKeyClass(VectorPair.class);
            conf2.setMapOutputValueClass(FloatWritable.class);
        }

        conf2.setOutputKeyClass(VectorPair.class);
        conf2.setOutputValueClass(FloatWritable.class);
        conf2.set("mapred.job.name", "APS-" + outputPath.getName());
        conf2.setNumTasksToExecutePerJvm(-1); // JVM reuse
        conf2.setSpeculativeExecution(false);
        conf2.setCompressMapOutput(true);
        conf2.setNumReduceTasks(numReducers);
        if (version == 0)
            conf2.setNumMapTasks(numMappers);
        JobClient.runJob(conf2);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SimilarityOld(), args);
        System.exit(exitCode);
    }
}