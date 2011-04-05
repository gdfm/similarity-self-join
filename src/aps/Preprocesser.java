package aps;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.TaskTracker.MapOutputServlet;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import aps.LexiconBuilder.Errors;
import aps.io.LexiconItem;
import aps.io.VectorComponent;
import aps.io.VectorComponentArrayWritable;
import aps.io.WholeFileInputFormat;
import aps.util.FileUtils;

import wt10g.Parser;
import wt10g.Stopper;
import wt10g.WT10GDocument;

public class Preprocesser extends Configured implements Tool {

    static final int MINIMUM_ID = 1; // vector ID <= 0 are reserved

    private static final String MAPPING = "docIDMap";
    private static final String DEFAULT_LEXICON = "lexicon/part-00000";
    private static final Log LOG = LogFactory.getLog(Preprocesser.class);

    public static class Vectorizer extends MapReduceBase implements
            Mapper<Text, Text, Text, VectorComponentArrayWritable> {

        private JobConf conf;
        private MapFile.Reader reader;
        private final Parser parser;
        private final Stopper stopper;

        public Vectorizer() throws IOException {
            parser = new Parser();
            stopper = new Stopper();
        }

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
            try {
                // open the lexicon in the DistributedCache
                reader = FileUtils.getLocalMapFileReader(conf);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        @Override
        public void map(Text key, Text value, OutputCollector<Text, VectorComponentArrayWritable> output,
                Reporter reporter) throws IOException {

            LOG.info("Parsing " + key);
            parser.parse(value.toString()); // parse the whole bundle

            for (WT10GDocument doc : parser) {
                key.set(doc.getDocID());

                String[] cleanList = stopper.removeStopwordsAndStem(doc.getText());
                assert (cleanList != null);

                VectorComponent buffer[] = vectorize(cleanList, reader);
                if (buffer != null) {
                    // sort descending (higher ID = higher frequency)
                    Arrays.sort(buffer);
                    // cast the buffer into a customized ArrayWritable
                    VectorComponentArrayWritable array = new VectorComponentArrayWritable(buffer);
                    output.collect(key, array);
                }
            }
        }
    }

    public static class IDMapper extends MapReduceBase implements
            Reducer<Text, VectorComponentArrayWritable, IntWritable, VectorComponentArrayWritable> {

        private MultipleOutputs mos;
        private int id = MINIMUM_ID;
        private final IntWritable outKey = new IntWritable();

        @Override
        public void configure(JobConf conf) {
            mos = new MultipleOutputs(conf);
        }

        @Override
        public void close() throws IOException {
            mos.close();
        }

        @Override
        public void reduce(Text docID, Iterator<VectorComponentArrayWritable> value,
                OutputCollector<IntWritable, VectorComponentArrayWritable> output, Reporter reporter)
                throws IOException {
            // translate the docID to int
            outKey.set(id++);
            mos.getCollector(MAPPING, reporter).collect(outKey, docID);
            output.collect(outKey, value.next());
        }
    }

    /**
     * Creates the input for similarity sorting it into descending order of MaxWeight(x)
     */
    public static class MaxWeightVectorizer extends MapReduceBase implements
            Mapper<Text, Text, DoubleWritable, VectorComponentArrayWritable> {

        private JobConf conf;
        private MapFile.Reader reader;
        private final Parser parser;
        private final Stopper stopper;
        private final DoubleWritable outKey;

        public MaxWeightVectorizer() throws IOException {
            parser = new Parser();
            stopper = new Stopper();
            outKey = new DoubleWritable();
        }

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
            try {
                // open the lexicon in the DistributedCache
                reader = FileUtils.getLocalMapFileReader(conf);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        @Override
        public void map(Text key, Text value, OutputCollector<DoubleWritable, VectorComponentArrayWritable> output,
                Reporter reporter) throws IOException {

            LOG.info("Parsing " + key);
            parser.parse(value.toString()); // parse the whole bundle
            try {
                for (WT10GDocument doc : parser) {
                    String[] cleanList = stopper.removeStopwordsAndStem(doc.getText());
                    assert (cleanList != null);

                    VectorComponent buffer[] = vectorize(cleanList, reader);
                    if (buffer != null) {
                        // sort descending (higher ID = higher frequency)
                        Arrays.sort(buffer);
                        // cast the buffer into a customized ArrayWritable
                        VectorComponentArrayWritable array = new VectorComponentArrayWritable(buffer);
                        outKey.set(array.maxWeight());
                        output.collect(outKey, array);
                    }
                }
            } catch (NoSuchElementException e) {
                // ignore parsing errors but report them
                reporter.incrCounter(Errors.PARSE_ERRORS, 1);
            }
        }
    }

    public static class MaxWeightReducer extends MapReduceBase implements
            Reducer<DoubleWritable, VectorComponentArrayWritable, IntWritable, VectorComponentArrayWritable> {

        private int id = MINIMUM_ID;
        private final IntWritable outKey = new IntWritable();

        @Override
        public void reduce(DoubleWritable maxWeight, Iterator<VectorComponentArrayWritable> value,
                OutputCollector<IntWritable, VectorComponentArrayWritable> output, Reporter reporter)
                throws IOException {
            while (value.hasNext()) {
                // create a new docID
                outKey.set(id++);
                output.collect(outKey, value.next());
            }
        }
    }

    public static class InverseDoubleComparator implements RawComparator<DoubleWritable> {
        private DoubleWritable.Comparator comp = new DoubleWritable.Comparator();

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -1 * comp.compare(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(DoubleWritable dw1, DoubleWritable dw2) {
            return -1 * Double.compare(dw1.get(), dw2.get());
        }
    }

    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = new JobConf(getConf(), Preprocesser.class);
        if (conf == null) {
            return -1;
        }

        if (args.length < 2) {
            System.err.println("Usage:\nPreprocesser <input_paths> <output_path> [lexicon_dir]");
            System.exit(1);
        }

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setInputFormat(WholeFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setMapOutputKeyClass(Text.class);
        // conf.setMapOutputKeyClass(DoubleWritable.class);
        conf.setMapOutputValueClass(VectorComponentArrayWritable.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(VectorComponentArrayWritable.class);

        conf.setMapperClass(Vectorizer.class);
        // conf.setMapperClass(MaxWeightVectorizer.class);
        conf.setReducerClass(IDMapper.class);
        // conf.setReducerClass(MaxWeightReducer.class);

        // conf.setOutputKeyComparatorClass(InverseDoubleComparator.class);
        conf.setNumReduceTasks(1);

        MultipleOutputs.addNamedOutput(conf, MAPPING, SequenceFileOutputFormat.class, IntWritable.class, Text.class);

        conf.setNumReduceTasks(1);

        String lexiconDir = DEFAULT_LEXICON;
        if (args.length >= 3)
            lexiconDir = args[2];
        DistributedCache.addCacheFile(URI.create(lexiconDir), conf);

        JobClient.runJob(conf);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Preprocesser(), args);
        System.exit(exitCode);
    }

    private static VectorComponent[] vectorize(String[] wordList, MapFile.Reader reader) throws IOException {
        // no useful word in this document
        if (wordList.length == 0)
            return null;
        // count occurrences
        Arrays.sort(wordList);
        Map<String, Integer> map = new HashMap<String, Integer>(wordList.length);
        // start with the first word
        String previous = wordList[0];
        int count = 0;
        for (String w : wordList) {
            if (!w.equals(previous)) {
                map.put(previous, count);
                previous = w;
                count = 0;
            }
            count++;
        }
        // put the last word
        assert previous.equals(wordList[wordList.length - 1]);
        map.put(previous, count);

        // normalize wrt |x| = sqrt(sum((x_i)^2))
        double magnitude = 0;
        for (Integer i : map.values()) {
            magnitude += Math.pow(i.intValue(), 2);
        }
        magnitude = Math.sqrt(magnitude);

        // buffer to sort the dimensions
        VectorComponent result[] = new VectorComponent[map.size()];
        LexiconItem lexValue = new LexiconItem();
        int i = 0;
        Text word = new Text();
        for (Entry<String, Integer> e : map.entrySet()) {
            // get the word ID from the lexicon
            word.set(e.getKey());
            reader.get(word, lexValue);
            long id = lexValue.getId();
            double weight = e.getValue() / magnitude; // normalize
            // fill the sort buffer
            result[i++] = new VectorComponent(id, weight);
        }
        return result;
    }
}
