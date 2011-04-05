package aps;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import aps.io.LexiconItem;
import aps.io.WholeFileInputFormat;

import wt10g.Parser;
import wt10g.Stopper;
import wt10g.WT10GDocument;

public class LexiconBuilder extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(LexiconBuilder.class);

    @Override
    public int run(String[] args) throws Exception {
        // Create a JobConf using the Configuration processed by ToolRunner
        JobConf conf1 = new JobConf(getConf(), LexiconBuilder.class);
        if (conf1 == null) {
            return -1;
        }

        DateFormat df = new SimpleDateFormat("yyyyMMdd-HHmmss");
        Date date = new Date();

        if (args.length < 2) {
            System.err.println("Usage: LexiconBuilder <input_path> <output_path>");
            System.exit(1);
        }

        // first part

        // input wt10g html bundles
        Path inputPath = new Path(args[0]);
        // map file {word -> <id,count>}, sorted by word, id increases with count
        Path outputPath = new Path(args[1]);
        // tmpPath1 holds <word,count> pairs, sorted by word
        Path tmpPath1 = new Path("/tmp/temp-" + df.format(date) + "-A");
        // tmpPath2 holds <word,count> pairs, sorted by count
        Path tmpPath2 = new Path("/tmp/temp-" + df.format(date) + "-B");

        // FileSystem fs = FileSystem.get(conf1);
        // fs.deleteOnExit(tmpPath1);
        // fs.deleteOnExit(tmpPath2);

        FileInputFormat.setInputPaths(conf1, inputPath);
        FileOutputFormat.setOutputPath(conf1, tmpPath1);

        conf1.setInputFormat(WholeFileInputFormat.class);
        conf1.setOutputFormat(SequenceFileOutputFormat.class);

        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(LongWritable.class);

        conf1.setMapperClass(LexerMapper.class);
        conf1.setReducerClass(LexerReducer.class);
        conf1.setCombinerClass(LexerReducer.class);

        JobClient.runJob(conf1);

        // second part
        JobConf conf2 = new JobConf(getConf(), LexiconBuilder.class);
        if (conf2 == null) {
            return -1;
        }

        FileInputFormat.setInputPaths(conf2, tmpPath1);
        FileOutputFormat.setOutputPath(conf2, tmpPath2);

        conf2.setInputFormat(SequenceFileInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);

        conf2.setMapOutputKeyClass(LongWritable.class);
        conf2.setMapOutputValueClass(Text.class);

        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(LongWritable.class);

        conf2.setMapperClass(InverseMapper.class);
        conf2.setReducerClass(SorterReducer.class);

        JobClient.runJob(conf2);

        // third part
        JobConf conf3 = new JobConf(getConf(), LexiconBuilder.class);
        if (conf3 == null) {
            return -1;
        }

        FileInputFormat.setInputPaths(conf3, tmpPath2);
        FileOutputFormat.setOutputPath(conf3, outputPath);

        conf3.setInputFormat(TextInputFormat.class);
        conf3.setOutputFormat(MapFileOutputFormat.class);

        conf3.setOutputKeyClass(Text.class);
        conf3.setOutputValueClass(LexiconItem.class);

        conf3.setMapperClass(IDMapper.class);
        conf3.setReducerClass(IdentityReducer.class);

        JobClient.runJob(conf3);

        return 0;
    }

    static enum Errors {
        PARSE_ERRORS
    };

    static class LexerMapper extends MapReduceBase implements Mapper<Text, Text, Text, LongWritable> {

        private JobConf conf;
        private final Text outKey = new Text();
        private final LongWritable one = new LongWritable(1);
        private final Parser parser = new Parser();
        private Stopper stopper;

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
            try {
                this.stopper = new Stopper();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void map(Text key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                throws IOException {

            LOG.info("Analyzing input file: " + conf.get("map.input.file"));
            parser.parse(value.toString());
            try {
                for (WT10GDocument doc : parser) {

                    String[] wordList = doc.getText();
                    String[] cleanList = stopper.removeStopwordsAndStem(wordList);

                    // find the set of words in this document
                    Set<String> lexicon = new HashSet<String>(cleanList.length);
                    for (String w : cleanList)
                        lexicon.add(w);

                    // emit '1' for each word to compute the document frequency
                    for (String l : lexicon) {
                        outKey.set(l);
                        output.collect(outKey, one);
                    }
                }
            } catch (NoSuchElementException e) {
                // ignore parsing errors but report them
                reporter.incrCounter(Errors.PARSE_ERRORS, 1);
            }
        }
    }

    static class LexerReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output,
                Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext())
                sum += values.next().get();
            output.collect(key, new LongWritable(sum));
        }
    }

    // static class SorterMapper extends MapReduceBase implements Mapper<Text, LongWritable, LongWritable, Text> {
    //
    // @Override
    // public void map(Text key, LongWritable value, OutputCollector<LongWritable, Text> output, Reporter reporter)
    // throws IOException {
    // output.collect(value, key);
    // }
    //
    // }

    static class SorterReducer extends MapReduceBase implements Reducer<LongWritable, Text, Text, LongWritable> {

        @Override
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, LongWritable> output,
                Reporter reporter) throws IOException {
            while (values.hasNext())
                output.collect(values.next(), key);
        }

    }

    static class IDMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LexiconItem> {

        private final Text outKey = new Text();
        private final LexiconItem outValue = new LexiconItem();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, LexiconItem> output, Reporter reporter)
                throws IOException {
            // input file used TextOutputFormat, using \t as separator
            String wc[] = value.toString().split("\t");
            // checking we have 2 fields: <word,count>
            assert (wc.length == 2);
            outKey.set(wc[0]);
            long count = (Long.parseLong(wc[1]));
            outValue.set(key.get(), count);
            // id is the byte offset of the word in the file
            output.collect(outKey, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LexiconBuilder(), args);
        System.exit(exitCode);
    }
}