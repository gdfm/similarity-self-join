package aps;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import aps.io.VectorComponent;
import aps.io.VectorComponentArrayWritable;

public class TextVectorConverter extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), TextVectorConverter.class);
        if (conf == null) {
            return -1;
        }

        if (args.length < 2) {
            System.err.println("Usage:\n TextVectorConverter <text_input> <sequencefile_output>");
            System.exit(1);
        }

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(VectorComponentArrayWritable.class);

        conf.setMapperClass(TVCMapper.class);
        conf.setNumReduceTasks(0);

        JobClient.runJob(conf);
        return 0;
    }

    static class TVCMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, IntWritable, VectorComponentArrayWritable> {
        private final IntWritable outKey = new IntWritable();
        private final VectorComponentArrayWritable outValue = new VectorComponentArrayWritable();

        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<IntWritable, VectorComponentArrayWritable> output, Reporter reporter)
                throws IOException {
            String[] tokens = value.toString().trim().split("\\s+");
            int vectorID = Integer.valueOf(tokens[0]);
            VectorComponent[] vca = new VectorComponent[tokens.length - 1];
            for (int i = 1; i < tokens.length; i++) {
                vca[i - 1] = parseVectorComponent(tokens[i]);
            }
            Arrays.sort(vca, Collections.reverseOrder()); // reverse sort as natural order is in decreasing dimensions
                                                          // but here high frequency dimensions are the lower dimensions
            outKey.set(vectorID);
            outValue.set(vca);
            output.collect(outKey, outValue);
        }
    }

    static VectorComponent parseVectorComponent(String svmFeature) {
        String[] tokens = svmFeature.split(":");
        assert tokens.length == 2;
        long id = Long.parseLong(tokens[0]);
        double weight = Double.parseDouble(tokens[1]);
        return new VectorComponent(id, weight);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TextVectorConverter(), args);
        System.exit(exitCode);
    }
}
