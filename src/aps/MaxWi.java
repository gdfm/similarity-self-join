package aps;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import aps.io.VectorComponent;
import aps.io.VectorComponentArrayWritable;

public class MaxWi extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), MaxWi.class);
		if (conf == null) {
			return -1;
		}

		if (args.length < 2) {
			System.err.println("Usage:\n MaxWi <preprocessed_input> <output>");
			System.exit(1);
		}

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(MapFileOutputFormat.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(MaxWeightMapper.class);
		conf.setReducerClass(MaxWeightReducer.class);
		conf.setCombinerClass(MaxWeightReducer.class);

		JobClient.runJob(conf);
		return 0;
	}

	static class MaxWeightMapper extends MapReduceBase implements
			Mapper<IntWritable, VectorComponentArrayWritable, LongWritable, DoubleWritable> {

		@Override
		public void map(IntWritable vectorID, VectorComponentArrayWritable value,
				OutputCollector<LongWritable, DoubleWritable> output, Reporter reporter) throws IOException {
			for (VectorComponent vc : value.toVectorComponentArray()) {
				output.collect(new LongWritable(vc.getID()), new DoubleWritable(vc.getWeight()));
			}
		}
	}

	static class MaxWeightReducer extends MapReduceBase implements
			Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

		@Override
		public void reduce(LongWritable componentID, Iterator<DoubleWritable> values,
				OutputCollector<LongWritable, DoubleWritable> output, Reporter reporter) throws IOException {
			double max = 0, current;
			while (values.hasNext()) {
				current = values.next().get();
				max = Math.max(max, current);
			}
			output.collect(componentID, new DoubleWritable(max));
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxWi(), args);
		System.exit(exitCode);
	}
}
