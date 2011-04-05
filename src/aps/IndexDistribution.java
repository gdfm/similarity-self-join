package aps;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import aps.io.IndexItemArrayWritable;

/**
 * Computes the index length distribution.
 * The output is a list of <posting_list_length, number_of_posting_lists>.
 */
public class IndexDistribution extends Configured implements Tool {

	public static class IndexDistributionMapper extends MapReduceBase implements Mapper<LongWritable, IndexItemArrayWritable, VLongWritable, VLongWritable> {

		private final VLongWritable outKey = new VLongWritable();
		private final VLongWritable outValue = new VLongWritable();
		
		@Override
		public void map(LongWritable key, IndexItemArrayWritable value,
				OutputCollector<VLongWritable, VLongWritable> output, Reporter reporter) throws IOException {
			outKey.set(value.toIndexItemArray().length);
			outValue.set(1);
			output.collect(outKey, outValue);
		}		
	}
	
	public static class IndexDistributionReducer extends MapReduceBase implements Reducer<VLongWritable, VLongWritable, VLongWritable, VLongWritable> {
		
		private final VLongWritable outValue = new VLongWritable();
		
		@Override
		public void reduce(VLongWritable key, Iterator<VLongWritable> values,
				OutputCollector<VLongWritable, VLongWritable> output, Reporter reporter) throws IOException {
			long sum = 0;
			while (values.hasNext())
				sum += values.next().get();
			outValue.set(sum);
			output.collect(key, outValue);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage:\nIndexDistribution <index_path> [output_path]");
			System.exit(1);
		}
		Path inputPath = new Path(args[0], "part*");
		Path outputPath;
		if (args.length > 1) {
			outputPath = new Path(args[1]);
		} else {
			outputPath = new Path(args[0] + "-distribution");
		}
		
		JobConf conf = new JobConf(getConf(), IndexDistribution.class);
		if (conf == null) {
			return -1;
		}
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		conf.setOutputKeyClass(VLongWritable.class);
		conf.setOutputValueClass(VLongWritable.class);
		conf.setMapperClass(IndexDistributionMapper.class);
		conf.setCombinerClass(IndexDistributionReducer.class);
		conf.setReducerClass(IndexDistributionReducer.class);
		conf.set("mapred.job.name", "APS-" + outputPath.getName());
		conf.setNumTasksToExecutePerJvm(-1); // JVM reuse
		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new IndexDistribution(), args);
		System.exit(exitCode);
	}
}
