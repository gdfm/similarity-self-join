package aps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import aps.io.GenericKey;
import aps.io.VectorComponentArrayWritable;

public class RemainderDistribution extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage:\nRemainderDistribution <index_path> <k_max> [output_path]");
			System.exit(1);
		}

		JobConf conf = new JobConf(getConf(), RemainderDistribution.class);
		if (conf == null) {
			return -1;
		}
		Path inputPath = new Path(args[0], "pruned");

		// pick kmax from the command line
		int kmax = Integer.parseInt(args[1]);
		if (kmax < 0) {
			System.err.println("<k_max> should be positive");
			System.exit(1);
		}

		Path outputPath;
		if (args.length > 2) {
			outputPath = new Path(args[2]);
		} else {
			outputPath = new Path(args[0] + "-distribution");
		}
		FileSystem fs = FileSystem.get(conf);
		MapFile.Reader reader = new MapFile.Reader(fs, inputPath.toString(), conf);
		IntWritable key = new IntWritable();
		VectorComponentArrayWritable value = new VectorComponentArrayWritable();
		reader.finalKey(key);
		int maxKey = key.get();

		for (int numStripes = 1; numStripes <= kmax; numStripes++) {
			for (int stripe = 0; stripe < numStripes; stripe++) {
				Path stripePath = new Path(outputPath, stripe + "-of-" + numStripes);
				SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, stripePath, IntWritable.class,
						VectorComponentArrayWritable.class);
				int from = GenericKey.StripePartitioner.minKeyInStripe(stripe, numStripes, maxKey);
				int to = from + GenericKey.StripePartitioner.numKeysInStripe(stripe, numStripes, maxKey);
				key.set(from);
				reader.getClosest(key, value);
				writer.append(key, value);
				while (reader.next(key, value) && key.get() < to)
					writer.append(key, value);
				System.out.println("Stripe " + (stripe + 1) + " of " + numStripes + " :\t" + writer.getLength());
				writer.close();
			}
			System.out.println();
		}
		fs.delete(outputPath, true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RemainderDistribution(), args);
		System.exit(exitCode);
	}
}
