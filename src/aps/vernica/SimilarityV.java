package aps.vernica;
import static java.lang.Double.compare;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import aps.Similarity;
import aps.io.Vector;
import aps.io.VectorComponent;
import aps.io.VectorComponentArrayWritable;
import aps.io.VectorPair;
import aps.util.FileUtils;

public class SimilarityV extends Configured implements Tool {

	private static final String PARAM_APS_THRESHOLD = "aps.threshold";
	private static final float DEFAULT_THRESHOLD = 0.7f;

	public static class SimVMapper extends MapReduceBase implements
			Mapper<IntWritable, VectorComponentArrayWritable, LongWritable, Vector> {

		private JobConf conf;
		private double threshold;
		private Map<Long, Double> maxWi = new HashMap<Long, Double>();

		private final LongWritable outKey = new LongWritable();
		private final Vector outValue = new Vector();

		@Override
		public void configure(JobConf conf) {
			this.conf = conf;
			threshold = conf.getFloat(PARAM_APS_THRESHOLD, DEFAULT_THRESHOLD);
			// open the maxWeight_i file in the DistributedCache
			boolean succeded = FileUtils.readMaxWiFile(conf, maxWi);
			if (!succeded)
				throw new AssertionError("Could not read maxWi file");
		}

		public void map(IntWritable vectorID, VectorComponentArrayWritable value,
				OutputCollector<LongWritable, Vector> output, Reporter reporter) throws IOException {

			double bound = 0, mwi;
			VectorComponent[] vcarray;
			// find the maximum indexable component using the bound
			vcarray = value.toVectorComponentArray();
			for (VectorComponent vc : vcarray) {
				mwi = maxWi.get(vc.getID());
				bound += mwi * vc.getWeight();
				if (compare(bound, threshold) >= 0) {
					outKey.set(vc.getID());
					outValue.set(vectorID.get(), value);
					output.collect(outKey, outValue);
				}
			}
		}
	}

	public static class SimVReducer extends MapReduceBase implements
			Reducer<LongWritable, Vector, VectorPair, FloatWritable> {

		private JobConf conf;
		private double threshold;
		private final VectorPair outKey = new VectorPair();
		private final FloatWritable outValue = new FloatWritable();

		@Override
		public void configure(JobConf conf) {
			this.conf = conf;
			threshold = conf.getFloat(PARAM_APS_THRESHOLD, DEFAULT_THRESHOLD);
		}

		@Override
		public void reduce(LongWritable key, Iterator<Vector> values,
				OutputCollector<VectorPair, FloatWritable> output, Reporter reporter) throws IOException {
			// define the counters to ease post-processing
			reporter.incrCounter(Similarity.APS.ADDEND, 0);
			reporter.incrCounter(Similarity.APS.COMBINED, 0);
			
			List<Vector> list = new LinkedList<Vector>();
			while (values.hasNext())
				list.add(new Vector(values.next()));
			Vector[] vectors = list.toArray(new Vector[list.size()]);
			for (int i = 0; i < vectors.length - 1; i++) {
				for (int j = i + 1; j < vectors.length; j++) {
					// length filter |y| > t / maxweight(x)
					if (compare(vectors[i].length(), Math.ceil(threshold / vectors[j].getMaxWeight())) >= 0
							&& compare(vectors[j].length(), Math.ceil(threshold / vectors[i].getMaxWeight())) >= 0) {
						// cheap upper bound dot(x,y) <= min(|x|,|y|) * maxweight(x) * maxweight(y)
						double dotProdBound = Math.min(vectors[i].length(), vectors[j].length())
								* vectors[i].getMaxWeight() * vectors[j].getMaxWeight();
						if (compare(dotProdBound, threshold) >= 0) {
							reporter.incrCounter(Similarity.APS.EVALUATED, 1);
							float similarity = (float) VectorComponentArrayWritable.dotProduct(vectors[i].getValue(),
									vectors[j].getValue());
							if (compare(similarity, threshold) >= 0) {
								reporter.incrCounter(Similarity.APS.SIMILAR, 1);
								int firstID = VectorPair.canonicalFirst(vectors[i].getId(), vectors[j].getId());
								int secondID = VectorPair.canonicalSecond(vectors[i].getId(), vectors[j].getId());
								outKey.set(firstID, secondID);
								outValue.set(similarity);
								output.collect(outKey, outValue);
							}
						}
					}
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), SimilarityV.class);

		if (args.length < 4) {
			System.err.println("Usage:\nSimilarityV <input_paths> <output_path> <threshold> <maxWi>");
			System.exit(1);
		}

		// pick the threshold from the command line
		float threshold = Float.parseFloat(args[2]);
		if (threshold < 0 || threshold >= 1) {
			System.err.println("<threshold> should be between 0 and 1");
			System.exit(1);
		}
		conf.setFloat(PARAM_APS_THRESHOLD, threshold);

		String maxWiDir = args[3];
		DistributedCache.addCacheFile(URI.create(maxWiDir), conf);

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1] + "-" + threshold + "-vernica");

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Vector.class);

		conf.setOutputKeyClass(VectorPair.class);
		conf.setOutputValueClass(FloatWritable.class);

		conf.setMapperClass(SimVMapper.class);
		conf.setReducerClass(SimVReducer.class);

		conf.set("mapred.job.name", "APS-V-" + outputPath.getName());
		conf.setNumTasksToExecutePerJvm(-1); // JVM reuse
		conf.setSpeculativeExecution(false);
		conf.setCompressMapOutput(true);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SimilarityV(), args);
		System.exit(exitCode);
	}
}
