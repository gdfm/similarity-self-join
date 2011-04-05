package aps.util;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import aps.io.VectorComponentArrayWritable;

public final class FileUtils {
	private static final Log LOG = LogFactory.getLog(FileUtils.class);

	static long fixMapFile(FileSystem fs, Path dir, Class<? extends Writable> keyClass,
			Class<? extends Writable> valueClass, boolean dryrun, Configuration conf) throws Exception {
		String dr = (dryrun ? "[DRY RUN ] " : "");
		Path data = new Path(dir, MapFile.DATA_FILE_NAME);
		Path index = new Path(dir, MapFile.INDEX_FILE_NAME);
		int indexInterval = 128;
		indexInterval = conf.getInt("io.map.index.interval", indexInterval);
		if (!fs.exists(data)) {
			// there's nothing we can do to fix this!
			throw new Exception(dr + "Missing data file in " + dir + ", impossible to fix this.");
		}
		if (fs.exists(index)) {
			// no fixing needed
			return -1;
		}
		SequenceFile.Reader dataReader = new SequenceFile.Reader(fs, data, conf);
		if (!dataReader.getKeyClass().equals(keyClass)) {
			throw new Exception(dr + "Wrong key class in " + dir + ", expected" + keyClass.getName() + ", got "
					+ dataReader.getKeyClass().getName());
		}
		if (!dataReader.getValueClass().equals(valueClass)) {
			throw new Exception(dr + "Wrong value class in " + dir + ", expected" + valueClass.getName() + ", got "
					+ dataReader.getValueClass().getName());
		}
		long cnt = 0L;
		Writable key = ReflectionUtils.newInstance(keyClass, conf);
		Writable value = ReflectionUtils.newInstance(valueClass, conf);
		SequenceFile.Writer indexWriter = null;
		if (!dryrun)
			indexWriter = SequenceFile.createWriter(fs, conf, index, keyClass, LongWritable.class);
		try {
			long pos = 0L;
			LongWritable position = new LongWritable();
			while (dataReader.next(key, value)) {
				cnt++;
				if (cnt % indexInterval == 0) {
					position.set(pos);
					if (!dryrun)
						indexWriter.append(key, position);
				}
				pos = dataReader.getPosition();
			}
		} catch (Throwable t) {
			// truncated data file. swallow it.
		}
		dataReader.close();
		if (!dryrun)
			indexWriter.close();
		return cnt;
	}

	public static boolean readMaxWiFile(JobConf conf, Map<? super Long, ? super Double> result) {
		String fname = null;
		// open the first file in the DistributedCache
		try {
			MapFile.Reader reader = getLocalMapFileReader(conf);
			LongWritable key = new LongWritable();
			DoubleWritable value = new DoubleWritable();
			while (reader.next(key, value))
				result.put(key.get(), value.get());
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean readRestFile(JobConf conf, Map<? super Integer, ? super VectorComponentArrayWritable> result) {
		String fname = null;
		// open the first file in the DistributedCache
		try {
			MapFile.Reader reader = getLocalMapFileReader(conf);
			IntWritable key = new IntWritable();
			VectorComponentArrayWritable value = new VectorComponentArrayWritable();
			while (reader.next(key, value))
				result.put(Integer.valueOf(key.get()), new VectorComponentArrayWritable(value));
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public static boolean readRestFile(JobConf conf, Map<? super Integer, ? super VectorComponentArrayWritable> result, int from, int to) {
		String fname = null;
		// open the first file in the DistributedCache
		try {
			MapFile.Reader reader = getLocalMapFileReader(conf);
			IntWritable key = new IntWritable(from);
			VectorComponentArrayWritable value = new VectorComponentArrayWritable();
			key = (IntWritable) reader.getClosest(key, value);
			LOG.info("First read key: " + key);
			result.put(Integer.valueOf(key.get()), new VectorComponentArrayWritable(value));
			while (reader.next(key, value) && key.get() < to)
				result.put(Integer.valueOf(key.get()), new VectorComponentArrayWritable(value));
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Merges the various sequence files containing the rest (pruned parts)
	 * and create an index for them
	 */
	public static void mergeRestFile(Configuration conf, Path indexPath, String dirName, int indexInterval) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path pruned = new Path(indexPath, dirName);
		FileStatus parts[] = fs.globStatus(new Path(pruned + "*"));
		List<Path> pruned_parts = new ArrayList<Path>(parts.length);
		for (FileStatus partFile : parts) {
			if (partFile.getLen() > 0)
				pruned_parts.add(partFile.getPath());
		}
		LOG.info(String.format("Found %d unindexed parts, merging %d non empty files", parts.length, pruned_parts
				.size()));
		if (pruned_parts.size() > 0) {
			SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, new IntWritable.Comparator(), IntWritable.class,
					VectorComponentArrayWritable.class, conf);
			sorter.merge(pruned_parts.toArray(new Path[pruned_parts.size()]), new Path(pruned, "data"));
			try {
				MapFile.Writer.setIndexInterval(conf, indexInterval);
				long numberOfEntries = FileUtils.fixMapFile(fs, pruned, IntWritable.class,
						VectorComponentArrayWritable.class, false, conf);
				LOG.info(String.format("Number of pruned entries: %d", numberOfEntries));
			} catch (Exception e) {
				e.printStackTrace();
			}
			LOG.info(String.format("Adding %s to the DistributedCache", pruned.getName()));
			DistributedCache.addCacheFile(pruned.toUri(), conf);
		}
		for (FileStatus p : parts) {
			fs.delete(p.getPath(), true);
		}
	}

	/**
	 * Opens the first MapFile in the DistributedCache
	 * 
	 * @param conf
	 *            configuration for the job
	 * @return the reader for the opened MapFile
	 * @throws IOException
	 */
	public static MapFile.Reader getLocalMapFileReader(Configuration conf) throws IOException {
		// open the first  in the DistributedCache
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		if (localFiles != null && localFiles.length > 0 && localFiles[0] != null) {
			String mapFileDir = localFiles[0].toString();
			FileSystem fs = FileSystem.getLocal(conf);
			return new MapFile.Reader(fs, mapFileDir, conf);
		} else {
			throw new IOException("Could not read lexicon file in DistributedCache");
		}
	}
}