package aps.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileRead {

	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		MapFile.Reader reader = null;
		try {
			reader = new MapFile.Reader(fs, uri, conf);
			WritableComparable<?> key = (WritableComparable<?>) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while (reader.next(key, value))
				System.out.printf("%s\t%s\n", key, value);
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
