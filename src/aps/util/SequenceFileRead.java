package aps.util;

import java.io.IOException;
import java.net.URI;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileRead {

	private static final int COLUMN_WIDTH = 80;

	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

			MapWritable mapValue = null;
			ArrayWritable arrayValue = null;
			Text textValue = null;
			boolean isMap = value instanceof MapWritable;
			boolean isArray = value instanceof ArrayWritable;
			boolean isText = value instanceof Text;
			String separator = (isMap || isArray) ? "\n" : "\t";
			String valueString = "";

			long position = reader.getPosition();
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";

				if (isMap) {
					mapValue = (MapWritable) value;
					StringBuilder sb = new StringBuilder();
					for (Entry<Writable, Writable> e : mapValue.entrySet()) {
						sb.append(e.getKey());
						sb.append('\t');
						sb.append(e.getValue());
						sb.append('\n');
					}
					valueString = sb.toString();
				} else if (isArray) {
					arrayValue = (ArrayWritable) value;
					StringBuilder sb = new StringBuilder();
					for (String s : arrayValue.toStrings()) {
						sb.append(s);
						sb.append('\n');
					}
					valueString = sb.toString();
				} else if (isText) {
					textValue = (Text) value;
					int length = textValue.getLength();
					if (length < COLUMN_WIDTH)
						valueString = value.toString();
					else
						valueString = "(" + length + ")";
				} else {
					valueString = value.toString();
				}

				System.out.printf("[%s%s]\t%s%s%s\n", position, syncSeen, key, separator, valueString);

				position = reader.getPosition(); // beginning of next record
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
