package aps.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

class WholeFileRecordReader implements RecordReader<Text, Text> {

	private FileSplit fileSplit;
	private Configuration conf;
	private boolean processed = false;

	public WholeFileRecordReader(FileSplit fileSplit, Configuration conf) throws IOException {
		this.fileSplit = fileSplit;
		this.conf = conf;
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return processed ? fileSplit.getLength() : 0;
	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public boolean next(Text key, Text value) throws IOException {
		if (!processed) {
			Path file = fileSplit.getPath();
			// System.err.println(file);
			FileSystem fs = file.getFileSystem(conf);
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = factory.getCodec(file);
			InputStream in = null;
			try {
				if (codec != null)
					in = codec.createInputStream(fs.open(file));
				else
					in = fs.open(file);
				ByteArrayOutputStream baos = new ByteArrayOutputStream((int) fileSplit.getLength());
				IOUtils.copyBytes(in, baos, conf);
				String contents = baos.toString();
				String name = file.getName();
				key.set(name);
				value.set(contents);

			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}
}
