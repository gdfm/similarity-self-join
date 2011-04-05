package aps.io;
import org.apache.hadoop.io.ArrayWritable;

public class IndexItemArrayWritable extends ArrayWritable {
	public IndexItemArrayWritable() {
		super(IndexItem.class);
	}

	public IndexItemArrayWritable(IndexItem[] values) {
		super(IndexItem.class, values);
	}

	public IndexItem[] toIndexItemArray() {
		return (IndexItem[]) toArray();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('\n');
		for (String s : this.toStrings()) {
			sb.append(s);
			sb.append('\n');
		}
		return sb.toString();
	}
}