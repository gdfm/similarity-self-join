package aps.io;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class GenericValue extends GenericWritable {

	@SuppressWarnings("unchecked")
	private static Class[] CLASSES = { VectorComponentArrayWritable.class, HalfPair.class, VectorPair.class};

	public GenericValue() {}

	@Override
	@SuppressWarnings("unchecked")
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}
}
