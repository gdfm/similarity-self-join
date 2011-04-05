package aps.io;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import aps.Similarity;

public class TestGenericKey {
	private static final int TIMES = (int) 1e7;

	public static class IntComparator extends WritableComparator {

		public IntComparator() {
			super(GenericKey.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int firstValue = readInt(b1, s1);
			int secondValue = readInt(b2, s2);
			int result = (firstValue == secondValue ? 0 : (firstValue < secondValue ? -1 : 1));
			if (result == 0) {
				firstValue = readInt(b1, s1 + Integer.SIZE / 8);
				secondValue = readInt(b2, s2 + Integer.SIZE / 8);
				result = (firstValue == secondValue ? 0 : (firstValue < secondValue ? -1 : 1));
			}
			return result;
		}
	}

	public static class BytesComparator extends WritableComparator {

		public BytesComparator() {
			super(GenericKey.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	@Test
	public void testComparator() throws IOException {
		GenericKey k1 = new GenericKey(1111111, 1);
		GenericKey k2 = new GenericKey(1111111, 255);

		GenericKey.Comparator comp = new GenericKey.Comparator();
		GenericKey.PrimaryComparator compPri = new GenericKey.PrimaryComparator();

		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		DataOutputStream output = new DataOutputStream(buffer);

		k1.write(output);
		k2.write(output);
		byte[] bytes = buffer.toByteArray();
		//System.out.println(Arrays.toString(bytes));

		assertTrue(comp.compare(bytes, 0, bytes.length / 2, bytes, bytes.length / 2, bytes.length / 2) < 0);
		assertTrue(compPri.compare(bytes, 0, bytes.length / 2, bytes, bytes.length / 2, bytes.length / 2) == 0);
		assertTrue(compPri.compare(bytes, bytes.length / 2 + Integer.SIZE / 8, 0, bytes, 0 + Integer.SIZE / 8, 0) > 0);
	}

	//@Test
	public void testBytesComparatorSpeed() throws IOException {
		testComparatorSpeed(new BytesComparator(), TIMES);
	}

	//@Test
	public void testIntComparatorSpeed() throws IOException {
		testComparatorSpeed(new IntComparator(), TIMES);
	}

	//@Test
	public void testKeyComparatorSpeed() throws IOException {
		testComparatorSpeed(new GenericKey.Comparator(), TIMES);
	}

	@Test
	public void testPartitioner() {
		int numPartitions = 28;
		GenericKey key = new GenericKey(12, 17024);
		GenericKey.StripePartitioner partitioner = new GenericKey.StripePartitioner();
		JobConf conf = new JobConf();
		conf.setInt(Similarity.PARAM_APS_MAXKEY, 17024);
		conf.setInt(Similarity.PARAM_APS_STRIPES, 7);
		partitioner.configure(conf);
		int n = partitioner.getPartition(key, new GenericValue(), numPartitions);
		assertEquals(22, n);
	}
	
	private void testComparatorSpeed(WritableComparator comp, int times) throws IOException {
		GenericKey k1 = new GenericKey();
		GenericKey k2 = new GenericKey();
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		DataOutputStream output = new DataOutputStream(buffer);
		Random rand = new Random(123456789);

		for (int i = 0; i < times; i++) {
			k1.set(rand.nextInt(Integer.MAX_VALUE), rand.nextInt(Integer.MAX_VALUE));
			k2.set(rand.nextInt(Integer.MAX_VALUE), rand.nextInt(Integer.MAX_VALUE));
			k1.write(output);
			k2.write(output);
			byte[] bytes = buffer.toByteArray();
			int res = comp.compare(bytes, 0, bytes.length / 2, bytes, bytes.length / 2, bytes.length / 2);
			assertEquals(Math.signum(k1.compareTo(k2)), Math.signum(res), 0);
			buffer.reset();
		}
	}

	// FIXME update tests
	@Test
	public void testFindSplit() {
		int spread = 1;
		assertEquals(0, GenericKey.StripePartitioner.findStripe(0,spread));
		assertEquals(2, GenericKey.StripePartitioner.findStripe(3,spread));
		assertEquals(3, GenericKey.StripePartitioner.findStripe(6,spread));
		assertEquals(3, GenericKey.StripePartitioner.findStripe(9,spread));
		assertEquals(4, GenericKey.StripePartitioner.findStripe(10,spread));
		assertEquals(13, GenericKey.StripePartitioner.findStripe(100,spread));
	}
	
	
	// FIXME update tests
	@Test
	public void testMinKeyInSplit() {
		int splits = 10;
		int maxKey = 1024;
		assertEquals(1, GenericKey.StripePartitioner.minKeyInStripe(0, splits, maxKey));
		assertEquals(0, GenericKey.StripePartitioner.findStripe(102, maxKey, splits));
		assertEquals(1, GenericKey.StripePartitioner.findStripe(104, maxKey, splits));
		assertEquals(104, GenericKey.StripePartitioner.minKeyInStripe(1, splits, maxKey));
		assertEquals(4, GenericKey.StripePartitioner.findStripe(500, maxKey, splits));
		assertEquals(413, GenericKey.StripePartitioner.minKeyInStripe(4, splits, maxKey));
		assertEquals(9, GenericKey.StripePartitioner.findStripe(1024, maxKey, splits));
		assertEquals(928, GenericKey.StripePartitioner.minKeyInStripe(9, splits, maxKey));
		// is this correct?
		assertEquals(9, GenericKey.StripePartitioner.findStripe(1025, maxKey, splits));
		assertEquals(1031, GenericKey.StripePartitioner.minKeyInStripe(10, splits, maxKey));
	}
}
