package aps.io;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;

import aps.io.VectorPair;

public class TestVectorPair {

	private static final int TIMES = (int) 1e6;

	@Test
	public void testComparator() throws IOException {
		VectorPair vp1 = new VectorPair();
		VectorPair vp2 = new VectorPair();
		VectorPair.Comparator comp = new VectorPair.Comparator();
		ByteArrayOutputStream buffer1 = new ByteArrayOutputStream();
		DataOutputStream output1 = new DataOutputStream(buffer1);
		ByteArrayOutputStream buffer2 = new ByteArrayOutputStream();
		DataOutputStream output2 = new DataOutputStream(buffer2);
		Random rand = new Random(123456789);

		for (int i = 0; i < TIMES; i++) {
			vp1.set(rand.nextInt(Integer.MAX_VALUE), rand.nextInt(Integer.MAX_VALUE));
			vp2.set(rand.nextInt(Integer.MAX_VALUE), rand.nextInt(Integer.MAX_VALUE));
			vp1.write(output1);
			vp2.write(output2);
			byte[] bytes1 = buffer1.toByteArray();
			byte[] bytes2 = buffer2.toByteArray();
			int l1 = WritableUtils.decodeVIntSize(bytes1[0]);
			l1 += WritableUtils.decodeVIntSize(bytes1[l1]);
			int l2 = WritableUtils.decodeVIntSize(bytes2[0]);
			l2 += WritableUtils.decodeVIntSize(bytes2[l2]);
			int res = comp.compare(bytes1, 0, l1, bytes2, 0, l2);
			assertEquals(vp1 + "\t-\t" + vp2 + "\n" + Arrays.toString(bytes1) + "\n l1=" + l1 + " l2=" + l2, Math
					.signum(vp1.compareTo(vp2)), Math.signum(res), 0);
			buffer1.reset();
			buffer2.reset();
		}
	}

}
