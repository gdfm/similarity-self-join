package aps.io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import aps.Similarity;

// FIXME Merge with VectorPair
public class GenericKey implements WritableComparable<GenericKey> {
	private int primary;
	private int secondary;

	public static class PrimaryPartitioner implements Partitioner<GenericKey, GenericValue> {
		@Override
		public int getPartition(GenericKey key, GenericValue value, int numPartitions) {
			return ((31 * key.primary) & Integer.MAX_VALUE) % numPartitions;
		}

		@Override
		public void configure(JobConf job) {
		}
	}

	public static class StripePartitioner implements Partitioner<GenericKey, GenericValue> {

		private int maxKey;
		private int nstripes;
		private int reducersPerStripe;

		//		@Override
		//		public int getPartition(GenericKey key, GenericValue value, int numPartitions) {
		//			int split = findSplit(key.secondary, maxKey, nsplits);
		//			int nr = numReducerInSplit(split);
		//			// discard secondary key in partitioning
		//			int hashKey = 31 * key.primary;
		//			// bitwise AND used to get a positive number
		//			hashKey &= Integer.MAX_VALUE;
		//			int result = (hashKey % nr) + minReducerInSplit(split);
		//			assert (result >= 0 && result < numPartitions);
		//			return result;
		//		}

		@Override
		public int getPartition(GenericKey key, GenericValue value, int numPartitions) {
			int stripe = findStripe(key.secondary, maxKey, nstripes);
			int nr = numReducerInStripe(stripe, reducersPerStripe);
			// discard secondary key in partitioning
			int hashKey = 31 * key.primary;
			// bitwise AND used to get a positive number
			hashKey &= Integer.MAX_VALUE;
			int result = (hashKey % nr) + minReducerInStripe(stripe, reducersPerStripe);
			assert (result >= 0 && result < numPartitions);
			return result;
		}

		//		static int findSplit(int secondaryKey, int maxKey, int splits) {
		//			// TODO implement intelligent rest spreading, right now the last split is heavily underloaded
		//			if (secondaryKey > 0) {
		//				int keysPerSplit = maxKey / splits;
		//				// this solves the leftover problem
		//				if (maxKey % splits > 0)
		//					keysPerSplit++;
		//				// secondaryKey - 1 puts the keys in base 0
		//				return (secondaryKey - 1) / keysPerSplit;
		//			} else
		//				return -1 * secondaryKey;
		//		}

		static int findStripe(int secondaryKey, int maxKey, int numStripes) {
			if (secondaryKey > 0) {
				int keysPerStripe = maxKey / numStripes;
				// this addresses the leftover issue
				if (maxKey % numStripes > 0)
					keysPerStripe++;
				// (secondaryKey - 1) puts the keys in base 0
				return (secondaryKey - 1) / keysPerStripe;
			} else
				return -1 * secondaryKey;
		}

		//		static int numReducerInSplit(int split) {
		//			return split + 1;
		//		}

		static int numReducerInStripe(int stripe, int spread) {
			return spread;
		}

		//		static int minReducerInSplit(int split) {
		//			return (split * (split + 1)) / 2;
		//		}

		static int minReducerInStripe(int stripe, int spread) {
			return stripe * spread;
		}

		//		static int minKeyInSplit(int split, int numSplits, int maxKey) {
		//			int keysPerSplit = numKeysInSplit(split, numSplits, maxKey);
		//			int result = split * keysPerSplit + 1;
		//			return result;
		//		}

		public static int minKeyInStripe(int stripe, int numStripes, int maxKey) {
			int keysPerStripe = numKeysInStripe(stripe, numStripes, maxKey);
			return stripe * keysPerStripe + 1;
		}

		//		static int numKeysInSplit(int split, int numSplits, int maxKey) {
		//			int keysPerSplit = maxKey / numSplits;
		//			if (maxKey % numSplits > 0)
		//				keysPerSplit++;
		//			return keysPerSplit;
		//		}

		public static int numKeysInStripe(int stripe, int numStripes, int maxKey) {
			int keysPerStripe = maxKey / numStripes;
			if (maxKey % numStripes > 0)
				keysPerStripe++;
			return keysPerStripe;
		}

		//		static int findSplit(int reducerID) {
		//			// solution for second order equation k^2 + k - 2n = 0 
		//			int result = (int) ((-1 + Math.sqrt(1+8*reducerID))/2);
		//			return result;
		//		}

		public static int findStripe(int reducerID, int spread) {
			return reducerID / spread;
		}

		//		static int numReducers(int numSplits) {
		//			return numSplits * (numSplits+ 1) / 2;
		//		}

		public static int numReducers(int numStripes, int spread) {
			return numStripes * spread;
		}

		@Override
		public void configure(JobConf conf) {
			maxKey = conf.getInt(Similarity.PARAM_APS_MAXKEY, 0);
			nstripes = conf.getInt(Similarity.PARAM_APS_STRIPES, 1);
			reducersPerStripe = conf.getInt(Similarity.PARAM_APS_REDUCER_PER_STRIPE, 1);
			if (maxKey == 0)
				throw new RuntimeException("Max key value not set");
		}
	}

	public static class PrimaryComparator extends WritableComparator {

		public PrimaryComparator() {
			super(GenericKey.class);
		}

		@SuppressWarnings("unchecked")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			GenericKey kv1 = (GenericKey) w1;
			GenericKey kv2 = (GenericKey) w2;
			int result = (kv1.primary < kv2.primary ? -1 : (kv1.primary == kv2.primary ? 0 : 1));
			return result;
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int firstValue = readInt(b1, s1);
			int secondValue = readInt(b2, s2);
			int result = (firstValue < secondValue ? -1 : (firstValue == secondValue ? 0 : 1));
			return result;
		}
	}

	public static class Comparator extends WritableComparator {

		public Comparator() {
			super(GenericKey.class);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			GenericKey kv1 = (GenericKey) w1;
			GenericKey kv2 = (GenericKey) w2;
			return kv1.compareTo(kv2);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int firstValue = readInt(b1, s1);
			int secondValue = readInt(b2, s2);
			int result = (firstValue < secondValue ? -1 : (firstValue == secondValue ? 0 : 1));
			if (result == 0) {
				firstValue = readInt(b1, s1 + Integer.SIZE / 8);
				secondValue = readInt(b2, s2 + Integer.SIZE / 8);
				result = (firstValue < secondValue ? -1 : (firstValue == secondValue ? 0 : 1));
			}
			return result;
		}
	}

	public GenericKey() {
	}

	public GenericKey(int primary, int secondary) {
		set(primary, secondary);
	}

	public int getPrimary() {
		return primary;
	}

	public int getSecondary() {
		return secondary;
	}

	public void set(int primary, int secondary) {
		this.primary = primary;
		this.secondary = secondary;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		primary = in.readInt();
		secondary = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(primary);
		out.writeInt(secondary);
	}

	@Override
	public int compareTo(GenericKey other) {
		int result = (this.primary < other.primary ? -1 : (this.primary == other.primary ? 0 : 1));
		if (result == 0)
			result = (this.secondary < other.secondary ? -1 : (this.secondary == other.secondary ? 0 : 1));
		return result;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof GenericKey) {
			GenericKey k = (GenericKey) other;
			return (this.compareTo(k) == 0);
		}
		return false;
	}

	@Override
	public String toString() {
		return primary + "\t" + secondary;
	}
}
