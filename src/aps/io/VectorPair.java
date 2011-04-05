package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class VectorPair implements WritableComparable<VectorPair> {
    /**
     * Minimum ID for vectors
     */
    public static final int MIN_ID = 1;

    private int first;
    private int second;

    /**
     * Sets relevant fields for a VectorPair. Invariant: first VectorID is always greater than second VectorID
     * 
     * @param first
     *            first VectorID
     * @param second
     *            second VectorID
     */
    public void set(int first, int second) {
        this.first = first;
        this.second = second;
        checkInvariants();
    }

    /**
     * Sets relevant fields for a VectorPair from another VectorPair (copy constructor).
     * 
     * @param other
     *            VectorPair to copy
     */
    public void set(VectorPair other) {
        set(other.first, other.second);
    }

    private void checkInvariants() {
        assert (first >= MIN_ID && second >= MIN_ID && first > second);
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
        checkInvariants();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public int compareTo(VectorPair other) {
        int result = (this.first == other.first ? 0 : (this.first < other.first ? -1 : 1));
        if (result == 0)
            result = (this.second == other.second ? 0 : (this.second < other.second ? -1 : 1));
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (o instanceof VectorPair) {
            VectorPair vp = (VectorPair) o;
            return first == vp.first && second == vp.second;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + first;
        result = 31 * result + second;
        return result;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    /**
     * Returns the vector ID that comes first in the canonical ordering between the two passed as parameters
     * 
     * @param id1
     *            a vector ID
     * @param id2
     *            another vector ID
     * @return the vector ID that comes first
     */
    public static int canonicalFirst(int id1, int id2) {
        return Math.max(id1, id2);
    }

    /**
     * Returns the vector ID that comes last in the canonical ordering between the two passed as parameters
     * 
     * @param id1
     *            a vector ID
     * @param id2
     *            another vector ID
     * @return the vector ID that comes last
     */
    public static int canonicalSecond(int id1, int id2) {
        return Math.min(id1, id2);
    }

    /**
     * A binary comparator optimised for VectorPairs
     */
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(VectorPair.class);
        }

        @Override
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

        static { // register this comparator
            WritableComparator.define(VectorPair.class, new Comparator());
        }
    }
}
