package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class VectorComponentArrayWritable extends ArrayWritable {

    @Deprecated
    private long maxIndexed;
    private double maxWeight;

    public VectorComponentArrayWritable() {
        super(VectorComponent.class);
    }

    public VectorComponentArrayWritable(VectorComponent[] values) {
        super(VectorComponent.class, values);
        maxWeight = maxWeight(values);
    }

    /**
     * Copy constructor. The values are deep copied.
     * 
     * @param copy
     */
    public VectorComponentArrayWritable(VectorComponentArrayWritable copy) {
        super(VectorComponent.class, Arrays.copyOf(copy.get(), copy.get().length, VectorComponent[].class));
        this.maxIndexed = copy.maxIndexed;
        this.maxWeight = copy.maxWeight;
    }

    @Override
    public void set(Writable[] values) {
        assert (values instanceof VectorComponent[]);
        maxWeight = maxWeight((VectorComponent[]) values);
        super.set(values);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        maxIndexed = in.readLong();
        maxWeight = in.readDouble();
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(maxIndexed);
        out.writeDouble(maxWeight);
        super.write(out);
    }

    public VectorComponent[] toVectorComponentArray() {
        return (VectorComponent[]) toArray();
    }

    @Deprecated
    public long getMaxIndexed() {
        return maxIndexed;
    }

    @Deprecated
    public void setMaxIndexed(long maxIndexed) {
        this.maxIndexed = maxIndexed;
    }

    /**
     * computes the maximum weight in the vector
     * 
     * @return the maximum weight
     */
    public double maxWeight() {
        return maxWeight;
    }

    /**
     * returns the length of the vector
     * 
     * @return the length
     */
    public int length() {
        return this.get().length;
    }

    /**
     * computes the sum of the weights in the vector
     * 
     * @return the sum
     */
    public double sumWeights() {
        double sum = 0;
        for (VectorComponent vc : this.toVectorComponentArray())
            sum += vc.getWeight();
        return sum;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (maxIndexed > 0) {
            sb.append('\t');
            sb.append('[');
            sb.append(maxIndexed);
            sb.append(']');
        }
        if (maxWeight > 0) {
            sb.append("\tmax=");
            sb.append(maxWeight);
        }
        sb.append('\n');
        for (String s : this.toStrings()) {
            sb.append(s);
            sb.append('\n');
        }
        return sb.toString();
    }

    /**
     * Computes the dot product of two sparse vectors. The dimensions are expected to be sorted descending.
     * 
     * @param vca1
     *            first vector
     * @param vca2
     *            second vector
     * @param minDim
     *            multiply from max down to this dimension, not included
     * @return the dot product of the vectors as a double
     */
    public static double dotProduct(VectorComponent[] vca1, VectorComponent[] vca2, long minDim) {
        // I assume the arrays are already correctly sorted
        // Arrays.sort(vca1);
        // Arrays.sort(vca2);
        // empty vectors
        if (vca1.length == 0 || vca2.length == 0)
            return 0;
        int i = 0, j = 0;
        double res = 0;
        while (i < vca1.length && j < vca2.length && vca1[i].getID() > minDim && vca2[j].getID() > minDim) {
            if (vca1[i].getID() > vca2[j].getID()) {
                i++;
            } else if (vca1[i].getID() < vca2[j].getID()) {
                j++;
            } else if (vca1[i].getID() == vca2[j].getID()) {
                res += vca1[i].getWeight() * vca2[j].getWeight();
                i++;
                j++;
            }
        }
        return res;
    }

    /**
     * Compute the dot product of two vector component arrays
     * 
     * @param vca1
     *            a vector component array
     * @param vca2
     *            another vector component array
     * @return the dot product
     */
    public static double dotProduct(VectorComponent[] vca1, VectorComponent[] vca2) {
        return dotProduct(vca1, vca2, 0);
    }

    /**
     * Compute the dot product of two sparse vectors
     * 
     * @param vcaw1
     *            a vector
     * @param vcaw2
     *            another vector
     * @return the dot product
     */
    public static double dotProduct(VectorComponentArrayWritable vcaw1, VectorComponentArrayWritable vcaw2) {
        return dotProduct(vcaw1.toVectorComponentArray(), vcaw2.toVectorComponentArray(), 0);
    }

    /**
     * Computes the dot product of two sparse vectors. The dimensions are expected to be sorted descending.
     * 
     * @param vcaw1
     *            first vector
     * @param vcaw2
     *            second vector
     * @param minDim
     *            multiply from max down to this dimension, not included
     * @return the dot product of the vectors as a double
     */
    public static double dotProduct(VectorComponentArrayWritable vcaw1, VectorComponentArrayWritable vcaw2, long minDim) {
        return dotProduct(vcaw1.toVectorComponentArray(), vcaw2.toVectorComponentArray(), minDim);
    }

    /**
     * Compute the component-wise product of two vectors. The resulting vector will have the same dimensions of the
     * input vectors, and the product of their weights as the new weight. BEWARE: right now the method assumes that the
     * vectors have the same dimensions
     * 
     * @param vca1
     *            a vector
     * @param vca2
     *            another vector
     * @return the component-wise product of two vectors
     */
    public static VectorComponent[] componentWiseProduct(VectorComponent[] vca1, VectorComponent[] vca2) {
        int resultSize = Math.min(vca1.length, vca2.length);
        VectorComponent[] res = new VectorComponent[resultSize];
        for (int i = 0; i < res.length; i++) {
            // we assume that the vectors have the same dimensions
            long l = vca1[i].getID();
            assert l == vca2[i].getID(); // TODO generalize to arbitrary vectors
            double w = vca1[i].getWeight() * vca2[i].getWeight();
            res[i] = new VectorComponent(l, w);
        }
        return res;
    }

    /**
     * Computes the maximum weight in a vector
     * 
     * @param vcarray
     *            the vector
     * @return the maximum weight
     */
    public static double maxWeight(VectorComponent[] vcarray) {
        Comparator<VectorComponent> comp = new Comparator<VectorComponent>() {
            @Override
            public int compare(VectorComponent vc1, VectorComponent vc2) {
                return Double.compare(vc1.getWeight(), vc2.getWeight());
            }
        };
        return Collections.max(Arrays.asList(vcarray), comp).getWeight();
    }

    /**
     * Normalises a vector in place. This method modifies the input vector weights so that they are unit normalised. No
     * memory is allocated in the process.
     * 
     * @param vector
     *            the input vector
     * @return the normalised input vector
     */
    public static VectorComponent[] normaliseInPlace(VectorComponent[] vector) {
        // compute magnitude
        double magnitude = 0;
        for (VectorComponent vc : vector)
            magnitude += Math.pow(vc.getWeight(), 2);
        magnitude = Math.sqrt(magnitude);
        // normalise
        for (VectorComponent vc : vector)
            vc.setWeight(vc.getWeight() / magnitude);
        // sort dimensions (decreasing id-frequency)
        Arrays.sort(vector);
        return vector;
    }
}
