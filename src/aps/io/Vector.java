package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A sparse vector in a multidimensional euclidean space.
 * The vector is identified by an ID. The value of the vector is a {@link VectorComponentArrayWritable}. 
 */
public class Vector implements Writable {
    private int id;
    private VectorComponentArrayWritable value;

    public Vector() {
        value = new VectorComponentArrayWritable();
    }

    /**
     * Copy constructor. The values are deep copied.
     * @param other
     */
    public Vector(Vector other) {
        this.id = other.id;
        this.value = new VectorComponentArrayWritable(other.value);
    }

    public int getId() {
        return id;
    }

    public void set(int id, VectorComponentArrayWritable value) {
        this.id = id;
        this.value = value;
    }

    public VectorComponentArrayWritable getValue() {
        return value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        value.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        value.write(out);
    }

    @Override
    public String toString() {
        return id + "\n" + value;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (o instanceof Vector) {
            Vector v = (Vector) o;
            return this.id == v.id;
        }
        return false;
    }

    public int length() {
        return value.length();
    }

    public double getMaxWeight() {
        return value.maxWeight();
    }

    public static long findSmallestCommonTermID(Vector v1, Vector v2) {
        VectorComponent vca1[] = v1.getValue().toVectorComponentArray();
        VectorComponent vca2[] = v2.getValue().toVectorComponentArray();
        // Arrays.sort(vca1);
        // Arrays.sort(vca2);
        // empty vectors
        if (vca1.length == 0 || vca2.length == 0)
            return -1;
        int i = vca1.length - 1, j = vca2.length - 1;
        while (i >= 0 && j >= 0) {
            // FIXME Works only if the dimensions inside the vector are ordered descending
            if (vca1[i].getID() > vca2[j].getID()) {
                j--;
            } else if (vca1[i].getID() < vca2[j].getID()) {
                i--;
            } else if (vca1[i].getID() == vca2[j].getID()) {
                return vca1[i].getID();
            }
        }
        return -1;
    }
}