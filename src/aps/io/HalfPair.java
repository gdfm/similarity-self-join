package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class HalfPair implements Writable {
    private int id;
    private float similarity;

    public int getID() {
        return id;
    }

    public float getSimilarity() {
        return similarity;
    }

    public void set(int id, float similarity) {
        this.id = id;
        this.similarity = similarity;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        similarity = in.readFloat();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeFloat(similarity);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (o instanceof HalfPair) {
            HalfPair hp = (HalfPair) o;
            return id == hp.id;
        }
        return false;
    }

    @Override
    public String toString() {
        return id + "\t" + similarity;
    }
}
