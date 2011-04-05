package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IndexItem implements WritableComparable<IndexItem> {

    private int vectorID;
    private double weight;
    private boolean flagged;
    private long maxIndexed;
    private double vectorMaxWeight;
    private int vectorLength;
    private double vectorSum;
    private double positionalMaxWeight;
    private double positionalSum;

    public IndexItem() {
    }

    public IndexItem(IndexItem other) {
        set(other);
    }

    public int getID() {
        return vectorID;
    }

    public double getWeight() {
        return weight;
    }

    public void flag() {
        this.flagged = true;
    }

    public boolean isFlagged() {
        return this.flagged;
    }

    public void setMaxIndexed(long maxIndexed) {
        this.maxIndexed = maxIndexed;
    }

    public long getMaxIndexed() {
        return maxIndexed;
    }

    public void setVectorMaxWeight(double vectorMaxWeight) {
        this.vectorMaxWeight = vectorMaxWeight;
    }

    public double vectorMaxWeight() {
        return vectorMaxWeight;
    }

    public void setVectorLength(int vectorLength) {
        this.vectorLength = vectorLength;
    }

    public int vectorLength() {
        return vectorLength;
    }

    public void setVectorSum(double vectorSum) {
        this.vectorSum = vectorSum;
    }

    public double vectorSum() {
        return vectorSum;
    }

    public double positionalMaxWeight() {
        return positionalMaxWeight;
    }

    public void setPositionalMaxWeight(double positionalMaxWeight) {
        this.positionalMaxWeight = positionalMaxWeight;
    }

    public double positionalSum() {
        return positionalSum;
    }

    public void setPositionalSum(double positionalSum) {
        this.positionalSum = positionalSum;
    }

    public void set(int vectorID, double weight) {
        set(vectorID, weight, false, 0L, 0F, 0, 0, 0, 0);
    }

    public void set(IndexItem item) {
        set(item.vectorID, item.weight, item.flagged, item.maxIndexed, item.vectorMaxWeight, item.vectorLength,
                item.vectorSum, item.positionalMaxWeight, item.positionalSum);
    }

    private void set(int vectorID, double weight, boolean flagged, long maxIndexed, double maxWeight, int length,
            double vectorSum, double posMaxWeight, double posSum) {
        this.vectorID = vectorID;
        this.weight = weight;
        this.flagged = flagged;
        this.maxIndexed = maxIndexed;
        this.vectorMaxWeight = maxWeight;
        this.vectorLength = length;
        this.vectorSum = vectorSum;
        this.positionalMaxWeight = posMaxWeight;
        this.positionalSum = posSum;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vectorID = in.readInt();
        weight = in.readDouble();
        flagged = in.readBoolean();
        maxIndexed = in.readLong();
        vectorMaxWeight = in.readDouble();
        vectorLength = in.readInt();
        vectorSum = in.readDouble();
        positionalMaxWeight = in.readDouble();
        positionalSum = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(vectorID);
        out.writeDouble(weight);
        out.writeBoolean(flagged);
        out.writeLong(maxIndexed);
        out.writeDouble(vectorMaxWeight);
        out.writeInt(vectorLength);
        out.writeDouble(vectorSum);
        out.writeDouble(positionalMaxWeight);
        out.writeDouble(positionalSum);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (o instanceof IndexItem) {
            IndexItem ii = (IndexItem) o;
            return this.vectorID == ii.vectorID;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + vectorID;
        return result;
    }

    @Override
    public String toString() {
        return vectorID + "\t " + weight + (flagged ? "\t* " : "\t  ")
                + ((maxIndexed == 0) ? "" : "[" + maxIndexed + "]");
    }

    @Override
    public int compareTo(IndexItem other) {
        return this.vectorID < other.vectorID ? -1 : this.vectorID > other.vectorID ? 1 : 0;
    }

    /* Static Methods */
    public static int getLeastPrunedVectorID(IndexItem firstItem, IndexItem secondItem) {
        int result = firstItem.getMaxIndexed() > secondItem.getMaxIndexed() ? firstItem.getID() : secondItem.getID();
        return result;
    }

    public static int getMostPrunedVectorID(IndexItem firstItem, IndexItem secondItem) {
        int result = firstItem.getMaxIndexed() > secondItem.getMaxIndexed() ? secondItem.getID() : firstItem.getID();
        return result;
    }
}