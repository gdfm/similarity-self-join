package aps.io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class VectorComponent implements WritableComparable<VectorComponent> {
	private long id;
	private double weight;

	public VectorComponent() {
	}

	public VectorComponent(long id, double weight) {
		this.id = id;
		this.weight = weight;
	}

	public long getID() {
		return id;
	}

	public double getWeight() {
		return weight;
	}
	
	public void setWeight(double weight) {
	    this.weight = weight;
	}

	/**
	 * Defines the natural ordering for VectorComponents. VectorComponents sort
	 * descending, from most frequent to least frequent. This method is
	 * inconsistent with equals because it does not take the weight into
	 * consideration.
	 */
	@Override
	public int compareTo(VectorComponent other) {
		// sorts descending, inconsistent with equals
		return this.id == other.id ? 0 : this.id < other.id ? 1 : -1;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readLong();
		weight = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeDouble(weight);
	}

	@Override
	public String toString() {
		return id + "\t" + weight;
	}

	@Override
	public int hashCode() {
		int result = 17;
		result = 31 * result + (int) (id ^ (id >>> 32));
		long dbits = Double.doubleToLongBits(weight);
		result = 31 * result + (int) (dbits ^ (dbits >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (o instanceof VectorComponent) {
			VectorComponent vc = (VectorComponent) o;
			return (this.id == vc.id && Double.compare(this.weight, vc.weight) == 0);
		}
		return false;
	}
}