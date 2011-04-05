package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * An item in the lexicon. Each item has a unique identifier and a count of how many times it occurs in the corpus.
 */
public class LexiconItem implements WritableComparable<LexiconItem> {

    private long id;
    private long count;

    public LexiconItem() {
    }

    public long getId() {
        return id;
    }

    public long getCount() {
        return count;
    }

    public void set(LongWritable id, LongWritable count) {
        set(id.get(), count.get());
    }

    public void set(long id, long count) {
        this.id = id;
        this.count = count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        count = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(count);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LexiconItem) {
            LexiconItem li = (LexiconItem) o;
            return id == li.id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return id + "\t" + count;
    }

    /**
     * The natural order for LexiconItems is decreasing in order of number of occurrences (highest number of occurrences
     * sorts first)
     */
    @Override
    public int compareTo(LexiconItem li) {
        // -1 * count.compareTo(li.count);
        return count == li.count ? 0 : count > li.count ? -1 : 1;
    }
}
