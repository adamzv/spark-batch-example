package org.example;

public class Town {

    private String town;
    private long count;

    public Town() {
    }

    public Town(String town, long count) {
        this.town = town;
        this.count = count;
    }

    public String getTown() {
        return town;
    }

    public void setTown(String town) {
        this.town = town;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
