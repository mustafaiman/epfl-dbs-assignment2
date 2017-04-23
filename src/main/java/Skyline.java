import java.util.*;

public class Skyline {
    //a partition that appeared later in partitions array cannot dominate an item from an earlier partition.
    public static ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions) {
        ArrayList<Tuple> result = partitions.get(0);
        for (int i = 1; i < partitions.size(); i++) {
            for (int j = partitions.get(i).size() - 1; j >= 0; j--) {
                Tuple item = partitions.get(i).get(j);
                for (Tuple inlist: result) {
                    if (inlist.dominates(item)) {
                        partitions.get(i).remove(j);
                    }
                }
            }
            result.addAll(partitions.get(i));
        }
        return result;
    }


    public static ArrayList<Tuple> dcSkyline(ArrayList<Tuple> inputList, int blockSize) {
        return null;
    }

    public static ArrayList<Tuple> nlSkyline(ArrayList<Tuple> partition) {
        ArrayList<Tuple> result = new ArrayList<>();

        Tuple candidate = null;
        while (partition.size() > 0) {
            if (candidate == null) {
                candidate = partition.get(partition.size() - 1);
                partition.remove(partition.size() - 1);
            }
            boolean survived = true;
            for (int i = partition.size() - 1; i >=0; i--) {
                Tuple item = partition.get(i);
                if (item == candidate)
                    continue;
                if (item.dominates(candidate)) {
                    candidate = item;
                    partition.remove(i);
                    survived = false;
                    break;
                } else if (candidate.dominates(item)) {
                    partition.remove(i);
                }
            }
            if (survived) {
                result.add(candidate);
                candidate = null;
            }
        }

        return result;
    }

}

class Tuple {
    private int price;
    private int age;

    public Tuple(int price, int age) {
        this.price = price;
        this.age = age;
    }

    public boolean dominates(Tuple other) {
        return this.price <= other.price && this.age <= other.age;
    }

    public boolean isIncomparable(Tuple other) {
        return !this.dominates(other) && !other.dominates(this);
    }

    public int getPrice() {
        return price;
    }

    public int getAge() {
        return age;
    }

    public String toString() {
        return price + "," + age;
    }

    public boolean equals(Object o) {
        if (o instanceof Tuple) {
            Tuple t = (Tuple) o;
            return this.price == t.price && this.age == t.age;
        } else {
            return false;
        }
    }
}