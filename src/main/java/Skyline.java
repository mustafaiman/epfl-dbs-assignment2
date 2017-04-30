import java.util.*;

public class Skyline {
    public static ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions) {
        if (partitions.size() == 1) {
            return nlSkyline(partitions.get(0));
        } else {
            ArrayList<ArrayList<Tuple>> other = new ArrayList<>();
            int middleIndex = (partitions.size() - 1) / 2;
            for (int i = partitions.size() - 1; i > middleIndex; i--) {
                other.add(partitions.get(i));
                partitions.remove(i);
            }
            ArrayList<Tuple> res1 = mergePartitions(partitions);
            ArrayList<Tuple> res2 = mergePartitions(other);
            res1.addAll(res2);
            return nlSkyline(res1);
        }
    }


    public static ArrayList<Tuple> dcSkyline(ArrayList<Tuple> inputList, int blockSize) {
        int numBlocks = inputList.size() / blockSize;
        if (inputList.size() % blockSize > 0)
            numBlocks++;
        ArrayList<ArrayList<Tuple>> allTuples = new ArrayList<>(numBlocks);
        int k = 0;
        while (!inputList.isEmpty()) {
            allTuples.add(new ArrayList<>(blockSize));
            for (int i = 0; i < blockSize; i++) {
                if (inputList.isEmpty())
                    break;
                allTuples.get(k).add(inputList.get(inputList.size() - 1));
                inputList.remove(inputList.size() - 1);
            }
            k++;
        }

        return mergePartitions(allTuples);

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