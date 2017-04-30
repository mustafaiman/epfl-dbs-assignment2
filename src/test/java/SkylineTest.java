import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Scanner;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by mustafa on 22.04.17.
 */
public class SkylineTest {
    @Test
    public void nestedLoopTest() {
        Tuple a1 = new Tuple(10, 10);
        Tuple a2 = new Tuple(11, 9);
        Tuple b2 = new Tuple(100, 10);
        Tuple a3 = new Tuple(12, 8);
        Tuple b1 = new Tuple(15, 8);
        Tuple a4 = new Tuple(12, 9);

        ArrayList<Tuple> tuples = new ArrayList<>();
        ArrayList<Tuple> expected = new ArrayList<>();
        tuples.add(a1);
        tuples.add(a2);
        tuples.add(a3);
        tuples.add(b1);
        tuples.add(b2);
        tuples.add(a4);


        ArrayList<Tuple> result = Skyline.nlSkyline(tuples);

        assertTrue(result.contains(a1));
        assertTrue(result.contains(a2));
        assertTrue(result.contains(a3));
        assertEquals(result.size(), 3);
    }

    @Test
    public void dataSetTest() throws FileNotFoundException {
        File file = new File(SkylineTest.class.getResource("car.csv").getFile());
        Scanner scanner = new Scanner(file);

        ArrayList<Tuple> l = new ArrayList<>();
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            String parts[] = line.split("\\|");
            l.add(new Tuple(Integer.parseInt(parts[0]), Integer.parseInt(parts[1])));
        }

        ArrayList<Tuple> skyline = Skyline.dcSkyline(l, 100);

        assertEquals(100, skyline.size());
        for (int i = 0; i < skyline.size(); i++) {
            for (int j = 0; j < i; j++) {
                assertFalse(skyline.get(i).dominates(skyline.get(j)));
                assertFalse(skyline.get(j).dominates(skyline.get(i)));
            }
        }

    }
}
