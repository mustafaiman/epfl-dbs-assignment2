import org.junit.Test;

import java.util.ArrayList;

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
}
