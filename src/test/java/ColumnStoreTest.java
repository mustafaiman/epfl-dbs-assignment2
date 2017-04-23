import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;

/**
 * Created by mustafa on 23.04.17.
 */
public class ColumnStoreTest {
    @Test
    public void filterWhereTest() throws IOException, URISyntaxException {
        ColumnStore cs = new ColumnStore();
        cs.parseSchema("attr1:Int,attr2:String");
        cs.loadData("lineitem.csv");
        assertEquals(1, cs.filterWhere("attr1|=|3").count());
        assertEquals(5, cs.filterWhere("attr2|<|Yaman").count());
        assertEquals(6, cs.filterWhere("attr2|<=|Yaman").count());
        assertEquals(1, cs.filterWhere("attr1|=|4,attr2|=|Yaman").count());
        assertEquals(1, cs.filterWhere("attr1|=|4,attr2|>=|Yaman").count());
        assertEquals(2, cs.filterWhere("attr2|=|Iman").count());
        assertEquals(5, cs.filterWhere("attr1|>=|3").count());
        assertEquals(4, cs.filterWhere("attr1|>|3").count());
    }

    @Test
    public void testAllProcess() throws IOException, URISyntaxException {
        String outputPath = "test-output.csv";
        ColumnStore.main(new String[] {"lineitem.csv", outputPath, "attr1:Int,attr2:String", "attr2,attr1", "attr1|=|4"});
        Scanner scanner = new Scanner(new File(outputPath));
        assertEquals("Mustafa,4", scanner.nextLine());
        assertEquals("Yaman,4", scanner.nextLine());
        new File(outputPath).delete();
    }
}
