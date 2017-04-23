import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by mustafa on 23.04.17.
 */
public class CompressedColumnStoreTest {

    @After
    public void tearDown() {
        if (CompressedColumnStore.sparkContext != null) {
            CompressedColumnStore.sparkContext.stop();
        }
    }

    @Test
    public void testAllProcess() throws IOException, URISyntaxException {
        String outputPath = "test-output.csv";
        CompressedColumnStore.main(new String[] {"lineitem.csv", outputPath, "attr1:Int,attr2:String", "attr2,attr1", "attr2|=|Iman,attr1|<|13", "attr2"});
        Scanner scanner = new Scanner(new File(outputPath));
        assertEquals("Iman,1", scanner.nextLine());
        assertEquals("Iman,12", scanner.nextLine());
        new File(outputPath).delete();
    }

    @Test
    public void testAllProcess2() throws IOException, URISyntaxException {
        String outputPath = "test-output.csv";
        CompressedColumnStore.main(new String[] {"lineitem.csv", outputPath, "attr1:Int,attr2:String", "attr2,attr1", "attr2|=|Iman,attr1|<|10", "attr2"});
        Scanner scanner = new Scanner(new File(outputPath));
        assertEquals("Iman,1", scanner.nextLine());
        new File(outputPath).delete();
    }

    @Test
    public void testAllProcess3() throws IOException, URISyntaxException {
        String outputPath = "test-output.csv";
        CompressedColumnStore.main(new String[] {"lineitem.csv", outputPath, "attr1:Int,attr2:String", "attr2,attr1", "attr2|=|ABSENT", "attr2"});
        Scanner scanner = new Scanner(new File(outputPath));
        assertFalse(scanner.hasNext());
        new File(outputPath).delete();
    }

    @Test
    public void testAllProcess4() throws IOException, URISyntaxException {
        String outputPath = "test-output.csv";
        CompressedColumnStore.main(new String[] {"lineitem.csv", outputPath, "attr1:Int,attr2:String", "attr2,attr1", "attr1|=|3", "attr1"});
        Scanner scanner = new Scanner(new File(outputPath));
        assertEquals("Kamil,3", scanner.nextLine());
        new File(outputPath).delete();
    }
}
