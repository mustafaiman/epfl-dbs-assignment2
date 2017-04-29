import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by mustafa on 29.04.17.
 */
public class Task4Test {
    @After
    public void tearDown() {
        if (Task4.sparkContext != null) {
            Task4.sparkContext.stop();
        }
    }

    @Test
    public void testSmall() throws IOException, URISyntaxException {
        String outputPath = "task4small.txt";
        Task4.main(new String[] {"customer.tbl", "orders.tbl", outputPath});


    }
}
