package hbasedemo.test;

import hbasedemo.businesslogic.*;
import java.io.*;
import java.util.*;
import org.junit.*;

public class HBaseDemoTest {
    
    public HBaseDemoTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
    @Test
    public void testReadData() throws IOException {
        IntervalDataSet intervalDataSet = DataLayer.readIntervalDataSet("1000");
    }
    
    static final int BATCH_SIZE = 10;
    @Test
    public void testGenerateData() throws IOException {        
        int startMeterId = 1;
        try {
            FileReader fr = new FileReader("c:/meternumberwritten.txt");
            BufferedReader br = new BufferedReader(fr);
            startMeterId = Integer.parseInt(br.readLine());
        }
        catch (Exception e) {};
        while (startMeterId <= 159991)
        {
            System.out.println("Generating batch for base meter number: " + startMeterId);
            List<IntervalDataSet> intervalDataSets = new ArrayList<IntervalDataSet>();
            for (int i = 0; i < BATCH_SIZE; i++)
            {
                intervalDataSets.add(DataLayer.generateIntervalDataSet(Integer.toString(startMeterId + i)));                
            }
            DataLayer.writeIntervalDataSets(intervalDataSets);                
            startMeterId += BATCH_SIZE;
            
            PrintWriter writer = new PrintWriter("c:/meternumberwritten.txt", "UTF-8");
            writer.println(Integer.toString(startMeterId));
            writer.close();
        }               
    }
}

