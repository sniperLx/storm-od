import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Slf4jTest {

//  private final static Logger log = LoggerFactory.getLogger(Slf4jTest.class);

    private final static Logger log1 = LoggerFactory.getLogger("log.sync");


    public static void main(String[] args) {

        try {
            log1.error("this is an error test.");
            log1.info("{} is {}.", new Object[]{"a", "b"});
            System.out.println("OK.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
