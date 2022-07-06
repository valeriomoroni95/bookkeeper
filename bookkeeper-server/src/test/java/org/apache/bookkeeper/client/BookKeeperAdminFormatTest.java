package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collection;

//Test public static boolean format(ServerConfiguration conf, boolean isInteractive, boolean force).
//It formats the BookKeeper metadata in zookeeper.
@RunWith(Parameterized.class)
public class BookKeeperAdminFormatTest extends BookKeeperClusterTestCase{

    //BookKeeper default digest type, used in test classes.
    private final BookKeeper.DigestType DefaultDigestType = BookKeeper.DigestType.CRC32;

    private final String DefaultPassword = "Default_Password";

    //used to instantiate the super class and to create a ledger in setup phase.
    private static final int BookiesNumber = 2;

    private final boolean hasAValidServerConfiguration; //if ServerConfiguration is valid or not
    private final boolean isInteractive; //Whether format should ask prompt for confirmation if old data exists or not.
    private final boolean isInteractiveClient; //Client answer
    private final boolean force; //If non-interactive and force is true, then old data will be removed without prompt.
    private boolean expectedOutcome;


    public BookKeeperAdminFormatTest(boolean expectedOutcome, boolean hasAValidServerConfiguration, boolean isInteractive, boolean isInteractiveClient, boolean force) {

        //Call to the BookKeeperClusterTestCase constructor
        super(BookiesNumber);
        //test parameters
        this.expectedOutcome = expectedOutcome;
        this.hasAValidServerConfiguration = hasAValidServerConfiguration;
        this.isInteractive = isInteractive;
        this.isInteractiveClient = isInteractiveClient;
        this.force = force;

    }

    //Parameters association
    @Parameterized.Parameters
    public static Collection<?> getParameters(){
        return Arrays.asList(new Object[][] {

                //EXPECTED_OUTCOME, HAS_VALID_SERV_CONF, IS_INTERACTIVE, IS_INTERACTIVE_CLIENT,   FORCE
                {       false,             false,             true,             false,            false},
                {       true,              true,              false,            false,             true},
                {       false,             true,              true,             false,            false},
                {       true,              true,              true,             true,              true},
                {       false,             true,              true,             false,            false},
                {       true,              true,              false,            false,             true},

        });
    }

    @Before
    public void setUp(){

        System.out.println("Configuring test environment");
        try {
            //Calling parent constructor setup method
            super.setUp();
        }catch (Exception e){ e.printStackTrace(); }

        try {
            //Construct a default client-side configuration
            ClientConfiguration conf = new ClientConfiguration();
            //set the metadataServiceUri from Zookeeper Cluster
            conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

            //Declaring how many ledgers I want to create
            int numOfLedgers = 2;

            //Create a bookkeeper client using a configuration object.
            //A zookeeper client and a client event loop group will be
            //instantiated as part of this constructor.
            try (BookKeeper bkc = new BookKeeper(conf)) {
                //Adding 2 ledgers
                for (int i = 0; i < numOfLedgers; i++) {
                    try (LedgerHandle lh = bkc.createLedger(BookiesNumber, BookiesNumber, DefaultDigestType, DefaultPassword.getBytes())) {
                        //Adding entry for each ledger
                        //Using a random entry
                        lh.addEntry("000".getBytes());
                    }
                }
            }
            catch( Exception e){
                e.printStackTrace();
            }

        } catch (Exception e1) {
            e1.printStackTrace();
        }

    }


    @Test
    public void formatTest() {

        System.out.println("Starting format test");

        boolean outcome;

        //If both isInteractive and isInteractiveClient are true, set new standard input stream,
        //Creates ByteArrayInputStream that uses buf as its buffer array. The initial value of
        //position is offset and the initial value of count is the minimum of offset+length and buf.length.
        if (isInteractive) {
            //If this param is true, the client wrote "y\n". length is 2 because "y\n" are 2 bytes.
            if (isInteractiveClient) System.setIn(new ByteArrayInputStream("y\n".getBytes(), 0, 2));
            //Otherwise, the client pressed "n\n", so do the same with that buf.
            else System.setIn(new ByteArrayInputStream("n\n".getBytes(), 0, 2));
        }

        try{
            //If hasAValidServerConfiguration is true, call format with inherited baseConf, from BookKeeperClusterTestCase.
            if(hasAValidServerConfiguration) outcome = BookKeeperAdmin.format(baseConf, isInteractive, force);
            //Otherwise, call it passing null
            else outcome = BookKeeperAdmin.format(null, isInteractive, force);
        //If an exception is raised, the outcome is false.
        } catch (Exception e) {
            outcome = false;
            e.printStackTrace();
        }

        Assert.assertEquals(outcome, expectedOutcome);
        System.out.println("Format test ended");


    }

    @After
    public void tearDown() throws Exception{
        System.out.println("Starting tearDown");
        //Calling the constructor tearDown method
        super.tearDown();
        System.out.println("tearDown ended");
    }
}
