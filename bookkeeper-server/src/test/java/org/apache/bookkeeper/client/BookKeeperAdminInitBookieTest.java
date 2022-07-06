package org.apache.bookkeeper.client;

import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;


// Test public static boolean initBookie(ServerConfiguration conf).
// It Initializes bookie, by making sure that the journalDir,
// ledgerDirs and indexDirs are empty and there is no registered
// Bookie with this BookieId.
@RunWith(Parameterized.class)
public class BookKeeperAdminInitBookieTest extends BookKeeperClusterTestCase{

    //Used to instantiate the super class
    private static final int BookiesNumber = 2;

    //Index to recognize the bookie.
    private final int bookieIndex = 0;
    private ServerConfiguration serverConfiguration = null;

    //Test parameters
    private final boolean hasAValidServerConfiguration; //Used to check if a valid server configuration is provided
    private final boolean deleteDirectories; //Used during the setup to decide if it's needed to delete the directories
    private boolean writeCookieOnTheBookie; //Used to check if it's needed to write cookie on the bookie
    private boolean deleteBookie; //Used to check if it's needed to remove the bookie or not
    private boolean expectedOutcome;


    public BookKeeperAdminInitBookieTest(boolean expectedOutcome, boolean hasAValidServerConfiguration, boolean deleteDirectories, boolean writeCookieOnTheBookie, boolean deleteBookie) {
        super(BookiesNumber);
        this.hasAValidServerConfiguration = hasAValidServerConfiguration;
        this.expectedOutcome = expectedOutcome;
        this.deleteDirectories = deleteDirectories;
        this.writeCookieOnTheBookie = writeCookieOnTheBookie;
        this.deleteBookie = deleteBookie;

    }

    @Before
    public void setUp() throws Exception {
        System.out.println("setUp is now starting");

        try {
            //Calling the superclass setUp method
            super.setUp();
        }catch (Exception e){
            e.printStackTrace();
        }

        //If true, configure by index the serverConfiguration, using the BookKeeperClusterTestCase method
        if (hasAValidServerConfiguration) serverConfiguration = confByIndex(bookieIndex);

        //If serverConfiguration is set and deleteDirectories is true
        if (serverConfiguration !=null && this.deleteDirectories) {

            //Get ledgers' metadata directories
            File[] ledgerDirs = serverConfiguration.getLedgerDirs();
            //For each ledger directory
            for (File ledgerDir : ledgerDirs) {
                //Delete the directory
                FileUtils.deleteDirectory(ledgerDir);
            }
            //Get index directories to store ledger index files
            File[] indexDirs = serverConfiguration.getIndexDirs();
            if (indexDirs != null) {
                //For each index directory
                for (File indexDir : indexDirs) {
                    //Delete the directory
                    FileUtils.deleteDirectory(indexDir);
                }
            }
            //Get directories to store journal files
            File[] journalDirs = serverConfiguration.getJournalDirs();
            //For each journal directory
            for (File journalDir : journalDirs) {
                //Delete the directory
                FileUtils.deleteDirectory(journalDir);
            }
        }
    }


    //Parameter association
    @Parameterized.Parameters
    public static Collection<?> getParameters(){
        return Arrays.asList(new Object[][] {
                //expectedOutcome, hasAValidServConfig, deleteDirectories, writeCookieBookie, deleteBookie
                {      true,             true,                true,              false,          true},
                {      false,            false,               true,              false,          true},
                {      false,            true,                false,             true,          false},
                {      false,            true,                true,              true,          false},
                {      false,            true,                true,              false,         false},
                {      false,            true,                true,              true,           true}

        });
    }


    @Test
    public void initBookie() throws Exception {
        System.out.println("Test starts from here");
        boolean outcome;

        //Check if serverConfiguration is already available
        if (serverConfiguration !=null) {

            //Get the bookieID, passing the valid server configuration
            BookieId bookieID = BookieImpl.getBookieId(serverConfiguration);
            //Check if deleteBookie is true or false
            if (deleteBookie) {
                try {
                    //If true, kill the bookie
                    killBookie(bookieIndex);
                }catch (IndexOutOfBoundsException e){
                    //If something is wrong here, killBookie has failed
                    System.out.println("There's no bookie to kill!");
                }

                //Create the path to get the cookie related to the bookie
                String bookieRelatedCookiePath =
                        //Returns the metadata service URI from Zookeeper
                        ZKMetadataDriverBase.resolveZkLedgersRootPath(serverConfiguration)
                                //it means "cookies", but it's a BookKeeper constant
                                + "/" + BookKeeperConstants.COOKIE_NODE
                                //the bookie ID
                                + "/" + bookieID.toString();

                //Delete the cookie related to this bookie with the above created path from Zookeeper.
                //The version is -1 just to match any node's versions.
                zkc.delete(bookieRelatedCookiePath, -1);
            }

            //If writeCookieOnTheBookie is true
            if (this.writeCookieOnTheBookie) {
                //run the Lambda function to create the registration manager used for registering/unregistering bookies.
                runFunctionWithRegistrationManager(serverConfiguration, rm -> {
                    //Write a test cookie in bytes with a charset, giving it also a new version
                    Versioned<byte[]> cookieData = new Versioned<>("testCookieData".getBytes(StandardCharsets.UTF_8), Version.NEW);
                    try {
                        //Write the cookie data, which will be used for verifying the integrity of the bookie environment
                        rm.writeCookie(bookieID, cookieData);

                    } catch (Exception e) {
                        System.out.println("Unable to write cookieData!");
                    }
                    //This Lambda function doesn't return anything, it's used just to write cookie data.
                    return null;
                });
            }

            try {
                //Try the method under test here
                outcome = BookKeeperAdmin.initBookie(serverConfiguration);
            } catch (Exception e) {
                outcome =false;

            }
        //If no serverConfiguration is provided, just call the method under test here
        }else{
            try {
                outcome = BookKeeperAdmin.initBookie(serverConfiguration);
            } catch (Exception e) {
                outcome = false;

            }
        }

        Assert.assertEquals(expectedOutcome, outcome);
        System.out.println("Test has just completed.");


    }

    @After
    public void tearDown() throws Exception {
        System.out.println("tearDown starts now");

        try {
            //Kill the created bookie
            killBookie(bookieIndex);
        }catch (IndexOutOfBoundsException e){
            //If I'm here, there's no bookie to kill
            System.out.println("IndexOutOfBoundsException");
        }
        //Call the father's tearDown method
        super.tearDown();
        System.out.println("tearDown has just completed!");
    }

}
