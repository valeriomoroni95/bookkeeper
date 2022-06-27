package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class LedgerHandleReadEntriesTest extends BookKeeperClusterTestCase {


    //Declaration of widely used string in test class.

    //BookKeeper default dygest type, used in test classes.

    private static final int BookiesNumber = 10;
    private static final BookKeeper.DigestType DefaultDigestType = BookKeeper.DigestType.CRC32;
    //Bookkeeper default number of entries, used in test classes.
    private static final int NumberOfEntries = 10;
    private static final String Password = "generic_password";

    private static final String ValidString = "This string should be valid, should not generate any problems and should be long enough to be a valid parameter";
    // Creating a generic byte buffer.
    private static byte[] ValidByteData = ValidString.getBytes();
    private static final int ValidOffset = 0;
    private static int ValidDataLength = ValidByteData.length;

    //Using a black-box approach, I'm using strings defining what I expect from each test case.
    private static final String Success = "Passed.";
    private static final String Fail = "Failed.";

    private static final String NotExpectedException = "I wasn't expecting this exception to be raised right here: ";
    private static final String ShouldFail = "I was expecting this test to fail, but it didn't.";

    // Test parameters.
    private static long firstEntry;
    private static long lastEntry;
    private static String expectedOutcome;

    // Class under test.
    private LedgerHandle lh;


    //Public constructor --> It calls the configure method.
    public LedgerHandleReadEntriesTest(long firstEntry, long lastEntry, String expectedOutcome){
        super(BookiesNumber);
        configure(firstEntry,lastEntry,expectedOutcome);
    }

    //Parameters instantiation
    public void configure(long firstEntry, long lastEntry, String expectedOutcome){
        this.firstEntry=firstEntry;
        this.lastEntry=lastEntry;
        this.expectedOutcome=expectedOutcome;
    }


    //Parameters association
    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{

                //First Entry   Last Entry    Expected Outcome
                {     1,            2,           Success},
                {     1,            1,           Success},
                {     0,           -1,              Fail},
                {    -1,            0,              Fail},
                /*// added test cases to boost coverage up
                {     0,   NumberOfEntries - 1,     PASS},
                {     0,   NumberOfEntries,         FAIL},
                {     0,   NumberOfEntries + 1,     FAIL},*/
        });
    }


    //Before each test, I need to instantiate LedgerHandle class. To accomplish this, I need to create a Ledger.
    //After that, I write NumberOfEntries entries into it.
    @Before
    public void setup() {
        try {
            //bkc is a BookkeeperTestClient istance, in BookkeeperClusterTestCase, needed to setup the environment.
            lh = bkc.createLedger(DefaultDigestType, Password.getBytes());
        } catch (BKException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < NumberOfEntries; i++) {
            try {
                lh.addEntry(ValidByteData, ValidOffset, ValidDataLength);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BKException e) {
                e.printStackTrace();
            }
        }
    }


    //Test method.
    @Test
    public void ledgerReadEntriesTest() {
        Enumeration<LedgerEntry> entries = null;
        try {
            //Try the method under test passing each test case from getTestParameters()
            entries = lh.readEntries(firstEntry, lastEntry);
        } catch (Exception e) {
            assertTrue(buildExceptionString(e), expectedOutcome == Fail);
            return;
        }
        assertTrue("Enumeration read is empty.", entries.hasMoreElements());
        int readEntries = 0;
        // Read each entry and check if what I wrote is what I'm reading.
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertTrue("What I'm reading is different from the input", Arrays.equals(entry.getEntry(), ArrayUtils.subarray(ValidByteData, ValidOffset, ValidDataLength)));
            readEntries++;
        }
        assertEquals("I'm reading less entries comparing them to the input", lastEntry - firstEntry + 1, readEntries);
        assertTrue(ShouldFail, expectedOutcome == Success);
    }


    //Teardown phase.
    @After
    public void teardown() {
        try {
            lh.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
    }

    public static String buildExceptionString(Exception e) {

        return NotExpectedException + e.toString();
    }
}
