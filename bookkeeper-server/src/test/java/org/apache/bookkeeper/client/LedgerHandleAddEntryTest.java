package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class LedgerHandleAddEntryTest extends BookKeeperClusterTestCase {


    //Declaration of widely used string in test class.

    //Default Bookies Number value, used in test classes.
    private static final int BookiesNumber = 10;
    private static final String NotExpectedException = "I wasn't expecting this exception to be raised right here: ";

    //BookKeeper default digest type, used in test classes.
    private static final BookKeeper.DigestType DefaultDigestType = BookKeeper.DigestType.CRC32;
    private static final String ValidString = "This string should be valid, should not generate any problems and should be long enough to be a valid parameter";
    private static final String EmptyString = "";
    private static final String Password = "generic_password";

    //Using a black-box approach, I'm using strings defining what I expect from each test case.
    private static final String Success = "Passed.";
    private static final String Fail = "Failed.";
    private static final String ShouldFail = "I was expecting this test to fail, but it didn't.";

    // Test parameters.
    private static byte[] data;
    private static int offset;
    private static int length;
    private static String expectedOutcome;

    // Class under test.
    private LedgerHandle lh;

    //Public constructor --> It calls the configure method and calls the parent constructor
    public LedgerHandleAddEntryTest(byte[] data, int offset, int length, String expectedOutcome){
        super(BookiesNumber);
        configure(data, offset, length, expectedOutcome);
    }

    //Parameters instantiation
    public void configure(byte[] data, int offset, int length, String expectedBehavior){
        this.data = data;
        this.offset=offset;
        this.length=length;
        this.expectedOutcome =expectedBehavior;
    }


    //Parameters association
    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        // Build an array with bytes out of the byte size.
        Integer outOfLimitsValue = -4000;
        Byte outOfLimitsByteValue = outOfLimitsValue.byteValue();
        byte[] outOfLimitsByteArray = {outOfLimitsByteValue, outOfLimitsByteValue};
        int stringLength = ValidString.length();
        return Arrays.asList(new Object[][]{

                //      DATA              OFFSET           LENGTH     EXPECTED_OUTCOME
                {ValidString.getBytes(),    1,          stringLength - 1,      Success},
                {ValidString.getBytes(), stringLength - 1,        0,           Success},
                {ValidString.getBytes(), stringLength,            0,           Success},
                {ValidString.getBytes(),    0,                   -1,              Fail},
                {ValidString.getBytes(),   -1,            stringLength,           Fail},
                {ValidString.getBytes(), stringLength + 1,        1,              Fail},
                {ValidString.getBytes(),    1,            stringLength,           Fail},
                {EmptyString.getBytes(),    0,            stringLength,           Fail},
                {outOfLimitsByteArray,      0,            stringLength,           Fail},
                {       null,               0,            stringLength,           Fail},
        });
    }

    //Before each test, I need to instantiate LedgerHandle class. To accomplish this, I need to create a Ledger.
    @Before
    public void setupTheEnvironment() {
        try {
            //bkc is a BookkeeperTestClient instance, in BookkeeperClusterTestCase, needed to setup the environment.
            lh = bkc.createLedger(DefaultDigestType, Password.getBytes());
        } catch (BKException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //Test method.
    @Test
    public void ledgerAddEntryThenReadIt() {

        //Initializing the return value.
        long returnValue = -1;
        try {
            // Try the method under test passing each test case from getTestParameters()
            // The return value here is the entryID of the new inserted entry.
            returnValue = lh.addEntry(data, offset, length);
        } catch (Exception e) {
            //If something happens here, it means addEntry has failed.
            assertTrue(notExpectedExceptionString(e), expectedOutcome == Fail);
            return;
        }
        //If everything is fine, returnValue should be >=0
        assertTrue("Invalid entry ID: " + returnValue, returnValue >= 0);

        //Now I read to check what I wrote
        //I declare an enumeration of entries, initialized to null.
        Enumeration<LedgerEntry> entries = null;
        try {
            //Read entries from the first one till the last confirmed
            entries = lh.readEntries(0, lh.getLastAddConfirmed());
        } catch (Exception e) {
            //If something goes bad here, readEntries has failed.
            assertTrue(notExpectedExceptionString(e), expectedOutcome == Fail);
        }
        //while there are elements in the enumeration
        while (entries.hasMoreElements()) {
            //Get the next element
            LedgerEntry entry = entries.nextElement();
            //Get the content of the entry
            byte[] returned = entry.getEntry();
            //Create a new String, starting from offset and reaching offset + length
            //in order to obtain a substring with length --> length
            String newString = new String(data).substring(offset, offset + length);
            //Check if what I read is exactly the same string that I wrote
            assertTrue("Input and output doesn't coincide." + newString + " --- " + new String(returned), Arrays.equals(newString.getBytes(), returned));
        }
        //If everything goes well, mark this test with Success
        assertTrue(ShouldFail, expectedOutcome == Success);
    }

    //Teardown phase.
    @After
    public void cleanUp() {
        try {
            lh.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
    }

    public static String notExpectedExceptionString(Exception e) {

        return NotExpectedException + e.toString();
    }
}
