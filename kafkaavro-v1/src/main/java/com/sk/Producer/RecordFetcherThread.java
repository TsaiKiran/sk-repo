package com.sk.Producer;


import com.sk.CDR;
import com.sk.ConfigReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class RecordFetcherThread implements Runnable {
    private Logger log = LoggerFactory.getLogger( RecordFetcherThread.class.getSimpleName() );
    private final ArrayBlockingQueue<CDR> cdrQueue;
    private final CountDownLatch latch;
    private FF ff;
    private final String filePath = ConfigReader.getProperty( "InputFilePath" );
    private final String fileRegex = ConfigReader.getProperty( "InputFilePattern.regexp" );

    public RecordFetcherThread( ArrayBlockingQueue<CDR> cdrQueue, CountDownLatch latch ) {
        this.cdrQueue = cdrQueue;
        this.latch = latch;
        this.ff = new FF( filePath, fileRegex );

    }

    @Override
    public void run() {
        System.out.println( "reached" + RecordFetcherThread.class.getSimpleName() );
        System.out.println("***********************************");
        try {
            Boolean keepOnRunning = true;
            while (keepOnRunning) {
                List<CDR> cdrs;
                try {
                    cdrs = ff.fetchCDRs();

                    System.out.println( "Fetched" + cdrs.size() + "records" );
                    Thread.sleep( 6 * 1000 );
                    if (cdrs.size() == 0) {
                        keepOnRunning = false;
                    } else {
                        System.out.println( "Queue size" + cdrQueue.size() );
                        for (CDR cdr : cdrs) {
                            cdrQueue.put( cdr );
                        }
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.sleep( 200 );
                } finally {
                    Thread.sleep( 200 );
                }
            }
        } catch (InterruptedException e) {
            log.warn( "RecordFetcher Interrupted" );
        } finally {
            this.close();
        }

    }

    private void close() {
        log.info( "Closing" );
        latch.countDown();
        log.info( "Closed" );
    }

}
