package com.sk.Producer;


import com.sk.CDR;
import org.quartz.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

public class KafkaAvroProducerv1{
   private Logger log = LoggerFactory.getLogger( KafkaAvroProducerv1.class.getSimpleName() );

    private ExecutorService executor;
    private CountDownLatch latch;
    private RecordFetcherThread recordFetcherThread;
    private RecordProducerThread recordProducerThread;

    public static void main( String[] args ) {
        KafkaAvroProducerv1 app = new KafkaAvroProducerv1();
        app.start();
    }

    private KafkaAvroProducerv1() {
        latch = new CountDownLatch( 2 );
        executor = Executors.newFixedThreadPool( 2 );
        ArrayBlockingQueue<CDR> cdrsQueue = new ArrayBlockingQueue<>( 500 );
        recordFetcherThread = new RecordFetcherThread( cdrsQueue, latch );
        recordProducerThread = new RecordProducerThread( cdrsQueue, latch );
    }

    private void start() {
        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            if (!executor.isShutdown()) {
                log.info( "Shutdown triggered" );
                shutdown();
            }
        } ) );
        log.info( "Application started!" );
        executor.submit( recordFetcherThread );
        executor.submit( recordProducerThread );
        System.out.println( "Two tasks" );
        log.info( "TWo runnable task's are submitted" );
        try {
            log.info( "Latch await" );
            latch.await();
            log.info( "Threads completed" );
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            shutdown();
            log.info( "Application closed succesfully" );
        }
    }

    private void shutdown() {
        if (!executor.isShutdown()) {
            log.info( "Shutting down" );
            System.out.println( "shutting down" );
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination( 2000, TimeUnit.MILLISECONDS )) { //optional *
                    log.warn( "Executor did not terminate in the specified time." ); //optional *
                    List<Runnable> droppedTasks = executor.shutdownNow(); //optional **
                    log.warn( "Executor was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed." ); //optional **
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }


}

