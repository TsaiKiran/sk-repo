package com.sk.Producer;
import com.sk.CDR;
import com.sk.CDRKey;
import com.sk.ConfigReader;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class RecordProducerThread implements Runnable {
    private Logger log = LoggerFactory.getLogger(RecordProducerThread.class.getSimpleName());
    private final ArrayBlockingQueue<CDR> cdrQueue;
    private final CountDownLatch latch;
    private final KafkaProducer<CDRKey, CDR> kafkaProducer;
    private final String Topic= ConfigReader.getProperty("ProducerTopic");
    FileProcessor fileProcessor=new FileProcessor();

    public RecordProducerThread( ArrayBlockingQueue<CDR> cdrQueue, CountDownLatch latch) {
        this.cdrQueue = cdrQueue;
        this.latch = latch;
        this.kafkaProducer = createKafkaProducer();
    }
    KafkaProducer<CDRKey, CDR> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty( "bootstrap.servers", ConfigReader.getProperty("bootstrap.servers"));
        properties.setProperty( "acks","1" );
        properties.setProperty( "retries",ConfigReader.getProperty("producer.retries") );
        properties.setProperty( "key.serializer", KafkaAvroSerializer.class.getName() );
        properties.setProperty( "value.serializer", KafkaAvroSerializer.class.getName() );
        properties.setProperty( "schema.registry.url",ConfigReader.getProperty( "schema.registry.url" ) );
        //properties.setProperty( ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true" );
        //properties.setProperty( ProducerConfig.RETRIES_CONFIG, Integer.toString( Integer.MAX_VALUE ) );
        //properties.setProperty( ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5" );//Same,as default value is 5
        //Message Compression: snappy,lz4,gzip
        //properties.setProperty( ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy" );
        //linger time in milliseconds
        //properties.setProperty( ProducerConfig.LINGER_MS_CONFIG, "15" );
        //Batch size configuration
        //properties.setProperty( ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString( 64 * 1024 ) );//16KB is the batch size

        return new KafkaProducer<>( properties );
    }

    @Override
    public void run() {
        int recordCount=0;
        try{
            while(latch.getCount()>1 || cdrQueue.size()>0){
                CDR cdr=cdrQueue.poll();
                if(cdr==null)
                    Thread.sleep( 200 );
                else{
                    recordCount += 1;
                    System.out.println( "Sending cdr"+recordCount+":"+cdr.getFileName() );

                    CDRKey key=fileProcessor.keyGenerator( cdr );
                    System.out.println("Key generated to send"+key.getFileID());
                    kafkaProducer.send( new ProducerRecord<>(Topic,key,cdr) );
                    System.out.println("Sending cdr");

                    Thread.sleep(200);

                }
            }
        } catch (InterruptedException e) {
            log.warn( "Avro Producer is Interrupted" );
            e.printStackTrace();
        }
        finally {
            close();
        }

    }
    public void close(){
        System.out.println( "Closing Producer" );
        kafkaProducer.close();
        latch.countDown();
    }



}
