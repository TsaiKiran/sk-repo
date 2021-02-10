package com.sk.Consumer;

import com.sk.CDR;
import com.sk.CDRKey;
import com.sk.ConfigReader;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumerv1 {
    private static outputFileProcessor ff=new outputFileProcessor();

        public static void main( String[] args ) {
        Properties properties=new Properties();
        properties.setProperty( "bootstrap.servers", ConfigReader.getProperty("bootstrap.servers" ));
        properties.setProperty( "group.id",ConfigReader.getProperty("Consumer.Group.ID") );
        properties.setProperty( "enable.auto.commit","false" );
        properties.setProperty( "auto.offset.reset","earliest" );

        properties.setProperty( "key.deserializer", SpecificAvroDeserializer.class.getName() );
        properties.setProperty( "value.deserializer", SpecificAvroDeserializer.class.getName() );
        properties.setProperty( "schema.registry.url",ConfigReader.getProperty( "schema.registry.url" ) );
        properties.setProperty( "specific.avro.reader","true" );


        KafkaConsumer<CDRKey, CDR> consumer= new KafkaConsumer<>( properties );
        String topic =ConfigReader.getProperty( "EnrichedTopic" );

        consumer.subscribe( Collections.singleton( topic ) );
        System.out.println("Waiting for data.........");
        while (true){
            ConsumerRecords<CDRKey,CDR> records= consumer.poll( 500 );
            for(ConsumerRecord<CDRKey,CDR> record:records){
                ff.processOutputFile(record);
                System.out.println(record.toString());
            }
            consumer.commitSync();
        }


    }



}
