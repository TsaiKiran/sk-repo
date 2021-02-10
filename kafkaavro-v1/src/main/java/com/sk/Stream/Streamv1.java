package com.sk.Stream;

import com.sk.CDR;
import com.sk.CDRKey;
import com.sk.ConfigReader;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.*;


public class Streamv1 {
    private static int count = 0;


    public static void main( String[] args ) {
        System.out.println( "Starting to Stream..." );
        HashMap<String, CustomerDetailsDAO> recordMap = null;
        try {
            recordMap = MySqlDriver.dbPull();
            System.out.println( recordMap.keySet() );
        } catch (Exception exception) {
            exception.printStackTrace();
        }

        //System.out.println(recordMap.get("12323").getBill());
        //System.out.println(recordMap.get("5345").getCustAccountNum()+"***"+recordMap.get("12323").getCustAccountNum()+"***"
        //+recordMap.get( "ac10yt" ).getCustAccountNum());

        Properties config = new Properties();
        config.put( StreamsConfig.APPLICATION_ID_CONFIG, ConfigReader.getProperty( "Streams.App.ID" ) );
        config.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigReader.getProperty( "bootstrap.servers" ) );
        config.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest" );
        config.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class );
        config.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class );
        config.put( "schema.registry.url", ConfigReader.getProperty( "schema.registry.url" ) );
        config.put( StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0" );//Not a production practice

        StreamsBuilder builder = new StreamsBuilder();
        //Stream from Kafka
        KStream<CDRKey, CDR> trojan = builder.stream( ConfigReader.getProperty( "ProducerTopic" ) );

        //List<String> record=null;

        HashMap<String, CustomerDetailsDAO> finalRecordMap = recordMap;
        KStream<CDRKey, CDR> product = trojan.mapValues( ( value ) -> {
            return DataEnricher( value, finalRecordMap );
        } );
        product.to( ConfigReader.getProperty( "EnrichedTopic" ) );


        KafkaStreams streams = new KafkaStreams( builder.build(), config );
        streams.start();
        //print the topology
        System.out.println( streams.toString() );
        //Shutdown hook for graceful shutdown
        Runtime.getRuntime().

                addShutdownHook( new Thread( streams::close ) );


    }

    private static CDR DataEnricher( CDR cdr, HashMap<String, CustomerDetailsDAO> finalRecordMap ) {
        count++;
        System.out.println( "Count:" + count );
        CDR baseCdr = cdr;
        CDR outCdr = CDR.newBuilder()
                .setFileName( baseCdr.getFileName() )
                .setCustomerName( baseCdr.getCustomerName() )
                .setCustAccountNum( baseCdr.getCustAccountNum() )
                .setDataUsage( baseCdr.getDataUsage() )
                .setPlanID( baseCdr.getPlanID() )
                .setBill( (long) enrich( baseCdr.getCustAccountNum(), finalRecordMap ) )
                .build();
        return outCdr;
    }

    private static int enrich( String AccoutNum, HashMap<String, CustomerDetailsDAO> recordMap ) {
        if (recordMap.containsKey( AccoutNum )) {
            int Bill = recordMap.get( AccoutNum ).getBill();
            System.out.println( "Accountnum:" + AccoutNum + "***BILL:" + Bill );
            return Bill;
        } else {
            return -1;
        }

    }
}





