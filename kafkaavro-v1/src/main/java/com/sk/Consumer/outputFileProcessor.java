package com.sk.Consumer;

import com.sk.CDR;
import com.sk.CDRKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class outputFileProcessor {

    public void processOutputFile( ConsumerRecord<CDRKey, CDR> record ) {
        //a.getFileDestination();
        System.out.println("*********");
        String Destination=record.key().getFileDestination();
        String fileName = record.key().getFileName();
        String str=buildCdr(record.value());


        fileWriter( Destination, fileName, str );
        System.out.println( str );


    }

    public void fileWriter( String Destination, String fileName, String str ) {
        try {
            BufferedWriter f = new BufferedWriter( new FileWriter( Destination + "//" + fileName, true ) );
            f.write( str );
            f.close();
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }
    public String buildCdr(CDR cdr){
        StringBuilder str = new StringBuilder();
        str.append( cdr.getCustomerName());
        str.append( "," );
        str.append( cdr.getCustAccountNum());
        str.append( "," );
        str.append( cdr.getPlanID());
        str.append( "," );
        str.append( cdr.getDataUsage() );
        str.append( "," );
        str.append( cdr.getBill() );
        str.append( "\n" );
        return str.toString();
    }
}

