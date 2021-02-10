package com.sk.Producer;


import com.sk.CDR;
import com.sk.CDRKey;
import com.sk.ConfigReader;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Scanner;

public class FileProcessor {
    private static final String FQCN = FileProcessor.class.getName();
    private static final String ArchivePath = ConfigReader.getProperty("ArchiveFilePath" );

    public void fileReader( String filePath, String fileName, int Latch, List<CDR> cdrs ) {
        try {
            Scanner scanner = new Scanner( new FileReader(
                    filePath ) );

            while (scanner.hasNextLine()) {
                String b = scanner.nextLine();
                // read next line
                cdrs.add( doRecordParsing( b, fileName, Latch ) );
                System.out.println( "CDR's Size: " + cdrs.size() );

            }
            scanner.close();
            Archivefile( filePath, fileName );

        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }

    public CDR doRecordParsing( String record, String fileName, int Latch ) {
        String[] values = record.split( ",", -1 );
        CDR cdr = CDR.newBuilder()
                .setFileName( fileName )
                .setCustomerName( values[0] )
                .setCustAccountNum( values[1] )
                .setDataUsage( Float.parseFloat( values[2] ) )
                .setPlanID( Integer.parseInt( values[3] ) )
                .setBill( Long.parseLong( values[4] ) )
                .build();
        System.out.println( cdr.getCustomerName() + "," + cdr.getCustAccountNum() );
        return cdr;

    }
    public CDRKey keyGenerator( CDR cdr){
        System.out.println("***in key Generator");
        String[] tokens=cdr.getFileName().split( ".",-1 );
        System.out.printf( "%s %s %s%n", tokens[0], tokens[1], tokens[2] );
        CDRKey key=CDRKey.newBuilder()
                .setFileName( cdr.getFileName() )
                .setFileDestination( ConfigReader.getProperty( "DestinationFilePath" ) )
                .setFileID( tokens[1] )
                .build();
        return key;
    }

    public void Archivefile( String filePath, String fileName ) {

        try {
            Files.move( Paths.get( filePath ), Paths.get( ArchivePath + fileName ), StandardCopyOption.REPLACE_EXISTING );
            System.out.println( "Successfully moved the file" );
        } catch (IOException exception) {
            System.out.println( "Failed to move the file" );
            exception.printStackTrace();
        }



    } 


}




