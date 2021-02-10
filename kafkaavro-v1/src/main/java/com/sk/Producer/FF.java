package com.sk.Producer;




import com.sk.CDR;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class FF {
   //public static void main( String args[] ) {
    private final String filePath;//= "F:\\program";
    private final String fileRegex;//= "(TAP)\\.\\d{3}\\.txt";
    FileProcessor fileProcessor = new FileProcessor();
    int noOffiles = 1;

    public FF( String filePath, String fileRegex ) {
        this.filePath = filePath;
        this.fileRegex = fileRegex;
    }

    // try-catch block to handle exceptions
    public List<CDR> fetchCDRs() {
        try {
            System.out.println( "in ff" );
            // Create a file object
            File f = new File( filePath );
            // Create a FileFilter
            FileFilter filter = f1 -> Pattern.matches( fileRegex, f1.getName() );
            // Get all the names of the files present
            // in the given directory
            // which are text files
            File[] files = f.listFiles( filter );
            System.out.println( "no.of matched files:" + files.length );
            return getNextCdrs( files );

        } catch (Exception exception) {
            exception.printStackTrace();
            return Collections.emptyList();
        }
    }

    public List<CDR> getNextCdrs( File[] files ) {
        if (files.length > 0) {
            System.out.println( "No.of files:" + files.length + "-------------------" );
            List<CDR> cdrs = new ArrayList<>();
            for (int i = 0; i < files.length; i++) {
                noOffiles = files.length;
                String filePath = files[i].getAbsolutePath();
                String fileName = files[i].getName();
                System.out.println( filePath );
                System.out.println( fileName );

                fileProcessor.fileReader( filePath, fileName, noOffiles, cdrs );

            }
            return cdrs;
        } else {
            return Collections.emptyList();
        }
    }
}
