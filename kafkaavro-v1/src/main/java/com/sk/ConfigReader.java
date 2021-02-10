package com.sk;
import com.mysql.jdbc.StringUtils;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Timer;


public class ConfigReader {
    public static final String FQCN=ConfigReader.class.getName();

    private ConfigReader(){ }
    private static final Properties properties = new Properties();
    private static final Properties dbProperties =new Properties();
    private static final String configProperties="kafka.properties";
    private static Timer timer;
    private static InputStream inStream;


    public static String getConfigProperties() {
        return configProperties;
    }
     static{
        try {
            inStream=(ConfigReader.class.getClassLoader().getResourceAsStream(configProperties ));
            if(inStream!=null){
                properties.load( inStream );
            }
        } catch (IOException exception) {
            exception.printStackTrace();
            System.out.println(FQCN+"Unable to find"+configProperties+" file in class path");
        }
    }
    public static String getProperty(String propertyName){
        String retPropertyValue=properties.getProperty(propertyName);
        if(StringUtils.isEmptyOrWhitespaceOnly( retPropertyValue )){
            System.out.println("No Valid Value for given Property Name/Key");
            return dbProperties.getProperty(propertyName);
        }
        else{
            return retPropertyValue;
        }

    }
}