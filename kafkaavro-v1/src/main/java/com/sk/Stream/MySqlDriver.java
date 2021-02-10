package com.sk.Stream;

import com.sk.ConfigReader;

import java.sql.*;
import java.util.HashMap;

public class MySqlDriver {
    private static final String FQCN = MySqlDriver.class.getSimpleName();
    private CustomerDetailsDAO DAO = new CustomerDetailsDAO();

    public static Connection getConnection() {
        Connection con = null;
        try {
            con = DriverManager.getConnection( ConfigReader.getProperty( "db.url" ), ConfigReader.getProperty( "db.user" ), ConfigReader.getProperty( "db.pwd") );
        } catch (SQLException ex) {
            System.out.println( FQCN + "exception:" + ex );
        }
        return con;
    }

    public static HashMap<String, CustomerDetailsDAO> dbPull() {

        // create the hashmap
        HashMap<String, CustomerDetailsDAO> map = new HashMap<String, CustomerDetailsDAO>();

        Statement st = null;
        ResultSet rs = null;
        Connection con = getConnection();


        try {
            st = con.createStatement();
            rs = st.executeQuery( ConfigReader.getProperty( "getCustomerDetails_query" ) );
            while (rs.next()) {
                CustomerDetailsDAO dao = new CustomerDetailsDAO();
                dao.setCustomerName( rs.getString( "CustomerName" ) );
                dao.setCustAccountNum( rs.getString( "CustAccountNum" ) );
                dao.setDataUsage( rs.getInt( "DataUsage" ) );
                dao.setPlanID( rs.getInt( "PlanID" ) );
               // System.out.println(rs.getInt( "PlanID" ));
                dao.setEmailstatement( rs.getBoolean( "emailStatement" ) );
                dao.setBill( rs.getInt( "Bill" ) );

                // set data in the hashmap
                map.put( dao.getCustAccountNum(), dao );
               // int a = map.get( dao.getCustAccountNum() ).getBill();
               // System.out.println(dao.getCustAccountNum()+"***"+dao.getBill()+"***");

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return map;
    }
}
