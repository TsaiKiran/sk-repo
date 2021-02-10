package com.sk.Stream;

public class CustomerDetailsDAO {

    private String CustomerName;
    private String CustAccountNum;
    private int DataUsage;
    private int PlanID;
    private boolean emailstatement;
    private int Bill;


    public String getCustomerName() {
        return CustomerName;
    }

    public void setCustomerName( String customerName ) {
        CustomerName = customerName;
    }

    public String getCustAccountNum() {
        return CustAccountNum;
    }

    public void setCustAccountNum( String custAccountNum ) {
        CustAccountNum = custAccountNum;
    }

    public int getDataUsage() {
        return DataUsage;
    }

    public void setDataUsage( int dataUsage ) {
        DataUsage = dataUsage;
    }

    public int getPlanID() {
        return PlanID;
    }

    public void setPlanID( int planID ) {
        PlanID = planID;
    }

    public boolean isEmailstatement() {
        return emailstatement;
    }

    public void setEmailstatement( boolean emailstatement ) {
        this.emailstatement = emailstatement;
    }

    public int getBill() {
        return Bill;
    }

    public void setBill( int bill ) {
        Bill = bill;
    }
}
