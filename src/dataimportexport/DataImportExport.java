/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dataimportexport;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Stream;
/**
 *
 * @author mahfujar.alam
 */
public class DataImportExport {

    private static final String FILE_EXPORT_LOCATION="E:\\Repository\\ExportedFiles";
    private final int MAX_LINE_PER_FILE=200000;
    private int validDataWriteCount=0;
    private int invalidDataWriteCount=0;
    private boolean isCreatedValidFile [];
    private boolean isCreatedInvalidFile [];
    private static int noOfLineExecutedPerThread=50000;
    private static int counterInsertThreadExit=0;
    private static String databaseConnectionURL="jdbc:sqlserver://localhost:1433;databaseName=CustomerDB;" + "user=sa;password=a;";
    public static void main(String[] args) {      
        FileInputStream fileInputStream=null;
        List<List<String>> allDataList=new ArrayList<>(); 
        List<String> listPhoneNo=new ArrayList<>();
        try{
            int noOflines,counter=1;
            String fileLocation="E:\\Repository\\1Mcustomers.txt";
            fileInputStream=new FileInputStream(fileLocation);
            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(fileInputStream)); 
            Stream<String> streamLines=bufferedReader.lines();
            Iterator iterator=streamLines.iterator();
            List<String> dataRows=new ArrayList<>();       
            while(iterator.hasNext()){
                String singleRow=(String)iterator.next();
                String [] singleRowArray=singleRow.split(",");
                if(!listPhoneNo.contains(singleRowArray[5])){
                    dataRows.add(singleRow);
                    listPhoneNo.add(singleRowArray[5]);
                }
                if(counter>=noOfLineExecutedPerThread){
                    if(!dataRows.isEmpty()){
                        allDataList.add(dataRows);
                    } 
                    dataRows=new ArrayList<>();
                    counter=0;
                }
                counter++;
            }
            if(!dataRows.isEmpty()){
                allDataList.add(dataRows);
            }  
            noOflines=allDataList.size();
            if(noOflines<1){
                System.out.println("No Data Found!");
                return;
            }
            DataImportExport dataImportExport=new DataImportExport();
            dataImportExport.threadStartImport(allDataList);
            while(true){
                if(counterInsertThreadExit==allDataList.size()){
                    break;
                }
                Thread.sleep(3000);
            }
            Statement statement;
            Connection connection = null;
            int countValidData=0,countInvalidData=0;
            connection = DriverManager.getConnection(databaseConnectionURL);
            statement = connection.createStatement();
            ResultSet resultSet=statement.executeQuery("exec CountCustomer");
            while(resultSet.next()){
                countValidData=Integer.valueOf(resultSet.getString("CountAllValid"));
                countInvalidData=Integer.valueOf(resultSet.getString("CountAllInvalid"));
                break;
            }
            dataImportExport.threadStartExport(countValidData,countInvalidData);
        } catch(FileNotFoundException e){
            System.out.println("File not found! : "+e.getMessage());
        } 
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    private void threadStartExport(int countValidData,int countInvalidData){
        try{
            int threadCountValid=((int)(countValidData/noOfLineExecutedPerThread))+1;
            isCreatedValidFile=new boolean[(countValidData/MAX_LINE_PER_FILE)+1];
            List<ReadAndExportValidData> exportValidData=new ArrayList<>();
            for(int i=0;i<threadCountValid;i++){
                int maxIndex=(i+1)*noOfLineExecutedPerThread;
                if(maxIndex>countValidData){
                    maxIndex=countValidData;
                }
                exportValidData.add(new ReadAndExportValidData(i,i*noOfLineExecutedPerThread,maxIndex));
            }
            threadCountValid=((int)(countInvalidData/noOfLineExecutedPerThread))+1;
            isCreatedInvalidFile=new boolean[(countInvalidData/MAX_LINE_PER_FILE)+1];
            List<ReadAndExportInvalidData> exportInvalidData=new ArrayList<>();
            for(int i=0;i<threadCountValid;i++){
                int maxIndex=(i+1)*noOfLineExecutedPerThread;
                if(maxIndex>countInvalidData){
                    maxIndex=countInvalidData;
                }
                exportInvalidData.add(new ReadAndExportInvalidData(i,i*noOfLineExecutedPerThread,maxIndex));
            }
            for(int i=0;i<exportValidData.size();i++){
                exportValidData.get(i).start();
            }
            for(int i=0;i<exportInvalidData.size();i++){
               exportInvalidData.get(i).start();
            }
        } catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    private void threadStartImport(List<List<String>> allDataList){
        try{
            List<ReadAndInsertData> raidwts=new ArrayList<>();
            for(int i=0;i<allDataList.size();i++){
                ReadAndInsertData readAndInsertData=new ReadAndInsertData(i, allDataList.get(i));
                raidwts.add(readAndInsertData);
            }
            for(int i=0;i<raidwts.size();i++){
                raidwts.get(i).start();
            }
        } catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    private class ReadAndInsertData extends Thread {
        int threadNo;
        List<String> dataRows;
        Statement statement;
        Connection connection = null;
        int invalidLengthcounter=0;
        
        private boolean CheckPhoneNo(String phoneNo){
            try{
                int counterStartBracket=0;
                int counterEndBracket=0;
                for(int i=0;i<phoneNo.length();i++){
                    phoneNo=phoneNo.replace(" ","");
                    if((phoneNo.charAt(i)>='0' && phoneNo.charAt(i)<='9')|| phoneNo.charAt(i)=='-'|| phoneNo.charAt(i)=='(' || phoneNo.charAt(i)==')'){
                        if(phoneNo.charAt(i)=='('){
                            counterStartBracket++;
                        }
                        if(phoneNo.charAt(i)==')'){
                            counterEndBracket++;
                        }
                    } else{
                        return false;
                    }
                }
                return (counterStartBracket==counterEndBracket);
            } catch(Exception e){
                System.out.println(e.toString());
            }
            return false;
        }
        private boolean CheckEmailId(String emailId){
            try{
                String [] opOne=new String[2];
                opOne[0]="";opOne[1]="";
                int dotCount=0;
                boolean foundDot=false;
                if(emailId.length()<5){
                    return false;
                }
                for(int i=0;i<emailId.length();i++){
                    if(emailId.charAt(i)=='.' || foundDot){
                        if(!foundDot){
                            dotCount++;
                        }
                        if(!(emailId.charAt(i)=='.')){
                            opOne[1]+=String.valueOf(emailId.charAt(i));
                        }  
                        foundDot=true;
                    } else{
                        opOne[0]+=String.valueOf(emailId.charAt(i));
                    }
                }
                if(dotCount!=1){
                    return false;
                }
                for(int i=0;i<opOne[1].length();i++){
                    if(!((opOne[1].charAt(i)>='A' && opOne[1].charAt(i)<='Z') || (opOne[1].charAt(i)>='a' && opOne[1].charAt(i)<='z'))){
                        return false;
                    }
                }
                String [] opTwo=opOne[0].split("@");
                if(opTwo.length!=2){
                    return false;
                }
                for(int i=0;i<opTwo[1].length();i++){
                    if(!((opTwo[1].charAt(i)>='A' && opTwo[1].charAt(i)<='Z') || (opTwo[1].charAt(i)>='a' && opTwo[1].charAt(i)<='z'))){
                        return false;
                    }
                }
                for(int i=0;i<opTwo[0].length();i++){
                    if(!((opTwo[0].charAt(i)>='A' && opTwo[0].charAt(i)<='Z') || (opTwo[0].charAt(i)>='a' && opTwo[0].charAt(i)<='z') || (opTwo[0].charAt(i)=='_') || (opTwo[0].charAt(i)>='0' && opTwo[0].charAt(i)<='9'))){
                        return false;
                    }
                }
                return true;
            } catch(Exception e){
                System.out.println(e.toString());
            }
            return false;
        }
        public  ReadAndInsertData(int threadNo,List<String> dataRows){
            this.threadNo=threadNo;
            this.dataRows=dataRows;
        }
        @Override
        public void run(){
            try {                   
                    System.out.println("Thread import "+threadNo+" started... total data= "+dataRows.size());
                    connection = DriverManager.getConnection(databaseConnectionURL);
                    statement = connection.createStatement();

                    for(int i=0;i<dataRows.size();i++){
                               String [] singleRow=dataRows.get(i).split(",");
                               boolean isValid;
                               if(singleRow.length>=8){
                                   isValid=CheckPhoneNo(singleRow[5]);
                                   if(isValid){
                                       isValid=CheckEmailId(singleRow[6]);
                                   }
                                   //need to add data length check
                                   String flag=(isValid?"1":"0");
                                   statement.execute("exec InsertCustomer '"+flag+"','"+singleRow[0]+"','"+singleRow[1]+"','"+singleRow[2]+"','"+singleRow[3]+"','"+singleRow[4]+"','"+singleRow[5]+"','"+singleRow[6]+"','"+singleRow[7]+"'");  
                               } else{
                                   invalidLengthcounter++;
                               }
                    }  
                    System.out.println("Length <8 found: "+invalidLengthcounter);
                    
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            System.out.println("Thread import "+threadNo+" exiting...");
            counterInsertThreadExit++;
        }     
    }
    private class ReadAndExportValidData extends Thread {
        int threadNo;
        int minIndex;
        int maxIndex;
        int maxLinePerFile=10000;
        public ReadAndExportValidData(int threadNo,int minIndex,int maxIndex){
            this.threadNo=threadNo;
            this.minIndex=minIndex;
            this.maxIndex=maxIndex;
        }
        @Override
        public void run(){
            System.out.println("Thread export valid data "+threadNo+" started... minIndex:"+minIndex+" maxIndex:"+maxIndex);
            try{
                Statement statement;
                Connection connection = null;
                
                connection = DriverManager.getConnection(databaseConnectionURL);
                statement = connection.createStatement();
                ResultSet resultSet=statement.executeQuery("exec SelectCustomer '1','"+String.valueOf(minIndex)+"','"+String.valueOf(maxIndex)+"'");
                while(resultSet.next()){
                    writeValidDataToFile(resultSet.getString("FieldOne")+","+resultSet.getString("FieldTwo")+","+resultSet.getString("FieldThree")+","+resultSet.getString("FieldFour")+","+
                                         resultSet.getString("FieldFive")+","+resultSet.getString("PhoneNo")+","+resultSet.getString("Email")+","+resultSet.getString("IP"));
                }   
            } catch(Exception e){
                System.out.println(e.toString());
            }
            System.out.println("Thread export valid data "+threadNo+" exited...");
        }  
    }
    private class ReadAndExportInvalidData extends Thread {
        int threadNo;
        int minIndex;
        int maxIndex;
        int maxLinePerFile=10000;
        public ReadAndExportInvalidData(int threadNo,int minIndex,int maxIndex){
            this.threadNo=threadNo;
            this.minIndex=minIndex;
            this.maxIndex=maxIndex;
        }
        @Override
        public void run(){
            System.out.println("Thread export invalid data "+threadNo+" started... minIndex:"+minIndex+" maxIndex:"+maxIndex);
            try{
                Statement statement;
                Connection connection = null;
                
                connection = DriverManager.getConnection(databaseConnectionURL);
                statement = connection.createStatement();
                ResultSet resultSet=statement.executeQuery("exec SelectCustomer '0','"+String.valueOf(minIndex)+"','"+String.valueOf(maxIndex)+"'");
                while(resultSet.next()){
                    writeInvalidDataToFile(resultSet.getString("FieldOne")+","+resultSet.getString("FieldTwo")+","+resultSet.getString("FieldThree")+","+resultSet.getString("FieldFour")+","+
                                         resultSet.getString("FieldFive")+","+resultSet.getString("PhoneNo")+","+resultSet.getString("Email")+","+resultSet.getString("IP"));
                }   
            } catch(Exception e){
                System.out.println(e.toString());
            }
            System.out.println("Thread export invalid data "+threadNo+" exited...");
        }  
    }
    private synchronized void writeValidDataToFile(String line){
        try{
            File file=null;
            for(int i=1;;i++){
                if(isCreatedValidFile[i-1]){
                    continue;
                }             
                String fileName="ValidFile_"+i+".txt";
                 try{
                     file = new File(FILE_EXPORT_LOCATION+"//ValidFiles//"+fileName);
                     if(!file.getParentFile().exists()){
                         file.getParentFile().mkdirs();
                     }
                 } catch(Exception e){
                     System.out.println("Cannot create directory in lication "+FILE_EXPORT_LOCATION+" : "+e.getMessage());
                     return;
                 }
                 if (!file.exists()) {
                    try {
                        file.createNewFile();   
                    } catch (IOException e) {
                        System.out.println(e.toString());
                    }
                    validDataWriteCount=0;
                     break;
                 } else {
                     if(MAX_LINE_PER_FILE>validDataWriteCount){
                        break;
                    } else {
                        isCreatedValidFile[i-1]=true;
                     }
                 }
            }
            BufferedWriter bufferedWriter=null;
            try{
                bufferedWriter=new BufferedWriter(new FileWriter(file,true));
                bufferedWriter.append(line);
                bufferedWriter.newLine();
                validDataWriteCount++;
            } catch(Exception e){
                System.out.println(e.toString());
            } finally{
                if(bufferedWriter!=null)
                    bufferedWriter.close();
            }
        } catch(Exception e){
            System.out.println(e.toString());
        }
    }
    private synchronized void writeInvalidDataToFile(String line){
        try{
            File file=null;
            for(int i=1;;i++){
                if(isCreatedInvalidFile[i-1]){
                    continue;
                }             
                String fileName="InvalidFile_"+i+".txt";
                 try{
                     file = new File(FILE_EXPORT_LOCATION+"//InvalidFiles//"+fileName);
                     if(!file.getParentFile().exists()){
                         file.getParentFile().mkdirs();
                     }
                 } catch(Exception e){
                     System.out.println("Cannot create directory in lication "+FILE_EXPORT_LOCATION+" : "+e.getMessage());
                     return;
                 }
                 if (!file.exists()) {
                    try {
                        file.createNewFile();   
                    } catch (IOException e) {
                        System.out.println(e.toString());
                    }
                    invalidDataWriteCount=0;
                     break;
                 } else {
                     if(MAX_LINE_PER_FILE>invalidDataWriteCount){
                        break;
                    } else {
                        isCreatedInvalidFile[i-1]=true;
                     }
                 }
            }
            BufferedWriter bufferedWriter=null;
            try{
                bufferedWriter=new BufferedWriter(new FileWriter(file,true));
                bufferedWriter.append(line);
                bufferedWriter.newLine();
                invalidDataWriteCount++;
            } catch(Exception e){
                System.out.println(e.toString());
            } finally{
                if(bufferedWriter!=null)
                    bufferedWriter.close();
            }
        } catch(Exception e){
            System.out.println(e.toString());
        }
    }
}