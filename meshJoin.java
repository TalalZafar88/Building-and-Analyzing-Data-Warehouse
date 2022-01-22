package meshjoin;
 


import java.sql.*;
import java.util.*;
 

import com.google.common.collect.*;



public class meshJoin {
	
	public static class Transaction {
		
		  int TRANSACTION_ID;
		  String PRODUCT_ID;
		  String CUSTOMER_ID;
		  String CUSTOMER_NAME;
		  String STORE_ID;
		  String STORE_NAME;
		  String T_DATE;
		  int Quantity;
		  
		  public Transaction(int TRANSACTION_ID,String PRODUCT_ID,String CUSTOMER_ID,String CUSTOMER_NAME,String STORE_ID,String STORE_NAME,String T_DATE,int QUANTITY) {
			  
			    this.TRANSACTION_ID = TRANSACTION_ID;
			    this.PRODUCT_ID = PRODUCT_ID;
				this.CUSTOMER_ID = CUSTOMER_ID;
				this.CUSTOMER_NAME =  CUSTOMER_NAME;
				this.STORE_ID = STORE_ID;
				this.STORE_NAME =  STORE_NAME;
				this.T_DATE =  T_DATE;
				this.Quantity = QUANTITY;
			    
			  }
		 
		  
		}
	
	public static class Master  {
		
		  String PRODUCT_ID;
		  String PRODUCT_NAME;
		  String SUPPLIER_ID;
		  String SUPPLIER_NAME;
		  Float PRICE;
		  
		  public Master(String PRODUCT_ID, String PRODUCT_NAME, String SUPPLIER_ID, String SUPPLIER_NAME, float PRICE) {
			  
			    this.PRODUCT_ID = PRODUCT_ID;
				this.PRODUCT_NAME = PRODUCT_NAME;
				this.SUPPLIER_ID =  SUPPLIER_ID;
				this.SUPPLIER_NAME = SUPPLIER_NAME;
				this.PRICE =  PRICE;
				
			    
			  }
		 
		}
	
	public static class Queue_hash {
		
		int TRANSACTION_ID;
		String PRODUCT_ID;
		
		public Queue_hash(int a,String b) {
			
			this.TRANSACTION_ID =a;
			this.PRODUCT_ID = b;
		}
		
	}
	
	public static int quater(String a) {
		
		
		int q = Integer.parseInt(a);
		
		if (q>=1 & q<=3) {
			
			return 1;
		}
		
		else if(q>=4 & q<=6) {
			
			return 2;
			
		}
		
        else if(q>=7 & q<=9) {
			
			return 3;
			
		}
		
        else {
        	
        	return 4;
        }
		
	}
	
	
	public static void InsertDW(Transaction a, Master b,Statement myStmt, ResultSet myRs, Connection con) {
		
		try
		{	  	
		 String sql1 ="INSERT IGNORE INTO PRODUCT VALUES(?,?)";
		 PreparedStatement fq =con.prepareStatement(sql1);
		 fq.setString(1,a.PRODUCT_ID);
		 fq.setString(2,b.PRODUCT_NAME);
		 fq.execute();
		 
		 
		 String sql2 = "INSERT IGNORE INTO STORE VALUES(? , ?)";
		 fq =con.prepareStatement(sql2);
		 fq.setString(1,a.STORE_ID);
		 fq.setString(2,a.STORE_NAME);
		 fq.execute();
		 
		 
		 String sql3 = "INSERT IGNORE INTO SUPPLIER VALUES(? , ?)";
		 fq =con.prepareStatement(sql3);
		 fq.setString(1, b.SUPPLIER_ID);
		 fq.setString(2, b.SUPPLIER_NAME);
		 fq.execute();
		 
		 String sql4 = "INSERT IGNORE INTO CUSTOMER VALUES(?,?)";	
		 fq =con.prepareStatement(sql4);
		 fq.setString(1, a.CUSTOMER_ID);
		 fq.setString(2, a.CUSTOMER_NAME);
		 fq.execute();
		 
		 String[] arrOfStr = a.T_DATE.split("-", 3);
		 int quarter = quater(arrOfStr[1]);
		 String sql5 = "INSERT IGNORE INTO TIME VALUES(?,?,?,?,?)";
		 fq =con.prepareStatement(sql5);
		 fq.setString(1, a.T_DATE);
		 fq.setInt(2, Integer.parseInt(arrOfStr[2]));
		 fq.setInt(3, Integer.parseInt(arrOfStr[1]));
		 fq.setInt(4, quarter);
		 fq.setInt(5, Integer.parseInt(arrOfStr[0]));
		 fq.execute();
		 
		 
		 String sql7 = "SELECT COUNT(*) FROM SALES WHERE CUSTOMER_ID ='"+a.CUSTOMER_ID+"' AND  SUPPLIER_ID= '"+b.SUPPLIER_ID+"' AND PRODUCT_ID='"+a.PRODUCT_ID+"' AND TIME_ID='"+a.T_DATE+"' AND STORE_ID='"+a.STORE_ID+"' ";
		 myRs = myStmt.executeQuery(sql7);
		 int num = 0;
		 while(myRs.next()) 
			{
				
			    num = myRs.getInt("COUNT(*)");
			}
		 
		 if (num>=1) {
		
	     String sql8 = "UPDATE SALES SET QUANTITY = QUANTITY + ?, TOTAL_SALE = TOTAL_SALE + ? WHERE (CUSTOMER_ID = ? AND  SUPPLIER_ID= ? AND PRODUCT_ID= ? AND TIME_ID= ? AND STORE_ID= ?)";
	     	
	     fq =con.prepareStatement(sql8);
		 fq.setInt(1, a.Quantity);
		 fq.setDouble(2, (a.Quantity*b.PRICE));
		 fq.setString(3, a.CUSTOMER_ID);
		 fq.setString(4, b.SUPPLIER_ID);
		 fq.setString(5, a.PRODUCT_ID);
		 fq.setString(6, a.T_DATE);
		 fq.setString(7, a.STORE_ID);
		 fq.execute();

		 
		 }
		 else {
		 String sql6 = "INSERT IGNORE INTO SALES VALUES(?,?,?,?,?,?,?)";
		 
		 fq =con.prepareStatement(sql6);
		 fq.setString(1, a.CUSTOMER_ID);
		 fq.setString(2, b.SUPPLIER_ID);
		 fq.setString(3, a.PRODUCT_ID);
		 fq.setString(4, a.T_DATE);
		 fq.setString(5, a.STORE_ID);
		 fq.setInt(6, a.Quantity);
		 fq.setDouble(7, a.Quantity*b.PRICE);
		 
		 fq.execute();
		 }
		 		
		}
		
		catch (Exception exc) 
		{
			exc.printStackTrace();
		}
		
		
		
	}
	
	
	
	public static void main(String[] args) {
		
	Multimap<String, Transaction> map = ArrayListMultimap.create();

		
	int stream_buffer_size = 50;
	int disk_buffer_size = 10;
	int start = 0;
	int end =  10;
	int fetch1= 1;
	int fetch2= 50;
	boolean flag =false;
    
	
	Transaction streambuffer[]= new Transaction[stream_buffer_size];
	Master temp1[] = null;
	Queue<Queue_hash[]> q = new LinkedList<>();
	Connection myConn = null;
	Statement myStmt = null;
	ResultSet myRs = null;
	

	

	try 
	
	{ 
		
		myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/Project", "root" , "12345");
		
		myStmt = myConn.createStatement();
		
		
		String sql1 = "SELECT * FROM MASTERDATA";
		String sql2 = "SELECT COUNT(*) FROM MASTERDATA";
		String sql3 = "SELECT COUNT(*) FROM TRANSACTIONS";
		
		
		myRs = myStmt.executeQuery(sql2);
		int mastersize=0;
		 		
		while(myRs.next()) 
		{
			
		    mastersize = myRs.getInt("COUNT(*)");
		}
		
		temp1 = new Master[mastersize];
		myRs = myStmt.executeQuery(sql1);
		int i = 0;
		
		while (myRs.next()) 
		{
			
			temp1[i] = new Master(myRs.getString("PRODUCT_ID"),myRs.getString("PRODUCT_NAME"),myRs.getString("SUPPLIER_ID"),myRs.getString("SUPPLIER_NAME"),myRs.getFloat("PRICE"));
			i++;
			
		}
		
		myRs = myStmt.executeQuery(sql3);
		int transize = 0;
		while(myRs.next()) 
		{
			
		    transize = myRs.getInt("COUNT(*)");
		}
		
		//-----------------------------------------------\\
	
		int l =0;
		
		while(l<transize || !(q.isEmpty())  ) {	
		
		Queue_hash trans[] = new Queue_hash[stream_buffer_size];
		
		if(l <transize) 
		
		{
			
		System.out.println(l+" "+"FETCHING");		
	    System.out.println(fetch1+" " +fetch2);	
			
		
		String sql  = "SELECT * FROM TRANSACTIONS WHERE TRANSACTION_ID BETWEEN "+fetch1+" AND "+fetch2+" ";
				
		myRs = myStmt.executeQuery(sql);
		int j = 0;
		
		while (myRs.next()) 
		{
			
			streambuffer[j] = new Transaction(myRs.getInt("TRANSACTION_ID"),myRs.getString("PRODUCT_ID"),myRs.getString("CUSTOMER_ID"),myRs.getString("CUSTOMER_NAME"),myRs.getString("STORE_ID"),myRs.getString("STORE_NAME"),myRs.getString("T_DATE"),myRs.getInt("QUANTITY"));
		    j++;
			
			
		}
		
		
		
		for(int x=0 ; x<streambuffer.length ; ++x) {
			
			trans[x] =new Queue_hash(streambuffer[x].TRANSACTION_ID,streambuffer[x].PRODUCT_ID);
			map.put(streambuffer[x].PRODUCT_ID,streambuffer[x]);
			
		}
		
					
		q.add(trans);
	    

		}
			
		System.out.println("disk buffer => "+start+" " +end);	
		for( ; start< end ;++start) {
			
			
			
			if(map.containsKey(temp1[start].PRODUCT_ID)) {
				
				Collection<Transaction> values = map.get(temp1[start].PRODUCT_ID);
				Iterator<Transaction> iterator = values.iterator(); 
				
				while (iterator.hasNext()) 
				{  
					 Transaction a = iterator.next();
				     InsertDW(a,temp1[start],myStmt,myRs,myConn);
				     			     				     
				}
				
				
			}
			 
			 		
		}
		
		start = 0;
		
		if(end==mastersize) {
			flag= true;
			start = 0;
			end = disk_buffer_size;
			
			
		}
		else {

			start+=end;
			end+=disk_buffer_size;
			
		}
		
		if (flag==true) {
			
		trans = q.poll();
		
		for(int v= 0; v< trans.length; ++v) {
			
		    System.out.println(trans[v].TRANSACTION_ID + " removed");

			Collection<Transaction> values1 = map.get(trans[v].PRODUCT_ID);
			Iterator<Transaction> iterator1 = values1.iterator(); 
			
			while (iterator1.hasNext()) 
			{  
				 Transaction a = iterator1.next();
				 
			     if(trans[v].TRANSACTION_ID == a.TRANSACTION_ID) {
			    	 
			    	 iterator1.remove();
			    		 
			     }
			     			     				     
			}
			
			
		}
		
		}
		
		
		l=l+stream_buffer_size;
		fetch1+= stream_buffer_size;
		fetch2+= stream_buffer_size;
		
	}	
		
				
	}// try 
		
  			
	catch (Exception exc) 
	{
		exc.printStackTrace();
	}

	
	}
	

}
