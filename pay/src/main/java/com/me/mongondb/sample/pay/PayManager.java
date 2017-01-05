package com.me.mongondb.sample.pay;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.ne;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;

public class PayManager {

	private static final String DBNAME = "demo";
	private static final String ACCOUNTS = "accounts";
	private static final String TRANSACTIONS = "transactions";

	private static MongoClient cli = null;

	{
		//cli = new MongoClient("127.0.0.1", 27017);
		String host="10.19.156.250";
		int port = 27017;
		String userName="zhongyi";
		String password="zhongyi";
	    String authenDB="demo";
		
	    ServerAddress addr=new ServerAddress(host,port);
		List<MongoCredential> credentialsList=new ArrayList<MongoCredential>();
		
		MongoCredential credential =MongoCredential.createCredential(userName,authenDB,password.toCharArray());
		credentialsList.add(credential);
		
		cli=new MongoClient(addr, credentialsList);
	}

	public static void main(String[] args) {
		try {
			PayManager pm=new PayManager();
			System.out.println(pm.getISODate(new Date()));

//			pm.initAccounts();
//			
//			String orderId=pm.getOrderId();
//			double amt=300;
//			String source="A";
//			String dest="B";
//			
//			pm.transfer(orderId, amt, source, dest);
		} finally {
			cli.close();
		}	
	}
	
	public String getOrderId() {
		return ObjectId.get().toString();
	}

	public void initAccounts() {
		MongoCollection<Document> mc = cli.getDatabase(DBNAME).getCollection(ACCOUNTS);
		mc.drop();

		List<Document> documents = new ArrayList<Document>();
		Document _d = new Document();
		_d.append("_id", "A").append("balance", 1000).append("pendingTransactions",new ArrayList<String>());
		System.out.println(_d.toJson());
		documents.add(_d);
		
		_d = new Document();
		_d.append("_id", "B").append("balance", 1000).append("pendingTransactions", new ArrayList<String>());
		documents.add(_d);
		
		mc.insertMany(documents);
	}

	public void transfer(String orderId, double amt, String source, String dest) {
		MongoCollection<Document> transCollection = cli.getDatabase(DBNAME).getCollection(TRANSACTIONS);
		Document existRecord = transCollection.find(eq(orderId)).first();

		if (null != existRecord) {
			if (existRecord.getString("state").equals(TransactionState.Done)) {
				System.out.println(String.format("duplicate orderid:%s,and order state:", orderId,
						existRecord.getString("state")));
			}
		} else {
			transCollection.insertOne(new Document().append("_id", orderId).append("source", source)
					.append("destination", dest).append("state", TransactionState.Initial).append("value", amt)
					.append("lastModified", new Date()));
			BasicDBObject bd = new BasicDBObject();
			
			this.prepareTransfer(orderId, amt, source, dest);
			this.doTransfer(orderId, amt, source, dest);
		}

	}

	public void doTransfer(String orderId, double amt, String source, String dest) {
		MongoCollection<Document> transCollection = cli.getDatabase(DBNAME).getCollection(TRANSACTIONS);
		MongoCollection<Document> accountsCollection = cli.getDatabase(DBNAME).getCollection(ACCOUNTS);

		accountsCollection.findOneAndUpdate(and(eq(source), ne("pendingTransactions", orderId)),
				new Document().append("$inc", new Document().append("balance", 0 - amt)).append("$push",
						new Document().append("pendingTransactions", orderId)));

		accountsCollection.findOneAndUpdate(and(eq(dest), ne("pendingTransactions", orderId)),
				new Document().append("$inc", new Document().append("balance", amt)).append("$push",
						new Document().append("pendingTransactions", orderId)));

		transCollection.findOneAndUpdate(and(eq(orderId), eq("state", TransactionState.Pendding)),
				new Document().append("$set", new Document().append("state", TransactionState.Applied))
						.append("$currentDate", new Document().append("lastModified", true)));

	}

	public void prepareTransfer(String orderId, double amt, String source, String dest) {
		MongoCollection<Document> transCollection = cli.getDatabase(DBNAME).getCollection(TRANSACTIONS);
		Bson filter = and(eq(orderId), eq("state", TransactionState.Initial));

		Document updateDoc = new Document().append("$set", new Document().append("state", TransactionState.Pendding))
				.append("$currentDate", new Document().append("lastModified", true));
		Document old = transCollection.findOneAndUpdate(filter, updateDoc);

		System.out.println(null != old ? old.toJson()
				: "match nothing, so can't transform state from " + TransactionState.Initial + " to "
						+ TransactionState.Pendding);
	}

	public void afterTransger(String orderId, double amt, String source, String dest) {
		MongoCollection<Document> transCollection = cli.getDatabase(DBNAME).getCollection(TRANSACTIONS);
		MongoCollection<Document> accountsCollection = cli.getDatabase(DBNAME).getCollection(ACCOUNTS);

		accountsCollection.findOneAndUpdate(and(eq(source), ne("pendingTransactions", orderId)),
				new Document().append("$pull", new Document().append("pendingTransactions", orderId)));

		accountsCollection.findOneAndUpdate(and(eq(dest), ne("pendingTransactions", orderId)),
				new Document().append("$pull", new Document().append("pendingTransactions", orderId)));

		transCollection.findOneAndUpdate(and(eq(orderId), eq("state", TransactionState.Applied)),
				new Document().append("$set", new Document().append("state", TransactionState.Done))
						.append("$currentDate", new Document().append("lastModified", true)));

	}
	
	private String getISODate(Date date){
		return date.getYear()+"-"+(date.getMonth()+1)+"-"+date.getDate()+ " "+date.toLocaleString();
	}
}

enum TransactionState {
	Initial("initial"), Pendding("pendding"), Applied("applied"), Done("done"), Cancelling("cancelling"), Cancelled(
			"cancelled");

	private String value;

	private TransactionState(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return value;
	}

}
