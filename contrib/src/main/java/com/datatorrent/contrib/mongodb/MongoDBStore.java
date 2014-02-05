package com.datatorrent.contrib.mongodb;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.db.Connectable;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MongoDBStore implements Connectable
{
  
  /**
   * hostname in the format "host1:port1;host2:port2..."
   */
  @NotNull
  private String hostName;
  private String dataBase;

  private String userName;
  private String passWord;
  private transient MongoClient mongoClient;
  private transient DB db;
  private transient DBCollection maxWindowCollection;
  
  private boolean isConnected;
  
  private String maxWindowTable;
  private String windowIdColumnName;
  private String operatorIdColumnName;
  private String appIdNameColumnName;
  

  @Override
  public void connect() throws IOException
  {
    try {
      List<ServerAddress> sas = new LinkedList<ServerAddress>();
      for (String name : hostName.split(";")) {
        String[] nameParts = name.split(":");
        if(nameParts.length==1){
          sas.add(new ServerAddress(nameParts[0]));
        } else {
          sas.add(new ServerAddress(nameParts[0], Integer.parseInt(nameParts[1])));
        }
      }
      mongoClient = new MongoClient(sas);
      db = mongoClient.getDB(dataBase);
      if (userName != null && passWord != null) {
        isConnected = db.authenticate(userName, passWord.toCharArray());
      }
      maxWindowCollection = db.getCollection(maxWindowTable);
    } catch (Exception e) {
      throw new RuntimeException("creating mongodb client", e);
    }
  }

  @Override
  public void disconnect() throws IOException
  {
    mongoClient.close();
    isConnected = false;
  }

  @Override
  public boolean isConnected()
  {
    return isConnected;
  }

  public DBCollection getCollection(String tableName)
  {
    if(isConnected){
     return db.getCollection(tableName);
    } else {
      throw new RuntimeException("mongodb is not connected");
    }
  }

  public long getOrInitializeLastWindowId(String appIdName, int operatorId)
  {
    BasicDBObject query = new BasicDBObject();
    query.put(operatorIdColumnName, operatorId);
    query.put(appIdNameColumnName, appIdName);
    DBCursor cursor = maxWindowCollection.find(query);
    try {
      if (cursor.hasNext()) {
        Object obj = cursor.next().get(windowIdColumnName);
        return (Long) obj;
      } else {
        // last window id not found for this app operator
        BasicDBObject doc = new BasicDBObject();
        doc.put(windowIdColumnName, (long)0);
        doc.put(appIdNameColumnName, appIdName);
        doc.put(operatorIdColumnName, operatorId);
        maxWindowCollection.save(doc);
        return 0;
      }
    } finally {
      if(cursor!=null){
        cursor.close();
      }
    }
  }

  public void insertLastWindowId(String appIdName, int operatorId, long windowId)
  {
    BasicDBObject doc = new BasicDBObject();
    doc.put(windowIdColumnName, (long)0);
    doc.put(appIdNameColumnName, appIdName);
    doc.put(operatorIdColumnName, operatorId);
    maxWindowCollection.save(doc);
  }

  public void updateLastWindowId(String appIdName, int operatorId, long windowId)
  {
    BasicDBObject where = new BasicDBObject(); // update maxWindowTable for windowId information
    where.put(operatorIdColumnName, operatorId);
    where.put(appIdNameColumnName, appIdName);
    BasicDBObject value = new BasicDBObject();
    value.put(windowIdColumnName, windowId);
    maxWindowCollection.findAndModify(where, value); 
  }
  
  public String getWindowIdColumnName()
  {
    return windowIdColumnName;
  }

  public void setWindowIdColumnName(String windowIdColumnName)
  {
    this.windowIdColumnName = windowIdColumnName;
  }

  public String getOperatorIdColumnName()
  {
    return operatorIdColumnName;
  }

  public void setOperatorIdColumnName(String operatorIdColumnName)
  {
    this.operatorIdColumnName = operatorIdColumnName;
  }
  
  public void setMaxWindowTable(String maxWindowTable)
  {
    this.maxWindowTable = maxWindowTable;
  }
  
  public String getMaxWindowTable()
  {
    return maxWindowTable;
  }
  
  public void setHostname(String hostname){
    this.hostName = hostname;
  }
  
  public String getHostname(){
    return hostName;
  }
  
  public void setPassWord(String passWord)
  {
    this.passWord = passWord;
  }
  
  public String getPassWord()
  {
    return passWord;
  }
  
  public void setUserName(String userName)
  {
    this.userName = userName;
  }
  
  public String getUserName()
  {
    return userName;
  }
  
  public String getAppIdNameColumnName()
  {
    return appIdNameColumnName;
  }
  
  public void setAppIdNameColumnName(String appIdNameColumnName)
  {
    this.appIdNameColumnName = appIdNameColumnName;
  }
  
  public void setDataBase(String dataBase)
  {
    this.dataBase = dataBase;
  }
  
  public String getDataBase()
  {
    return dataBase;
  }
  

}
