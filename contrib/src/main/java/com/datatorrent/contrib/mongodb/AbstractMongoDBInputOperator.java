/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.mongodb;

import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.db.AbstractStoreInputOperator;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import org.slf4j.LoggerFactory;

/**
 * MongoDB input adapter operator, which send query data from database.<p><br>
 * <p/>
 * <br>
 * Ports:<br>
 * <b>Input</b>: no input port <br>
 * <b>Output</b>: can have one output port<br>
 * <br>
 * Properties:<br>
 * <b>table</b>: the collection which query is get from<br>
 * <b>query</b>:the query object which can has any condition the user wants<br>
 * <b>resultCursor</b>:the result cursor that the query is returned<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None <br>
 * <br>
 * <b>Benchmarks</b>:
 * <br>
 *
 * @since 0.3.2
 */
public abstract class AbstractMongoDBInputOperator<T> extends AbstractStoreInputOperator<T, MongoDBStore>
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AbstractMongoDBInputOperator.class);
  private String table;
  private DBObject query;
  private transient DBCursor resultCursor;
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * Any concrete class derived from this has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   * It converts DBCursor into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param message
   */
  public abstract T getTuple(DBObject result);

  /**
   * query from collection
   */
  @Override
  public void emitTuples()
  {
    resultCursor = store.getCollection(table).find(query);
    while(resultCursor.hasNext()){
      outputPort.emit(getTuple(resultCursor.next()));
    }
    
  }

  public String getTable()
  {
    return table;
  }

  public void setTable(String table)
  {
    this.table = table;
  }

  public DBObject getQuery()
  {
    return query;
  }

  public void setQuery(DBObject query)
  {
    this.query = query;
  }
}
