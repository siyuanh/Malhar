/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.aerospike;

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * <p>
 * A generic implementation of {@link AbstractAerospikeNonTransactionalPutOperator} which can
 * take a POJO.
 * </p>
 * @displayName Aerospike Non-Transactional Put
 * @category Database
 * @tags output operator, put, non-transactional, POJO
 * @since 2.1.0
 */
public class AerospikePOJONonTransactionalPutOperator extends AbstractAerospikeNonTransactionalPutOperator<Object>
{
  private static transient final Logger LOG
    = LoggerFactory.getLogger(AerospikePOJONonTransactionalPutOperator.class);

  // Two element list; first retrieves the record key and second the list of bins in this tuple
  @NotNull
  private ArrayList<String> expressions;

  private transient Getter<Object, Key> keyGetter;
  private transient Getter<Object, List> binsGetter;

  // required by App Builder
  public AerospikePOJONonTransactionalPutOperator()
  {
  }

  /*
   * Two Java expressions that will yield the key and the list of modified Bins
   * for the destination record of this tuple
   * Example: {"getKey()", "getBins()"}
   */
  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  /*
   * Set field retrieval list of expressions.
   * @param ArrayList of field retrieval expressions
   */
  public void setExpressions(ArrayList<String> e)
  {
    this.expressions = e;
  }

  @Override
  protected Key getUpdatedBins(Object tuple, List<Bin> list)
  {
    if (null == keyGetter) {    // first tuple
      Class<?> tupleClass = tuple.getClass();
      keyGetter  = PojoUtils.createGetter(tupleClass, expressions.get(0), Key.class);
      binsGetter = PojoUtils.createGetter(tupleClass, expressions.get(1), List.class);
    }
    Key key = keyGetter.get(tuple);
    List<Bin> binList = binsGetter.get(tuple);
    if ( ! (null == binList || binList.isEmpty()) ) {
      list.addAll(binList);
    }
    return key;
  }

}
