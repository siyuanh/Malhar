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
package com.datatorrent.lib.algo;

import java.util.HashMap;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

import com.datatorrent.lib.util.BaseMatchOperator;

/**
 * This operator filters the incoming stream of key value pairs by obtaining the values corresponding to a specified key,
 * and comparing those values to a specified number.&nbsp;The first key value pair, in each window, to satisfy the comparison is emitted.
 * <p>
 * All key,val pairs with val of type String are emitted till the first match;  A compare metric is done based on the property "key", "value",
 * and "cmp". Then on no tuple is emitted in that window. The comparison is done by getting double value of the Number.
 * </p>
 * <p>
 * This module is a pass through<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are processed in current window. <br>
 * <b>Partitions : No, </b>will yield wrong results. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input port, expects HashMap&lt;K,String&gt;<br>
 * <b>first</b>: Output port, emits HashMap&lt;K,String&gt; if compare function returns true<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * Compile time checks<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * <b>Specific compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * <br>
 * </p>
 *
 * @displayName Emit Keyval Pairs Until Match (String)
 * @category Algorithmic
 * @tags filter, key value, string
 *
 * @since 0.3.2
 */
@OperatorAnnotation(partitionable = false)
public class FirstTillMatchString<K> extends BaseMatchOperator<K,String>
{
  /**
   * Tuple emit flag.
   */
  boolean emitted = false;

  /**
   * The input port on which incoming key value pairs are received.
   */
  public final transient DefaultInputPort<HashMap<K, String>> data = new DefaultInputPort<HashMap<K, String>>()
  {
    /**
     * Compares the key,val pair with the match condition. Till a match is found tuples are emitted.
     * Once a match is found, state is set to emitted, and no more tuples are compared (no more emits).
     */
    @Override
    public void process(HashMap<K, String> tuple)
    {
      if (emitted) {
        return;
      }
      String val = tuple.get(getKey());
      double tvalue = 0;
      if (val == null) { // skip if key does not exist
        return;
      }
      boolean error = false;
      try {
        tvalue = Double.valueOf(val);
      }
      catch (NumberFormatException e) {
        error = true;
      }
      if (!error) {
        if (compareValue(tvalue)) {
          emitted = true;
        }
      }
      if (!emitted) {
        first.emit(cloneTuple(tuple));
      }
    }
  };

  /**
   * The output port on which key value pairs are emitted until the first match.
   */
  public final transient DefaultOutputPort<HashMap<K, String>> first = new DefaultOutputPort<HashMap<K, String>>();

  /**
   * Emitted set is reset to false
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    emitted = false;
  }
}
