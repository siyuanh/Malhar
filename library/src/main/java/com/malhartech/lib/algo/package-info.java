/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>com.malhartech.lib.algo</b> is a library of algorithmic modules for reuse<p>
 * <br>
 * <br>The modules are<br>
 * <b>{@link com.malhartech.lib.algo.CompareCount}</b>: Takes in one stream via input port "data". A count is done on how many tuples satisfy the compare function. The function is given by
 * "key", "value", and "compare". If a tuple passed the test count is incremented. On end of window count iss emitted on the output port "count". The comparison is done by getting double value from the Number.<br>
 *
 * <b>{@link com.malhartech.lib.algo.Distinct}</b>: Takes a stream via input port "data" and emits distinct key,val pairs (i.e drops duplicates) on output port "distinct". Restarts at end of window boundary<br>
 * <b>{@link com.malhartech.lib.algo.FilterKeys}</b>: Takes a stream on input port "in_data", and outputs only keys specified by property "keys" on put output port out_data. If
 * property "inverse" is set to "true", then all keys except those specified by "keys" are emitted<br>
 * <b>{@link com.malhartech.lib.algo.FirstMatch}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "first". The comparison is done by getting double value from the Number. Both output ports are optional, but at least one has to be connected<br>
 * <b>{@link com.malhartech.lib.algo.FirstN}</b>: Takes in one stream via input port "data". Takes the first N tuples of a particular key and emits them as they come in on output port "first"<br>
 * <b>{@link com.malhartech.lib.algo.GroupBy}</b>: Takes two streams via input port "in_data1" and "in_data2", and outputs GroupBy property "Key" on output port out_data<br>
 * <b>{@link com.malhartech.lib.algo.InvertIndexArray}</b>: Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index"<br>
 * <b>{@link com.malhartech.lib.algo.InvertIndexMap}</b>: Takes in a stream via input port "data". Inverts the index and sends out the tuple on output port "index". Takes in specific queries on query port and outputs the data in the cache through console port on receiving the tuple and on each subsequent end_of_window tuple<br>
 * <b>{@link com.malhartech.lib.algo.LastMatch}</b>: Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". Every tuple
 * is checked and the last one that passes the condition is send during end of window on port "last". The comparison is done by getting double
 * value from the Number<br>
 * <b>{@link com.malhartech.lib.algo.LeastFrequentKey}</b>: Takes in one stream via input port "data". Occurrences of each key is counted and at the end of window the least frequent key is emitted on output port "count"<br>
 * <b>{@link coma.malhartech.lib.algo.LeastFrequentKeyValue}</b>: Takes in one stream via input port "data". Occurrences of all values for each key is counted and at the end of window the least frequent value is emitted
 * on output port "count" per key<br>
 * <b>{@link com.malhartech.lib.algo.MatchAll}</b>: Takes in one stream via input port "data". Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "compare". If all tuples passes a Boolean(true) is emitted, else a Boolean(false) is emitted on end of window on the output port "all"<br>
 * <b>{@link com.malhartech.lib.algo.MatchAny}</b>: Takes in one stream via input port "data". Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "compare". If any tuple passes a Boolean(true) is emitted, else a Boolean(false) is emitted on the output port "any". The comparison is done
 * by getting double value from the Number.<br>
 * <b>{@link com.malhartech.lib.algo.MostFrequentKey}</b>: Takes in one stream via input port "data". Occurrences of each key is counted and at the end of window the most frequent key is emitted on output port "count"<p>
 *  This module is an end of window module<br>
 * <b>{@link com.malhartech.lib.algo.MostFrequentKeyValue}</b>: Takes in one stream via input port "data". Occurrences of all values for each key is counted and at the end of window the most frequent value is emitted
 * on output port "count" per key<br>
 * <b>{@link com.malhartech.lib.algo.ReverseIndex}</b>: Takes a stream via input port "data" and emits the reverse index on output port index<br>
 * <b>{@link com.malhartech.lib.algo.ReverseIndexAppend}</b>:
 * <b>{@link com.malhartech.lib.algo.Sampler}</b>: Takes a stream via input port "data" and emits sample tuple on output port out_data<br>
 * <b>{@link com.malhartech.lib.algo.TupleQueue}</b>: * Takes in one stream via input port <b>data</b>. The data is key, value pair. It retains the last N values on that key. Output port gets the last N values. The node also provides a lookup via port <b>lookup</b><br>
 * <b>{@link com.malhartech.lib.algo.WindowedHolder}</b>: TBD<br>
 * <b>{@link com.malhartech.lib.algo.WindowTopCounter}</b>: TBD<br>
 * <br>
 */

package com.malhartech.lib.algo;
