/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.db;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

/**
 * An {@link AbstractStoreOutputOperator} which works with {@link WindowAware} stores that need the
 * committed window id when they startup.
 *
 * @param <T> type of the tuple
 * @param <S> store type
 */
public abstract class AbstractWindowAwareOutputOperator<T, S extends Connectable> extends AbstractStoreOutputOperator<T, S>
{
  protected long processedWindowId = -1;
  protected transient String appId;
  protected transient Integer operatorId;
  protected transient long currentWindowId = -1;

  public AbstractWindowAwareOutputOperator()
  {
    processedWindowId = -1;
    currentWindowId = -1;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
  }
  
  @Override
  public void endWindow()
  {
    super.endWindow();
    processedWindowId = currentWindowId;
  }
}
