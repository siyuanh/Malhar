/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.security.jetty;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.eclipse.jetty.plus.jaas.callback.ObjectCallback;

import com.datatorrent.lib.security.SecurityContext;
import com.datatorrent.lib.security.auth.callback.DefaultCallbackHandler;

/**
 * A callback handler to use with Jetty login module for gateway authentication
 *
 * @since 2.0.0
 */
public class JettyJAASCallbackHandler extends DefaultCallbackHandler
{
  @Override
  protected void processCallback(Callback callback) throws IOException, UnsupportedCallbackException
  {
    if (callback instanceof ObjectCallback) {
      ObjectCallback objcb = (ObjectCallback) callback;
      objcb.setObject(context.getValue(SecurityContext.PASSWORD));
    } else {
      super.processCallback(callback);
    }
  }
}
