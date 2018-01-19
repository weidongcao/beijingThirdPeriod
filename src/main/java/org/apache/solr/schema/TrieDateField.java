/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.schema;

import org.apache.lucene.index.IndexableField;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.update.processor.TimestampUpdateProcessorFactory;
import org.apache.solr.util.DateMathParser;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 重写Solr的toExternal方法,解决Solr的UTC时区的问题
 *
 */
public class TrieDateField extends TrieField implements DateValueFieldType {
    {
        this.type = TrieTypes.DATE;
    }

    @Override
    public Date toObject(IndexableField f) {
        return (Date) super.toObject(f);
    }

    @Override
    public Object toNativeType(Object val) {
        if (val instanceof String) {
            return DateMathParser.parseMath(null, (String) val);
        }
        return super.toNativeType(val);
    }

    @Override
    public String toExternal(IndexableField f) {
        final DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        format.setTimeZone(SolrRequestInfo.getRequestInfo().getClientTimeZone());
        return format.format((Date) toObject(f));
    }
}
