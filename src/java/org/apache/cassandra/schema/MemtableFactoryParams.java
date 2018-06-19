/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.schema;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.db.MemtableFactory;
import org.apache.cassandra.db.StandardMemtableFactory;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

public class MemtableFactoryParams
{
    // if we ever add more 'standard' options, can turn this into an enum like CompactionParams
    private static final String OPTIONS_CLASS = "class";

    public static final MemtableFactoryParams DEFAULT = new MemtableFactoryParams(StandardMemtableFactory.class,
                                                                                  Collections.emptyMap());

    private final Class<? extends MemtableFactory> factoryClass;
    private final Map<String, String> options;

    private MemtableFactoryParams(Class<? extends MemtableFactory> factoryClass, Map<String, String> options)
    {
        this.factoryClass = factoryClass;
        this.options = options;
    }

    public static MemtableFactoryParams create(Class<? extends MemtableFactory> klass, Map<String, String> map)
    {
        return new MemtableFactoryParams(klass, map);
    }

    public static MemtableFactoryParams fromMap(Map<String, String> map)
    {
        Map<String, String> options = new HashMap<>(map);

        String className = options.remove(OPTIONS_CLASS);
        if (className == null)
        {
            throw new ConfigurationException(format("Missing sub-option '%s' for the '%s' option",
                                                    OPTIONS_CLASS,
                                                    TableParams.Option.MEMTABLE_FACTORY));
        }
        return create(classFromName(className), options);
    }

    public Map<String, String> asMap()
    {
        Map<String, String> map = new HashMap(options);
        map.put(OPTIONS_CLASS, factoryClass.getName());
        return map;
    }

    public static Class<? extends MemtableFactory> classFromName(String name)
    {
        String className = name.contains(".")
                           ? name
                           : "org.apache.cassandra.db." + name;

        Class<MemtableFactory> factoryClass = FBUtilities.classForName(className, "memtable factory class");

        if (!MemtableFactory.class.isAssignableFrom(factoryClass))
        {
            throw new ConfigurationException(format("Memtable factory class %s is not derived from MemtableFactory",
                                                    className));
        }

        return factoryClass;
    }

    public void validate()
    {
        // currently a no-op (as the class type is checked earlier),
        // but follows the pattern of CompactionParams/CompressionParams
    }

    public MemtableFactory createInstance()
    {
        try
        {
            Constructor<? extends MemtableFactory> constructor = factoryClass.getConstructor(Map.class);
            return constructor.newInstance(options);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Class<? extends MemtableFactory> getFactoryClass()
    {
        return factoryClass;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("class", factoryClass.getName())
                          .add("options", options)
                          .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof MemtableFactoryParams))
            return false;

        MemtableFactoryParams params = (MemtableFactoryParams) o;

        return factoryClass.equals(params.factoryClass) && options.equals(params.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(factoryClass, options);
    }
}
