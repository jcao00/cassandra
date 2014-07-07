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

#include <jni.h>
#include <libaio.h>
#ifndef CASSANDRA_DIO_INCLUDE
#define CASSANDRA_DIO_INCLUDE

#ifdef __cplusplus
extern "C" {
#endif


/* native buffer ops */
JNIEXPORT jobject JNICALL Java_org_apache_cassandra_io_util_DirectReader_allocateNativeBuffer(JNIEnv *env, jobject class, jlong size);
JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_util_DirectReader_destroyNativeBuffer(JNIEnv *env, jobject class, jobject buffer);

/* file ops */
JNIEXPORT jlong JNICALL Java_org_apache_cassandra_io_util_DirectReader_open0(JNIEnv *env, jobject class, jstring file_name);
JNIEXPORT jlong JNICALL Java_org_apache_cassandra_io_util_DirectReader_close0(JNIEnv *env, jobject class, jint fd);

JNIEXPORT jlong JNICALL Java_org_apache_cassandra_io_util_DirectReader_filesize0(JNIEnv *env, jobject class, jint fd);
JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_util_DirectReader_pread0(JNIEnv *env, jobject class, jint fd, jobject buffer, jint size, jlong offset);


#ifdef __cplusplus
}
#endif
#endif
