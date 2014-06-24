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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
//#include <time.h>
#include <sys/file.h>
#include <stdbool.h>
//#include <sys/epoll.h>
//#include <sys/eventfd.h>
#include <unistd.h>
//#include <string.h>

#include "../cass_dio.h"

// we may want something less hard-coded, but this seems to be the accepted value in linux-land
#define ALIGNMENT 512

JNIEXPORT jobject JNICALL 
Java_org_apache_cassandra_utils_CLibrary_allocateBuffer(JNIEnv *env, jobject class, jlong size)
{
    if (size % ALIGNMENT)
    {
        return NULL;
    }

    void *buffer = 0;
    if (posix_memalign(&buffer, ALIGNMENT, size))
    {
        return NULL;
    }

    memset(buffer, 0, (size_t)size);
    jobject jbuffer = (*env)->NewDirectByteBuffer(env, buffer, size);
    return jbuffer;
}

JNIEXPORT jint JNICALL 
Java_org_apache_cassandra_utils_CLibrary_destroyBuffer(JNIEnv *env, jobject class, jobject buffer)
{
    if (buffer == 0)
    {
        return JNI_EINVAL;
    }
    void *buf = (*env)->GetDirectBufferAddress(env, buffer);
    free(buf);
    return 0;
}

JNIEXPORT jlong JNICALL 
Java_org_apache_cassandra_utils_CLibrary_filesize0(JNIEnv *env, jobject class, jint fd)
{
    struct stat statBuffer;
    if (fstat(fd, &statBuffer))
    {
        return JNI_EINVAL;
    }
    return statBuffer.st_size;
}

JNIEXPORT jint JNICALL
Java_org_apache_cassandra_utils_CLibrary_pread0(JNIEnv *env, jobject class, jint fd, jobject buffer, jint size, jlong offset)
{
    void *b = (*env)->GetDirectBufferAddress(env, buffer);
    if (!b)
    {
        return JNI_ENOMEM;
    }

    int status = pread(fd, b, size, offset);
    if (status < 0)
    {
        return -errno;
    }
    return status;
}

