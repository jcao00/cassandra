#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <jni.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/file.h>
#include <stdbool.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <string.h>

#include "linux_aio.h"

jmethodID callback_method_id = NULL;


jint
JNI_OnLoad(JavaVM *vm, void *reserved)
{
    JNIEnv* env;
    if ((*vm)->GetEnv(vm, (void **) &env, JNI_VERSION_1_6) != JNI_OK)
    {
        return JNI_ERR;
    }
    jclass cls = (*env)->FindClass(env, "org/apache/cassandra/io/aio/AioFileChannel");
    callback_method_id = (*env)->GetMethodID(env, cls, "callback", "(JII)V");
    if (callback_method_id == NULL)
    {
        return JNI_EINVAL;
    }

    return JNI_VERSION_1_6;
}

/* epoll-related functions */
JNIEXPORT jint JNICALL
Java_org_apache_cassandra_io_aio_Native_epollCreate(JNIEnv *env, jobject class)
{
    int efd = epoll_create(128);
    return efd;
}

JNIEXPORT jint JNICALL
Java_org_apache_cassandra_io_aio_Native_epollWait(JNIEnv *env, jobject class, jint efd, jint max)
{
    struct io_event events[max];
//    int num_events = io_getevents(ioctx, 1, 512, events, NULL);
    return 0;
}

JNIEXPORT jint JNICALL
Java_org_apache_cassandra_io_aio_Native_epollClose(JNIEnv *env, jobject class, jint efd)
{
    return close(efd);
}

/*
    buffer-related functions
 */
JNIEXPORT jobject JNICALL
Java_org_apache_cassandra_io_aio_Native_newNativeBuffer(JNIEnv *env, jclass class, jlong size)
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
Java_org_apache_cassandra_io_aio_Native_resetBuffer(JNIEnv *env, jclass class, jobject buffer, jint size)
{
    void *buf = (*env)->GetDirectBufferAddress(env, buffer);
    if (buf == 0)
    {
        return JNI_EINVAL;
    }

    memset(buf, 0, (size_t)size);
    return 0;
}

JNIEXPORT jint JNICALL
Java_org_apache_cassandra_io_aio_Native_destroyBuffer(JNIEnv *env, jclass class, jobject buffer)
{
    if (buffer == 0)
    {
        return JNI_EINVAL;
    }
    void *buf = (*env)->GetDirectBufferAddress(env, buffer);
    free(buf);
    return 0;
}

/*
    aio-related functions
*/
io_context_t *
convert_context(JNIEnv *env, jobject controllerAddress)
{
    return (io_context_t *)(*env)->GetDirectBufferAddress(env, controllerAddress);
}

JNIEXPORT jobject JNICALL
Java_org_apache_cassandra_io_aio_Native_createAioContext(JNIEnv *env, jobject class, jint max_io)
{
    io_context_t *ctx = (io_context_t *)malloc(sizeof(io_context_t));
    if (!ctx)
    {
        return NULL;
    }
    memset(ctx, 0, sizeof(io_context_t));
    int status = io_queue_init(max_io, ctx);
    if (status < 0)
    {
        free(ctx);
        return NULL;
    }

    return (*env)->NewDirectByteBuffer(env, ctx, 0);
}

JNIEXPORT int JNICALL
Java_org_apache_cassandra_io_aio_Native_pollAioEvents(JNIEnv *env, jobject class, jobject ctx_addr, jint max_events, jint max_loops)
{
    io_context_t *ctx = convert_context(env, ctx_addr);
    struct io_event events[max_events];
    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 10;

    //not sure this is necessary...
    memset(events, 0, sizeof(struct io_event) * max_events);
    int evtcnt = 0;
    for (int i = 0; i < max_loops; i++)
    {
        int result = io_getevents(*ctx, 1, max_events, events, &timeout);
        // handle result < 0
        if (result > 0)
        {
            evtcnt += result;
            //fprintf(stdout, "got %d event(s) from io_getevents\n", result);
            for (int i = 0; i < result; i++)
            {
                struct io_event ev = events[i];
                struct iocb *iocb = (struct iocb *)ev.obj;
                struct iocb_refs *refs = (struct iocb_refs *)iocb->data;

                (*env)->CallVoidMethod(env, refs->callback_obj, callback_method_id, refs->id, ev.res, ev.res2);

                (*env)->DeleteGlobalRef(env, refs->callback_obj);
                (*env)->DeleteGlobalRef(env, refs->buffer);
                free(refs);
                free(iocb);
            }
        }
    }
    return evtcnt;
}

JNIEXPORT jint JNICALL
Java_org_apache_cassandra_io_aio_Native_destroyAioContext(JNIEnv *env, jobject class, jobject ctx_addr)
{
    io_context_t *ctx = convert_context(env, ctx_addr);
    int status = io_destroy(*ctx);
    return status;
}


JNIEXPORT jint JNICALL
Java_org_apache_cassandra_io_aio_Native_open0(JNIEnv *env, jobject class, jstring file_name)
{
    const char *name = (*env)->GetStringUTFChars(env, file_name, NULL);
    if (name == NULL)
    {
        return JNI_EINVAL;
    }
    fprintf(stdout, "opening w/ O_DIRECT\n");
    int fd = open(name, O_RDONLY | O_DIRECT, 0666);
    (*env)->ReleaseStringUTFChars(env, file_name, name);

    return fd;
}

JNIEXPORT jlong JNICALL
Java_org_apache_cassandra_io_aio_Native_size0(JNIEnv *env, jobject clazz, jint fd)
{
    struct stat statBuffer;
    if (fstat(fd, &statBuffer) < 0)
    {
        return JNI_EINVAL;
    }
    return statBuffer.st_size;
}

JNIEXPORT jint JNICALL
Java_org_apache_cassandra_io_aio_Native_read0(JNIEnv *env, jobject class, jobject ctx_addr, jobject callback_target, jlong id, jobject dst, jint size, jint fd, jlong position)
{
    io_context_t *ctx = convert_context(env, ctx_addr);
    if (!ctx)
    {
        return JNI_EINVAL;
    }
    struct iocb *iocb = (struct iocb *)malloc(sizeof(struct iocb));
    if (!iocb)
    {
        return JNI_ENOMEM;
    }
    memset(iocb, 0, sizeof(struct iocb));

    void *buf = (*env)->GetDirectBufferAddress(env, dst);
    if (!buf)
    {
        return JNI_ENOMEM;
    }
    else if (((long)buf) % 512)
    {
        return JNI_EINVAL;
    }
/*    else if ((size - buf) % 512)
    {
        return JNI_EINVAL;
        }*/

    io_prep_pread(iocb, fd, buf, size, position);
    struct iocb_refs *refs = (struct iocb_refs *)malloc(sizeof(struct iocb_refs));
    if (!refs)
    {
        free(buf);
        return JNI_ENOMEM;
    }
    memset(refs, 0, sizeof(struct iocb_refs));
    iocb->data = refs;
    refs->callback_obj = (*env)->NewGlobalRef(env, callback_target);
    
    //think i need buf here, not the global ref, but we do need to create the global ref and stash it somewhere (probably in refs)
    refs->buffer = (*env)->NewGlobalRef(env, dst);
    refs->id = id;

    return io_submit(*ctx, 1, &iocb);
}

JNIEXPORT jint JNICALL
Java_org_apache_cassandra_io_aio_Native_close0(JNIEnv *env, jobject class, jint fd)
{
    return close(fd);
}


