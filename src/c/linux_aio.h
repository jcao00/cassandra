#include <jni.h>
#include <libaio.h>
#ifndef CASSANDRA_AIO_INCLUDE
#define CASSANDRA_AIO_INCLUDE

#ifdef __cplusplus
extern "C" {
#endif

#define ALIGNMENT 512

struct iocb_refs {
    jobject callback_obj;
    jobject buffer;
    jlong id;
};


JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_aio_Native_epollCreate(JNIEnv *env, jobject class);
JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_aio_Native_epollWait(JNIEnv *env, jobject class, jint efd, jint max);
JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_aio_Native_epollClose(JNIEnv *env, jobject class, jint efd);

JNIEXPORT jobject JNICALL Java_org_apache_cassandra_io_aio_Native_newNativeBuffer(JNIEnv *env, jclass class, jlong size);
JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_aio_Native_resetBuffer(JNIEnv *env, jclass class, jobject buffer, jint size);
JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_aio_Native_destroyBuffer(JNIEnv *env, jclass class, jobject buffer);

JNIEXPORT jobject JNICALL Java_org_apache_cassandra_io_aio_Native_createAioContext(JNIEnv *env, jobject class, jint max_io);
JNIEXPORT int JNICALL Java_org_apache_cassandra_io_aio_Native_pollAioEvents(JNIEnv *env, jobject class, jobject ctx_addr, jint max_events, jint maxLoops);
JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_aio_Native_destroyAioContext(JNIEnv *env, jobject class, jobject ctx_addr);


JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_aio_Native_open0(JNIEnv *env, jobject class, jstring file_name);
JNIEXPORT jlong JNICALL Java_org_apache_cassandra_io_aio_Native_size0(JNIEnv *env, jobject class, jint fd);
JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_aio_Native_read0(JNIEnv *env, jobject class, jobject ctx_addr, jobject callback_target, jlong id, jobject dst, jint size, jint fd, jlong position);

JNIEXPORT jint JNICALL Java_org_apache_cassandra_io_aio_Native_close0(JNIEnv *env, jobject class, jint fd);

#ifdef __cplusplus
}
#endif
#endif
