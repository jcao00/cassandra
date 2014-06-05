#include <jni.h>
#ifndef CASSANDRA_STREAMING_INCLUDE
#define CASSANDRA_STREAMING_INCLUDE

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jint JNICALL Java_org_apache_cassandra_streaming_NativeStreamer_write0(JNIEnv *env, jobject class, jint in_fd, jlong offset, jint len, jint out_fd);

#ifdef __cplusplus
}
#endif
#endif
