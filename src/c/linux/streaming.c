#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <jni.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <stdbool.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <string.h>

#include "../streaming.h"

JNIEXPORT jint JNICALL 
Java_org_apache_cassandra_streaming_NativeStreamer_write0(JNIEnv *env, jobject class, jint in_fd, jlong offset, jint len, jint out_fd)
{
    // implementaion based on http://blog.superpat.com/2010/06/01/zero-copy-in-linux-with-sendfile-and-splice/
    int pipefd[2];
    if (pipe(pipefd) < 0)
        return JNI_ENOMEM;

    size_t total = 0;
    ssize_t bytes, bytes_sent, bytes_in_pipe;
    while (total < (size_t)len)
    {
        // Splice the data from in_fd into the pipe
        if ((bytes_sent = splice(in_fd, &offset, pipefd[1], NULL, len - total, SPLICE_F_MOVE)) <= 0) 
        {
            if (errno == EINTR || errno == EAGAIN) 
            {
                continue;
            }
            // dump error code somewhere, and close pipe properly
            return JNI_EINVAL;
        }

        // Splice the data from the pipe into out_fd
        bytes_in_pipe = bytes_sent;
        while (bytes_in_pipe > 0) 
        {
            if ((bytes = splice(pipefd[0], NULL, out_fd, NULL, bytes_in_pipe, SPLICE_F_MOVE)) <= 0) 
            {
                if (errno == EINTR || errno == EAGAIN) 
                {
                    continue;
                }
                // dump error code somewhere, and close pipe properly
                return JNI_EINVAL;
            }
            bytes_in_pipe -= bytes;
        }
        total += bytes_sent;
    }

    close(pipefd[0]);
    close(pipefd[1]);
    return total;
}
