#ifndef VIDEO_RECEIVE_BATCH_H
#define VIDEO_RECEIVE_BATCH_H

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>

#define VIDEO_RECEIVE_BATCH_SIZE 8
#define VIDEO_RECEIVE_BATCH_TRUNCATED (-2)

typedef struct _VIDEO_RECEIVE_BATCH {
    struct mmsghdr messages[VIDEO_RECEIVE_BATCH_SIZE];
    struct iovec vectors[VIDEO_RECEIVE_BATCH_SIZE];
    char* packets[VIDEO_RECEIVE_BATCH_SIZE];
    unsigned int count, next, last;
} VIDEO_RECEIVE_BATCH;

static void initializeVideoReceiveBatch(VIDEO_RECEIVE_BATCH* batch, size_t receiveSize) {
    memset(batch, 0, sizeof(*batch));
    for (unsigned int i = 0; i < VIDEO_RECEIVE_BATCH_SIZE; i++) {
        batch->vectors[i].iov_len = receiveSize;
        batch->messages[i].msg_hdr.msg_iov = &batch->vectors[i];
        batch->messages[i].msg_hdr.msg_iovlen = 1;
    }
}

// A non-NULL current packet was rejected by the RTP queue; reuse its slot.
// A NULL current packet has transferred ownership to the queue.
static int receiveVideoPacketBatch(VIDEO_RECEIVE_BATCH* batch, int socket,
                                  size_t bufferSize, char** current) {
    if (*current != NULL) {
        batch->packets[batch->last] = *current;
        *current = NULL;
    }

    if (batch->next == batch->count) {
        for (unsigned int i = 0; i < VIDEO_RECEIVE_BATCH_SIZE; i++) {
            if (batch->packets[i] == NULL) {
                batch->packets[i] = malloc(bufferSize);
                if (batch->packets[i] == NULL) {
                    errno = ENOMEM;
                    return -1;
                }
                batch->vectors[i].iov_base = batch->packets[i];
            }
        }

        int count;
        do {
            // Block for the first datagram only, then drain those already available.
            count = recvmmsg(socket, batch->messages, VIDEO_RECEIVE_BATCH_SIZE,
                            MSG_WAITFORONE, NULL);
        } while (count < 0 && errno == ECONNREFUSED);
        batch->next = 0;
        batch->count = count > 0 ? (unsigned int)count : 0;
        if (count <= 0) {
            if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK ||
                              errno == EINTR || errno == ETIMEDOUT)) {
                return 0;
            }
            return count;
        }
    }

    unsigned int index = batch->next++;
    batch->last = index;
    *current = batch->packets[index];
    batch->packets[index] = NULL;
    if (batch->messages[index].msg_hdr.msg_flags & MSG_TRUNC) {
        return VIDEO_RECEIVE_BATCH_TRUNCATED;
    }
    return (int)batch->messages[index].msg_len;
}

// The caller separately frees current; queued packets belong to the RTP queue.
static void destroyVideoReceiveBatch(VIDEO_RECEIVE_BATCH* batch) {
    for (unsigned int i = 0; i < VIDEO_RECEIVE_BATCH_SIZE; i++) {
        free(batch->packets[i]);
        batch->packets[i] = NULL;
    }
}
#endif
