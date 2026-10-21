#ifndef XCONN_H
#define XCONN_H
#define XC_ABI_VERSION 1
#include <stdint.h>
#include <stddef.h>
typedef struct {uint32_t status;uint8_t *data;size_t len;} XcResult;
uint32_t xc_abi_version(void);
/* operation: 1 submit, 2 wait-ready, 3 take, 4 cancel, 5 blocking request,
 * 6 wait for any completed submitted job (one shared host dispatcher).
 * 7 submit without the global completion queue (use xc_notify instead).
 * Input is borrowed only during the call; results must be freed exactly once. */
XcResult *xc_process(uint32_t operation,uint64_t handle,const uint8_t *input,size_t len);
/* Process-scoped host message posting; post copies the opaque message.
 * Transfers message ownership, calls release once, cancels job if post fails. */
typedef int8_t (*XcPost)(int64_t port, void *message);
typedef void (*XcFree)(void *message);
void xc_notify(uint64_t handle, int64_t port, void *message, XcPost post, XcFree release);
/* Accepts NULL or an unmodified result returned by xc_process, exactly once.
 * The result struct and its data are Rust-owned. Do not fabricate a result,
 * overwrite its fields, or free either allocation using a foreign allocator. */
void xc_result_free(XcResult *result);
#endif
