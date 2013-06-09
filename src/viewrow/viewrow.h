/**
 *     Copyright 2013 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 **/

#ifndef LCB_VIEWROW_H_
#define LCB_VIEWROW_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "../contrib/jsonsl/jsonsl.h"
#include <libcouchbase/couchbase.h>

typedef struct lcbex_rows_ctx_st lcbex_vrow_ctx_t;

typedef enum {
    /**
     * This is a row of view data. You can parse this as JSON from your
     * favorite decoder/converter
     */
    LCBEX_VROW_ROW,

    /**
     * All the rows have been returned. In this case, the data is the 'meta'.
     * This is a valid JSON payload which was returned from the server.
     * The "rows" : [] array will be empty.
     */
    LCBEX_VROW_COMPLETE,

    /**
     * A JSON parse error occured. The payload will contain string data. This
     * may be JSON (but this is not likely).
     * The callback will be delivered twice. First when the error is noticed,
     * and second at the end (instead of a COMPLETE callback)
     */
    LCBEX_VROW_ERROR
} lcbex_vrow_type_t;

typedef struct {
    /** The type of data encapsulated */
    lcbex_vrow_type_t type;

    /** string data */
    const char *data;

    /** length */
    size_t ndata;

} lcbex_vrow_datum_t;

typedef void (*lcbex_vrow_callback_t)(lcbex_vrow_ctx_t *ctx,
        const void *cookie,
        const lcbex_vrow_datum_t *resp);


/**
 * Do we always need to always make these lame structures?
 */
typedef struct {
    char *s;
    size_t len;
    size_t alloc;
} lcbex_vrow_buffer;


struct lcbex_rows_ctx_st {
    /* jsonsl parser */
    jsonsl_t jsn;

    /* jsonpointer match object */
    jsonsl_jpr_t jpr;

    /* buffer containing the skeleton */
    lcbex_vrow_buffer meta_buf;

    /* scratch/read buffer */
    lcbex_vrow_buffer current_buf;

    /* last hash key */
    lcbex_vrow_buffer last_hk;

    /* flags. This should be an int with a bunch of constant flags */
    int have_error;
    int initialized;
    int meta_complete;

    unsigned rowcount;

    /* absolute position offset corresponding to the first byte in current_buf */
    size_t min_pos;

    /* minimum (absolute) position to keep */
    size_t keep_pos;

    /**
     * size of the metadata header chunk (i.e. everything until the opening
     * bracket of "rows" [
     */
    size_t header_len;

    /**
     * Position of last row returned. If there are no subsequent rows, this
     * signals the beginning of the metadata trailer
     */
    size_t last_row_endpos;

    /**
     * User stuff:
     */

    /* wrapped cookie */
    void *user_cookie;

    /* callback to invoke */
    lcbex_vrow_callback_t callback;

};

/**
 * Creates a new vrow context object.
 * You must set callbacks on this object if you wish it to be useful.
 * You must feed it data (calling vrow_feed) as well. The data may be fed
 * in chunks and callbacks will be invoked as each row is read.
 */
lcbex_vrow_ctx_t*
lcbex_vrow_create(void);

#define lcbex_vrow_set_callback(vr, cb) vr->callback = cb

#define lcbex_vrow_set_cookie(vr, cookie) vr->user_cookie = cookie


/**
 * Resets the context to a pristine state. Callbacks and cookies are kept.
 * This may be more efficient than allocating/freeing a context each time
 * (as this can be expensive with the jsonsl structures)
 */
void
lcbex_vrow_reset(lcbex_vrow_ctx_t *ctx);

/**
 * Frees a vrow object created by vrow_create
 */
void
lcbex_vrow_free(lcbex_vrow_ctx_t *ctx);

/**
 * Feeds data into the vrow. The callback may be invoked multiple times
 * in this function. In the context of normal lcb usage, this will typically
 * be invoked from within an http_data_callback.
 */
void
lcbex_vrow_feed(lcbex_vrow_ctx_t *ctx, const char *data, size_t ndata);

/**
 * Gets the metadata from the vrow
 */
const char *
lcbex_vrow_get_meta(lcbex_vrow_ctx_t *ctx, size_t *len);

/**
 * Gets a chunk of data from the vrow. There is no telling what the format
 * of the contained data will be; thus there is no guarantee that it will be
 * parseable as complete JSON.
 *
 * This is mainly useful for debugging non-success view responses
 */
const char *
lcb_vrow_get_raw(lcbex_vrow_ctx_t *ctx, size_t *len);

#ifdef __cplusplus
}
#endif

#endif /* LCB_VIEWROW_H_ */
