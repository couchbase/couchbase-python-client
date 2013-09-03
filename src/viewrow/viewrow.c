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

#include "viewrow.h"
#include <assert.h>


#define DECLARE_JSONSL_CALLBACK(name) \
    static void name(\
                     jsonsl_t,jsonsl_action_t,\
                     struct jsonsl_state_st*,const char*)


DECLARE_JSONSL_CALLBACK(row_pop_callback);
DECLARE_JSONSL_CALLBACK(initial_push_callback);
DECLARE_JSONSL_CALLBACK(initial_pop_callback);
DECLARE_JSONSL_CALLBACK(meta_header_complete_callback);
DECLARE_JSONSL_CALLBACK(trailer_pop_callback);

/* conform to void */
#define JOBJ_RESPONSE_ROOT (void*)1
#define JOBJ_ROWSET (void*)2

static void
buffer_append(lcbex_vrow_buffer *vb, const void *data, size_t ndata)
{
    if (vb->alloc - vb->len < ndata) {
        /* multiple of two */
        size_t wanted_size = 64;
        while (wanted_size < ndata + vb->len) {
            wanted_size *= 2;
        }

        vb->alloc = wanted_size;
        vb->s = realloc(vb->s, vb->alloc);
    }

    memcpy(vb->s + vb->len, data, ndata);
    vb->len += ndata;
}

static void
buffer_reset(lcbex_vrow_buffer *vb, int free_chunk)
{
    vb->len = 0;

    if (free_chunk) {
        free(vb->s);
        vb->alloc = 0;
    }

}

/**
 * Gets a buffer, given an (absolute) position offset.
 * It will try to get a buffer of size desired. The actual size is
 * returned in 'actual' (and may be less than desired, maybe even 0)
 */
static const char *
get_buffer_region(lcbex_vrow_ctx_t *ctx, size_t pos, size_t desired,
                  size_t *actual)
{
    const char *ret = ctx->current_buf.s + pos - ctx->min_pos;
    const char *end = ctx->current_buf.s + ctx->current_buf.len;
    *actual = end - ret;

    if (ctx->min_pos > pos) {
        /* swallowed */
        *actual = 0;
        return NULL;
    }

    assert(ret < end);
    if (desired < *actual) {
        *actual = desired;
    }
    return ret;
}

/**
 * Consolidate the meta data into a single parsable string..
 */
static void
combine_meta(lcbex_vrow_ctx_t *ctx)
{
    const char *meta_trailer;
    size_t ntrailer;

    if (ctx->meta_complete) {
        return;
    }

    assert(ctx->header_len <= ctx->meta_buf.len);

    /* Adjust the length for the first portion */
    ctx->meta_buf.len = ctx->header_len;

    /* Append any trailing data */
    meta_trailer = get_buffer_region(ctx,
                                     ctx->last_row_endpos + 1, -1, &ntrailer);

    buffer_append(&ctx->meta_buf, meta_trailer, ntrailer);
    ctx->meta_complete = 1;
}



#define NORMALIZE_OFFSETS(buf, len) \
    buf++; /* beginning of '"' */ \
    len--;


static void
meta_header_complete_callback(jsonsl_t jsn,
                       jsonsl_action_t action,
                       struct jsonsl_state_st *state,
                       const jsonsl_char_t *at)
{

    lcbex_vrow_ctx_t *ctx = (lcbex_vrow_ctx_t*)jsn->data;
    buffer_append(&ctx->meta_buf,
                  ctx->current_buf.s, state->pos_begin);

    ctx->header_len = state->pos_begin;
    jsn->action_callback_PUSH = NULL;

    (void)action;
    (void)at;
}

static void
row_pop_callback(jsonsl_t jsn,
                 jsonsl_action_t action,
                 struct jsonsl_state_st *state,
                 const jsonsl_char_t *at)
{
    lcbex_vrow_ctx_t *ctx = (lcbex_vrow_ctx_t*)jsn->data;
    const char *rowbuf;
    size_t szdummy;

    if (ctx->have_error) {
        return;
    }

    ctx->keep_pos = state->pos_cur;
    ctx->last_row_endpos = state->pos_cur;
    ctx->rowcount++;

    if (state->data == JOBJ_ROWSET) {
        /* don't care anymore.. */
        jsn->action_callback_POP = trailer_pop_callback;
        jsn->action_callback_PUSH = NULL;
        return;
    }


    /* must be a JSON object! */
    if (!ctx->callback) {
        return;
    }

    rowbuf = get_buffer_region(ctx, state->pos_begin, -1, &szdummy);

    {
        /**
         * Create our context..
         */
        lcbex_vrow_datum_t dt = { 0 };
        dt.type = LCBEX_VROW_ROW;
        dt.data = rowbuf;
        dt.ndata = state->pos_cur - state->pos_begin + 1;
        ctx->callback(ctx, ctx->user_cookie, &dt);
    }

    (void)action;
    (void)at;
}

static int
parse_error_callback(jsonsl_t jsn,
                     jsonsl_error_t error,
                     struct jsonsl_state_st *state,
                     jsonsl_char_t *at)
{
    lcbex_vrow_ctx_t *ctx = (lcbex_vrow_ctx_t*)jsn->data;
    ctx->have_error = 1;
    {
        /* invoke the callback */
        lcbex_vrow_datum_t dt = { 0 };
        dt.type = LCBEX_VROW_ERROR;
        dt.data = ctx->current_buf.s;
        dt.ndata = ctx->current_buf.len;
        ctx->callback(ctx, ctx->user_cookie, &dt);
    }

    (void)error;
    (void)state;
    (void)at;

    return 0;
}

static void
trailer_pop_callback(jsonsl_t jsn,
                     jsonsl_action_t action,
                     struct jsonsl_state_st *state,
                     const jsonsl_char_t *at)
{
    lcbex_vrow_ctx_t *ctx = (lcbex_vrow_ctx_t*)jsn->data;
    lcbex_vrow_datum_t dt = { 0 };
    if (state->data != JOBJ_RESPONSE_ROOT) {
        return;
    }
    combine_meta(ctx);
    dt.data = ctx->meta_buf.s;
    dt.ndata = ctx->meta_buf.len;
    dt.type = LCBEX_VROW_COMPLETE;
    ctx->callback(ctx, ctx->user_cookie, &dt);

    (void)action;
    (void)at;
}

static void
initial_pop_callback(jsonsl_t jsn,
                     jsonsl_action_t action,
                     struct jsonsl_state_st *state,
                     const jsonsl_char_t *at)
{
    lcbex_vrow_ctx_t *ctx = (lcbex_vrow_ctx_t*)jsn->data;
    char *key;
    unsigned long len;

    if (ctx->have_error) {
        return;
    }

    if (JSONSL_STATE_IS_CONTAINER(state)) {
        return;
    }

    if (state->type != JSONSL_T_HKEY) {
        return;
    }

    key = ctx->current_buf.s + state->pos_begin;
    len = state->pos_cur - state->pos_begin;
    NORMALIZE_OFFSETS(key, len);

    buffer_reset(&ctx->last_hk, 0);
    buffer_append(&ctx->last_hk, key, len);

    (void)action;
    (void)at;
}

/**
 * This is called for the first few tokens, where we are still searching
 * for the row set.
 */
static void
initial_push_callback(jsonsl_t jsn,
                      jsonsl_action_t action,
                      struct jsonsl_state_st *state,
                      const jsonsl_char_t *at)
{
    lcbex_vrow_ctx_t *ctx = (lcbex_vrow_ctx_t*)jsn->data;
    jsonsl_jpr_match_t match;

    if (ctx->have_error) {
        return;
    }

    if (JSONSL_STATE_IS_CONTAINER(state)) {
        jsonsl_jpr_match_state(jsn,
                               state,
                               ctx->last_hk.s,
                               ctx->last_hk.len,
                               &match);
    }

    buffer_reset(&ctx->last_hk, 0);

    if (ctx->initialized == 0) {
        if (state->type != JSONSL_T_OBJECT) {
            ctx->have_error = 1;
            return;
        }

        if (match != JSONSL_MATCH_POSSIBLE) {
            ctx->have_error = 1;
            return;
        }
        /* tag the state */
        state->data = JOBJ_RESPONSE_ROOT;
        ctx->initialized = 1;
        return;
    }

    if (state->type == JSONSL_T_LIST && match == JSONSL_MATCH_POSSIBLE) {
        /* we have a match */
        jsn->action_callback_POP = row_pop_callback;
        jsn->action_callback_PUSH = meta_header_complete_callback;
        state->data = JOBJ_ROWSET;
    }

    (void)action; /* always PUSH */
    (void)at;
}

static void
feed_data(lcbex_vrow_ctx_t *ctx, const char *data, size_t ndata)
{
    size_t old_len = ctx->current_buf.len;

    buffer_append(&ctx->current_buf, data, ndata);

    jsonsl_feed(ctx->jsn, ctx->current_buf.s + old_len, ndata);

    /**
     * Do we need to cut off some bytes?
     */
    if (ctx->keep_pos > ctx->min_pos) {
        size_t lentmp, diff = ctx->keep_pos - ctx->min_pos;
        const char *buf = get_buffer_region(ctx,
                                            ctx->keep_pos, -1, &lentmp);

        memmove(ctx->current_buf.s,
                buf,
                ctx->current_buf.len - diff);

        ctx->current_buf.len -= diff;
    }

    ctx->min_pos = ctx->keep_pos;
}

/* Non-static wrapper */
void
lcbex_vrow_feed(lcbex_vrow_ctx_t *ctx, const char *data, size_t ndata)
{
    feed_data(ctx, data, ndata);
}


const char *
lcbex_vrow_get_meta(lcbex_vrow_ctx_t *ctx, size_t *len)
{
    combine_meta(ctx);
    *len = ctx->meta_buf.len;
    return ctx->meta_buf.s;
}


lcbex_vrow_ctx_t*
lcbex_vrow_create(void)
{
    lcbex_vrow_ctx_t *ctx;
    jsonsl_error_t err;

    ctx = calloc(1, sizeof(*ctx));
    ctx->jsn = jsonsl_new(512);
    ctx->jpr = jsonsl_jpr_new("/rows/^", &err);

    if (!ctx->jpr) {
        abort();
    }

    jsonsl_jpr_match_state_init(ctx->jsn, &ctx->jpr, 1);
    lcbex_vrow_reset(ctx);

    return ctx;
}

void
lcbex_vrow_reset(lcbex_vrow_ctx_t* ctx)
{
    /**
     * We create a copy, and set its relevant fields. All other
     * fields are zeroed implicitly. Then we copy the object back.
     */
    lcbex_vrow_ctx_t ctx_copy = { 0 };

    jsonsl_reset(ctx->jsn);
    buffer_reset(&ctx->current_buf, 0);
    buffer_reset(&ctx->meta_buf, 0);
    buffer_reset(&ctx->last_hk, 0);

    /**
     * Initially all callbacks are enabled so that we can search for the
     * rows array.
     */
    ctx->jsn->action_callback_POP = initial_pop_callback;
    ctx->jsn->action_callback_PUSH = initial_push_callback;
    ctx->jsn->error_callback = parse_error_callback;
    ctx->jsn->max_callback_level = 4;
    ctx->jsn->data = ctx;

    jsonsl_enable_all_callbacks(ctx->jsn);

    ctx_copy.jsn = ctx->jsn;
    ctx_copy.user_cookie = ctx->user_cookie;
    ctx_copy.callback = ctx->callback;
    ctx_copy.jpr = ctx->jpr;

    ctx_copy.current_buf = ctx->current_buf;
    ctx_copy.meta_buf = ctx->meta_buf;
    ctx_copy.last_hk = ctx->last_hk;

    *ctx = ctx_copy;
}

void
lcbex_vrow_free(lcbex_vrow_ctx_t *ctx)
{
    jsonsl_jpr_match_state_cleanup(ctx->jsn);
    jsonsl_destroy(ctx->jsn);
    jsonsl_jpr_destroy(ctx->jpr);

    buffer_reset(&ctx->current_buf, 1);
    buffer_reset(&ctx->meta_buf, 1);
    buffer_reset(&ctx->last_hk, 1);
    free(ctx);
}
