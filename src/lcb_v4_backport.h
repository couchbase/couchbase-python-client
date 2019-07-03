/**
 *     Copyright 2019 Couchbase, Inc.
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

#ifndef COUCHBASE_PYTHON_CLIENT_LCB_V4_BACKPORT_H
#define COUCHBASE_PYTHON_CLIENT_LCB_V4_BACKPORT_H

#include "util_wrappers.h"
#include <stdint.h>
#include <libcouchbase/api3.h>
#include <libcouchbase/couchbase.h>
#include <libcouchbase/ixmgmt.h>
#include <libcouchbase/n1ql.h>
#include <libcouchbase/subdoc.h>
#include <libcouchbase/views.h>
#include "libcouchbase/cbft.h"
#include <libcouchbase/crypto.h>
#include "pycbc_subdocops.h"

typedef lcb_PINGSVCTYPE lcb_PING_SERVICE;
typedef lcb_RESPVIEWQUERY lcb_RESPVIEW;
typedef lcb_CMDVIEWQUERY lcb_CMDVIEW;
typedef lcb_RESPN1QL lcb_RESPANALYTICS;
typedef lcb_error_t lcb_STATUS;

#define PYCBC_ENDURE 1

#define pycbc_RESPGET_USE(X) &temp
#define pycbc_RESPGET_DEFINE(X) LENGTHTYPE_DEFINE(X)
#define pycbc_RESPGET_RET(X) X
#define lcb_cmdping_destroy(CMD)
#define lcb_cmddiag_create(CMD) \
    lcb_CMDDIAG cmd_real = {0}; \
    *(CMD) = &cmd_real;
#define lcb_cmddiag_destroy(CMD)
#define lcb_cmddiag_prettify(CMD, X)                              \
    CMD->options = ((CMD)->options & ~LCB_PINGOPT_F_JSONPRETTY) | \
                   ((X) ? LCB_PINGOPT_F_JSONPRETTY : 0)
#define lcb_cmddiag_report_id(CMD, ID, NID) \
    (CMD)->id = ID;                         \
    (void)(NID);

#define lcb_cmdstats_create(DEST) \
    lcb_CMDSTATS cmd_real = {0};  \
    *(DEST) = &cmd_real;
#define lcb_cmdstats_destroy(DEST) LCB_SUCCESS
#define pycbc_cmdstats_kv(CMD)  (CMD)->cmdflags |= LCB_CMDSTATS_F_KV;
#define pycbc_stats(...) lcb_stats3(__VA_ARGS__)


#define lcb_cmdget_expiration(cmd, time) cmd->exptime = time;
#define lcb_cmdget_timeout(cmd, time) cmd->exptime = time;
#define lcb_cmdtouch_create(CMD) \
    lcb_CMDTOUCH cmd_real = {0}; \
    *(CMD) = &cmd_real;
#define lcb_cmdtouch_destroy(CMD) 0
#define lcb_cmdtouch_expiration(cmd, time) cmd->exptime = time;
#define lcb_cmdtouch_timeout(cmd, time) cmd->exptime = time;
#define lcb_cmdrget_expiration(cmd, time) cmd->exptime = time;
#define lcb_cmdcounter_delta(cmd, x) (cmd)->delta = x;
#define lcb_cmdcounter_initial(cmd, x) \
    (cmd)->initial = x;                \
    (cmd)->create = 1;
#define lcb_cmdcounter_timeout(cmd, x) (cmd)->exptime = x;
#define lcb_cmdcounter_expiration(cmd, x) (cmd)->exptime = x
#define lcb_cmdstore_flags(CMD, VAL) cmd->flags = VAL;
#define lcb_cmdgetreplica_expiration(CMD, TTL) (CMD)->exptime = TTL
#define lcb_cmdstore_create(CMD, OP) \
    PYCBC_ASSIGN((*CMD)->operation, (lcb_storage_t)(OP));
#define lcb_cmdstore_destroy(...) 0
#define lcb_cmdstore_cas(CMD, CAS) PYCBC_ASSIGN((CMD)->cas, CAS);
#define lcb_cmdstore_expiration(CMD, TTL) \
    PYCBC_ASSIGN((CMD)->exptime, (lcb_U32)(TTL));
#define lcb_cmdsubdoc_cas(CMD, CAS) (CMD)->cas = (lcb_U32)(CAS)
#define lcb_cmdsubdoc_expiration(CMD, EXP) (CMD)->exptime = (lcb_U32)(EXP)

#define lcb_resphttp_headers(htresp, dest) *(dest) = htresp->headers
#define lcb_resphttp_http_status(resp, dest) *(dest) = (resp)->htstatus
#define lcb_resphttp_body(resp, bodybuffer, bodylength) \
    *(bodybuffer) = resp->body;                         \
    (*bodylength) = resp->nbody;
#define lcb_respn1ql_http_response(INNER, DEST) *(DEST) = INNER->htresp;
#define lcb_respn1ql_row(INNER, ROW, ROW_COUNT) \
    {                                           \
        *(ROW) = (INNER)->row;                  \
        *(ROW_COUNT) = (INNER)->nrow;           \
    }
#define lcb_respn1ql_cookie(RESP, DEST) *(DEST) = (RESP)->cookie;
#define lcb_respn1ql_is_final(RESP) (RESP)->rflags &LCB_RESP_F_FINAL
#define lcb_respn1ql_status(RESP) (RESP)->rc
#define lcb_respanalytics_http_response(INNER, DEST) *(DEST) = INNER->htresp;
#define lcb_respanalytics_row(INNER, ROW, ROW_COUNT) \
    {                                           \
        *(ROW) = (INNER)->row;                  \
        *(ROW_COUNT) = (INNER)->nrow;           \
    }
#define lcb_respanalytics_cookie(RESP, DEST) *(DEST) = (RESP)->cookie;
#define lcb_respanalytics_is_final(RESP) (RESP)->rflags &LCB_RESP_F_FINAL
#define lcb_respanalytics_status(RESP) (RESP)->rc

#define lcb_cmdn1ql_create(CMD) \
    lcb_CMDN1QL cmd_real = {0}; \
    cmd = &cmd_real;
#define lcb_cmdn1ql_callback(CMD, CALLBACK) (CMD)->callback = (CALLBACK)
#define lcb_cmdn1ql_query(CMD, PARAMS, NPARAMS) \
    (CMD)->query = PARAMS;                      \
    (CMD)->nquery = NPARAMS;
#define lcb_cmdn1ql_handle(CMD, HANDLE) (CMD)->handle = HANDLE
#define lcb_cmdn1ql_adhoc(CMD, ENABLE)               \
    ((CMD)->cmdflags) &= (~LCB_CMDN1QL_F_PREPCACHE); \
    ((CMD)->cmdflags) |= (ENABLE ? LCB_CMDN1QL_F_PREPCACHE : 0);
#define lcb_cmdn1ql_parent_span(...) lcb_n1ql_set_parent_span(__VA_ARGS__)

#define lcb_cmdanalytics_create(CMD) \
    lcb_CMDN1QL cmd_real = {0}; \
    cmd = &cmd_real;
#define lcb_cmdanalytics_callback(CMD, CALLBACK) (CMD)->callback = (CALLBACK)
#define lcb_cmdanalytics_query(CMD, PARAMS, NPARAMS) \
    (CMD)->query = PARAMS;                      \
    (CMD)->nquery = NPARAMS;
#define lcb_cmdanalytics_handle(CMD, HANDLE) (CMD)->handle = HANDLE
#define lcb_cmdanalytics_parent_span(...) lcb_n1ql_set_parent_span(__VA_ARGS__)

lcb_STATUS lcb_n1ql(lcb_t instance, const void *cookie, const lcb_CMDN1QL *cmd);

lcb_STATUS lcb_analytics(lcb_t instance,
                         const void *cookie,
                         const lcb_CMDN1QL *cmd);

#define lcb_fts lcb_fts_query

#define lcb_respview_document(CTX, DEST) *(DEST) = CTX->docresp
#define lcb_respview_key(CTX, DEST, NDEST) \
    *(DEST) = (CTX)->key;                  \
    *(NDEST) = (CTX)->nkey;
#define lcb_respview_geometry(CTX, DEST, NDEST) \
    *(DEST) = (CTX)->geometry;                  \
    *(NDEST) = (CTX)->ngeometry;

pycbc_strn_base_const pycbc_view_geometry(const lcb_RESPVIEW *ctx);

#define VIEW_FIELDS_REDUCE(X, SEP)\
    X(key,key) SEP \
    X(value,row) SEP \
    X(geo, geometry)

#define lcb_respview_row(CTX, DEST, NDEST) \
    *(DEST) = (CTX)->value;                \
    *(NDEST) = (CTX)->nvalue;
#define lcb_respview_doc_id(RESP, DOCID, NDOCID) \
    *(DOCID) = (RESP)->docid;                    \
    *(NDOCID) = (RESP)->ndocid;
#define lcb_http_cancel(instance, req) lcb_cancel_http_request(instance, req)
#define lcb_cmdhttp_create(CMD, TYPE) (*CMD)->type = TYPE;
#define lcb_cmdhttp_body(HTCMD, BODY, NBODY) \
    HTCMD->body = BODY;                      \
    HTCMD->nbody = NBODY;
#define lcb_cmdhttp_content_type(HTCMD, CTYPE, CTYPELEN) \
    HTCMD->content_type = CTYPE;                         \
    PYCBC_DUMMY(HTCMD->ncontent_type = CTYPELEN;)
#define lcb_cmdhttp_method(HTCMD, METHOD) HTCMD->method = METHOD;
#define lcb_cmdhttp_handle(HTCMD, HANDLE) HTCMD->reqhandle = HANDLE;
#define lcb_respview_cookie(RESP, DEST) *(DEST) = (RESP)->cookie
#define lcb_respview_is_final(RESP) (RESP)->rflags &LCB_RESP_F_FINAL
#define lcb_respview_status(RESP) (RESP)->rc


#define lcb_cmdview_create(DEST) \
    lcb_CMDVIEW cmd_real = {0};  \
    *(DEST) = &cmd_real;
#define lcb_cmdview_design_document(VCMD, DESIGN, NDESIGN) \
    (VCMD)->ddoc = DESIGN;                                 \
    (VCMD)->nddoc = NDESIGN;
#define lcb_cmdview_view_name(VCMD, VIEW, NVIEW) \
    (VCMD)->view = VIEW;                         \
    (VCMD)->nview = NVIEW;
#define lcb_cmdview_option_string(VCMD, OPTSTR, NOPTSTR) \
    (VCMD)->optstr = OPTSTR;                             \
    (VCMD)->noptstr = NOPTSTR
#define lcb_cmdview_post_data(VCMD, BODY, NBODY) \
    (VCMD)->postdata = BODY;                     \
    (VCMD)->npostdata = NBODY
#define lcb_cmdview_handle(VCMD, HANDLE) (VCMD)->handle = HANDLE
#define lcb_cmdview_callback(VCMD, CALLBACK) (VCMD)->callback = CALLBACK;
#define lcb_cmdview_include_docs(VCMD, ENABLE)                                 \
    (VCMD)->cmdflags = ((VCMD)->cmdflags & ~LCB_CMDVIEWQUERY_F_INCLUDE_DOCS) | \
                       (ENABLE ? LCB_CMDVIEWQUERY_F_INCLUDE_DOCS : 0)
#define lcb_cmdview_no_row_parse(VCMD, ENABLE)                               \
    (VCMD)->cmdflags = ((VCMD)->cmdflags & ~LCB_CMDVIEWQUERY_F_NOROWPARSE) | \
                       (ENABLE ? LCB_CMDVIEWQUERY_F_NOROWPARSE : 0)
#define lcb_cmdview_spatial(VCMD, ENABLE)                                 \
    (VCMD)->cmdflags = ((VCMD)->cmdflags & ~LCB_CMDVIEWQUERY_F_SPATIAL) | \
                       (ENABLE ? LCB_CMDVIEWQUERY_F_SPATIAL : 0)
lcb_STATUS pycbc_cmdview_spatial(lcb_CMDVIEW *pCmdview, int is_spatial);

#define lcb_view(...) lcb_view_query(__VA_ARGS__)

#define lcb_cmdview_parent_span(...) lcb_view_set_parent_span(__VA_ARGS__)
#define lcb_cmdview_destroy(CMD)
#define lcb_cmdsubdoc_create(CMD) \
    lcb_CMDSUBDOC cmd_real = {0}; \
    *(CMD) = &cmd_real;
#define lcb_cmdsubdoc_destroy(CMD)
#define lcb_cmdsubdoc_key(CMD, KEY, NKEY) LCB_CMD_SET_KEY(CMD, KEY, NKEY)
#define lcb_cmdsubdoc_create_if_missing(CMD, ENABLE) (CMD)->cmdflags = (((CMD)->cmdflags &~LCB_CMDSUBDOC_F_UPSERT_DOC) | ((ENABLE)?LCB_CMDSUBDOC_F_UPSERT_DOC:0));
#define CMDSUBDOC_F_UPSERT_DOC LCB_CMDSUBDOC_F_UPSERT_DOC

#define lcb_respgetcid_cookie(RESP, DEST) *(DEST) = (RESP)->cookie;
#define lcb_respgetcid_status(RESP) (RESP)->rc
#define lcb_respgetcid_collection_id(RESP, DEST) *(DEST) = (RESP)->collection_id
#define lcb_respgetcid_manifest_id(RESP, DEST) *(DEST) = (RESP)->manifest_id
#define lcb_cmdsubdoc_parent_span(CMD, SPAN) \
    LCB_CMD_SET_TRACESPAN((CMD), (SPAN));
#define lcb_cmdobserve_parent_span(CMD, SPAN) \
    LCB_CMD_SET_TRACESPAN((CMD), (SPAN));
#define lcb_cmdendure_parent_span(CMD, ...) \
    LCB_CMD_SET_TRACESPAN((CMD), __VA_ARGS__);
#define lcb_cmdfts_parent_span(...) lcb_fts_set_parent_span(__VA_ARGS__)
#define lcb_cmdping_create(CMD) \
    lcb_CMDPING cmd_real = {0}; \
    *(CMD) = &cmd_real;

#define lcb_respfts_cookie(RESP,DEST) *(DEST)=(RESP)->cookie;

#define pycbc_resphttp_cookie(resp, type, target) \
    (*((type *)(target))) = resp->cookie;
/*
#define lcb_respping_cookie(RESP, DEST) *(DEST) = (RESP)->cookie;
#define lcb_respping_status(RESP) (RESP)->rc
 */
#define lcb_respping_result_size(RESP) resp->nservices
#define lcb_respping_result_service(RESP, INDEX, DEST) \
    *(DEST) = (RESP)->services[INDEX].type
#define lcb_respping_result_latency(RESP, INDEX, DEST) \
    *(DEST) = (RESP)->services[INDEX].latency
#define lcb_respping_result_status(RESP, INDEX) (RESP)->services[INDEX].rc
#define lcb_respping_result_remote(RESP, INDEX, BUFFER, NBUFFER) \
    *(BUFFER) = (RESP)->services[INDEX].server;                  \
    *(NBUFFER) = strlen((RESP)->services[INDEX].server);
#define lcb_respping_value(RESP, JSON, NJSON) \
    *(JSON) = resp->json;                     \
    *(NJSON) = resp->njson;

/*
 #define lcb_respdiag_cookie(RESP, DEST) *(DEST) = (RESP)->cookie
 #define lcb_respdiag_status(RESP) (RESP)->rc
*/
#define lcb_respdiag_value(RESP, JSON, NJSON) \
    *(JSON) = (RESP)->json;                   \
    *(NJSON) = (RESP)->njson;

#define lcb_respstore_observe_stored(resp_base, dest) \
    *dest = (resp_base->rflags & LCB_RESP_F_FINAL);

#define LCB_PING_SERVICE_KV LCB_PINGSVC_KV
#define LCB_PING_SERVICE_VIEWS LCB_PINGSVC_VIEWS
#define LCB_PING_SERVICE_N1QL LCB_PINGSVC_N1QL
#define LCB_PING_SERVICE_FTS LCB_PINGSVC_FTS
#define LCB_PING_SERVICE_ANALYTICS LCB_PINGSVC_ANALYTICS
#define LCB_PING_SERVICE__MAX LCB_PINGSVC__MAX

#define PYCBC_CMD_SET_KEY_SCOPE(SCOPE, CMD, KEY)                    \
    PYCBC_DEBUG_LOG("Setting key %.*s", (KEY).length, (KEY).buffer) \
    LCB_CMD_SET_KEY(CMD, (KEY).buffer, (KEY).length)
#define PYCBC_CMD_SET_VALUE_SCOPE(SCOPE, CMD, KEY)                    \
    PYCBC_DEBUG_LOG("Setting value %.*s", (KEY).length, (KEY).buffer) \
    LCB_CMD_SET_VALUE(CMD, (KEY).buffer, (KEY).length)
#define PYCBC_get_ATTR(CMD, attrib, ...) CMD->attrib = __VA_ARGS__;
#define PYCBC_touch_ATTR(CMD, attrib, ...) CMD->attrib = __VA_ARGS__;
#define PYCBC_getreplica_ATTR(CMD, attrib, ...) CMD->attrib = __VA_ARGS__;
#define PYCBC_unlock_ATTR(CMD, attrib, ...) CMD->attrib = __VA_ARGS__;
#define PYCBC_remove_ATTR(CMD, attrib, ...) CMD->attrib = __VA_ARGS__;
#define PYCBC_endure_ATTR(CMD, attrib, ...) CMD->attrib = __VA_ARGS__;
#define PYCBC_ASSIGN(LHS, RHS)                                    \
    PYCBC_DEBUG_LOG_CONTEXT(                                      \
            context, "Assigning %s (%d) to %s", #RHS, RHS, #LHS); \
    LHS = RHS;
#define PYCBC_RESP_GET(SCOPE, UCSCOPE, ATTRIB, TYPE) \
    PYCBC_SCOPE_GET(SCOPE, const lcb_RESP##UCSCOPE *, ATTRIB, TYPE);
#define pycbc_verb(VERB, INSTANCE, COOKIE, CMD) \
    pycbc_verb_postfix(3, VERB, INSTANCE, COOKIE, CMD)
#define pycbc_rget(INSTANCE, COOKIE, CMD) \
    pycbc_verb_postfix(3, rget, (INSTANCE), (COOKIE), (CMD))
#if PYCBC_LCB_API>0x02FF00
typedef lcb_DURABILITYLEVEL pycbc_DURABILITY_LEVEL;
#else
typedef int pycbc_DURABILITY_LEVEL;
#define LCB_DURABILITYLEVEL_NONE 0
#define LCB_DURABILITYLEVEL_MAJORITY_AND_PERSIST_ON_MASTER -1
#define LCB_COLLECTION_UNKNOWN -1;
#endif

typedef const lcb_RESPGET *pycbc_RESPGET;
typedef lcb_PINGSVCTYPE lcb_PING_SERVICE;
typedef lcb_RESPGET lcb_RESPGETREPLICA;
typedef struct lcb_st lcb_INSTANCE;
typedef struct {
    lcb_SDSPEC *specs;
    size_t nspecs;
    lcb_U32 options;
} lcb_SUBDOCOPS;


typedef enum {
    LCB_REPLICA_MODE_ANY = 0x00,
    LCB_REPLICA_MODE_ALL = 0x01,
    LCB_REPLICA_MODE_IDX0 = 0x02,
    LCB_REPLICA_MODE_IDX1 = 0x03,
    LCB_REPLICA_MODE_IDX2 = 0x04,
    LCB_REPLICA_MODE__MAX
} lcb_REPLICA_MODE;
typedef lcb_CMDBASE *pycbc_CMDBASE;
typedef lcb_CMDGET *pycbc_CMDGET;
typedef lcb_CMDTOUCH *pycbc_CMDTOUCH;
typedef lcb_CMDGETREPLICA *pycbc_CMDGETREPLICA;
typedef lcb_CMDREMOVE *pycbc_CMDREMOVE;
typedef lcb_CMDUNLOCK *pycbc_CMDUNLOCK;
typedef lcb_CMDENDURE *pycbc_CMDENDURE;
typedef lcb_CMDHTTP *pycbc_CMDHTTP;
typedef lcb_CMDSTORE *pycbc_CMDSTORE;
typedef lcb_CMDN1QL lcb_CMDANALYTICS;
typedef lcb_SDSPEC pycbc_SDSPEC;
typedef lcb_VIEWHANDLE pycbc_VIEW_HANDLE;
typedef lcb_http_request_t pycbc_HTTP_HANDLE;
typedef lcb_FTSHANDLE pycbc_FTS_HANDLE;
typedef lcb_N1QLHANDLE pycbc_N1QL_HANDLE;
typedef lcb_N1QLHANDLE pycbc_ANALYTICS_HANDLE;

lcb_STATUS lcb_cmdstore_durability(lcb_CMDSTORE *cmd,
                                   pycbc_DURABILITY_LEVEL level);

lcb_STATUS lcb_respfts_http_response(const lcb_RESPFTS *resp, const lcb_RESPHTTP **ptr);

lcb_STATUS lcb_respfts_row(const lcb_RESPFTS *resp, const char **pString, size_t *pInt);

int lcb_respfts_is_final(const lcb_RESPFTS *resp);

lcb_STATUS lcb_respfts_status(const lcb_RESPFTS *resp);


lcb_STATUS lcb_cmdfts_callback(lcb_CMDFTS *cmd, void (*callback)(lcb_t, int, const lcb_RESPFTS *));

lcb_STATUS lcb_cmdfts_query(lcb_CMDFTS *cmd, const void *pVoid, size_t length);

lcb_STATUS lcb_cmdfts_handle(lcb_CMDFTS *cmd, pycbc_FTS_HANDLE *pFTSREQ);

int lcb_mutation_token_is_valid(const lcb_MUTATION_TOKEN *pTOKEN);

struct pycbc_pybuffer_real;
lcb_STATUS lcb_cmdget_key(lcb_CMDBASE *ctx, struct pycbc_pybuffer_real *buf);

void lcb_cmdgetreplica_create(lcb_CMDGETREPLICA **pcmd, int strategy);

uint64_t pycbc_mutation_token_seqno(const struct lcb_MUTATION_TOKEN *pToken);

uint64_t pycbc_mutation_token_vbid(const struct lcb_MUTATION_TOKEN *pToken);

uint64_t pycbc_mutation_token_uuid(const struct lcb_MUTATION_TOKEN *pToken);

#define PYCBC_OBSERVE_STANDALONE

#define DEFAULT_VERBPOSTFIX 3

#if LCB_VERSION > 0x020807
#    define PYCBC_CRYPTO_VERSION 1
#else
#    define PYCBC_CRYPTO_VERSION 0
#endif

#define GET_ATTRIBS(X) X(get, lcb_CMDGET *, locktime, lock, int);

lcb_STATUS lcb_respview_http_response(const lcb_RESPVIEW *resp,
                                      const lcb_RESPHTTP **dest);


#define PYCBC_SCOPE_GET_DECL(SCOPE, CTXTYPE, ATTRIB, TYPE) \
    TYPE pycbc_cmd##SCOPE##_##ATTRIB(const CTXTYPE ctx)

#define PYCBC_SCOPE_SET_DECL(SCOPE, CTXTYPE, ATTRIB, MEMBER, TYPE) \
    lcb_STATUS lcb_cmd##SCOPE##_##ATTRIB(CTXTYPE ctx, TYPE value)

#define PYCBC_SCOPE_SET(SCOPE, CTXTYPE, ATTRIB, MEMBER, TYPE)     \
    lcb_STATUS lcb_cmd##SCOPE##_##ATTRIB(CTXTYPE ctx, TYPE value) \
    {                                                             \
        ctx->MEMBER = value;                                      \
        return LCB_SUCCESS;                                       \
    }

#define PYCBC_SCOPE_GET(SCOPE, CTXTYPE, ATTRIB, TYPE) \
    TYPE pycbc_##SCOPE##_##ATTRIB(const CTXTYPE ctx)  \
    {                                                 \
        return ctx->ATTRIB;                           \
    }

GET_ATTRIBS(PYCBC_SCOPE_SET_DECL);

#define LCB_STORE_WRAPPER(b) ADD_MACRO(LCB_STORE_##b);

enum {
#define PYCBC_BACKPORT_STORE(X) LCB_STORE_##X = LCB_##X
#define ALL_ENUMS(X) X(APPEND), X(PREPEND), X(SET), X(UPSERT), X(ADD), X(REPLACE)
    ALL_ENUMS(PYCBC_BACKPORT_STORE)
};

#define PYCBC_RESP_ACCESSORS_POSTFIX(DECL, IMP, UC, LC) \
    PYCBC_KEY_ACCESSORS(DECL, IMP, UC, LC)              \
    PYCBC_NOKEY_ACCESSORS(DECL, IMP, UC, LC)

#define PYCBC_GET_ACCESSORS_POSTFIX(DECL, IMP, UC, LC) \
    PYCBC_RESP_ACCESSORS(DECL, IMP, UC, LC);           \
    PYCBC_VAL_ACCESSORS(DECL, IMP, UC, LC);            \
    PYCBC_ITMFLAGS_ACCESSORS_U32(DECL, IMP, UC, LC)

#define PYCBC_HTTP_ACCESSORS_POSTFIX(DECL, IMP, UC, LC) \
    PYCBC_RESP_ACCESSORS(DECL, IMP, UC, LC);            \
    PYCBC_FLAGS_ACCESSORS_U32(DECL, IMP, UC, LC)        \
    PYCBC_HOST_ACCESSORS(DECL, IMP, UC, LC)
#define PYCBC_STATS_ACCESSORS_POSTFIX(DECL, IMP, UC, LC) \
    PYCBC_RESP_ACCESSORS(DECL, IMP, UC, LC)              \
    PYCBC_VAL_ACCESSORS(DECL, IMP, UC, LC)               \
    PYCBC_FLAGS_ACCESSORS_U64(DECL, IMP, UC, LC)

#define PYCBC_COUNT_ACCESSORS_POSTFIX(DECL, IMP, UC, LC) \
    PYCBC_RESP_ACCESSORS(DECL, IMP, UC, LC)              \
    PYCBC_LLUVAL_ACCESSORS(DECL, IMP, UC, LC)

#define PYCBC_NOKEY_ACCESSORS_POSTFIX(DECL, IMP, UC, LC) \
    PYCBC_NOKEY_ACCESSORS(DECL, IMP, UC, LC)

#define PYCBC_ENDURE_ACCESSORS_POSTFIX(DECL, IMP, UC, LC) \
    PYCBC_ENDURE_ACCESSORS(DECL, IMP, UC, LC)

#define ENDUREOPS(X, ...) X(ENDURE, endure)

#define PYCBC_OBSERVE_ACCESSORS_POSTFIX(DECL, IMP, UC, LC) \
    PYCBC_OBSERVE_ACCESSORS(DECL, IMP, UC, LC)
#define OBSERVEOPS(X, ...) X(OBSERVE, observe)

#ifndef PYCBC_CRYPTO_VERSION
#    if LCB_VERSION > 0x020807
#        define PYCBC_CRYPTO_VERSION 1
#    else
#        define PYCBC_CRYPTO_VERSION 0
#    endif
#endif

lcb_STATUS pycbc_crypto_register(lcb_INSTANCE *instance,
                                 const char *name,
                                 lcbcrypto_PROVIDER *provider);
lcb_STATUS pycbc_crypto_unregister(lcb_INSTANCE *instance, const char *name);
lcb_STATUS pycbc_encrypt_fields(lcb_INSTANCE* instance, lcbcrypto_CMDENCRYPT* cmd);
lcb_STATUS pycbc_decrypt_fields(lcb_INSTANCE* instance, lcbcrypto_CMDDECRYPT* cmd);
const lcb_MUTATION_TOKEN *pycbc_get_vbucket_mutation_token(
        lcb_INSTANCE *instance, lcb_KEYBUF *kb, lcb_STATUS *rc);

#define PYCBC_KEY_ACCESSORS(DECL, IMP, UC, LC)                           \
    DECL(lcb_STATUS lcb_resp##LC##_key(                                  \
            const lcb_RESP##UC *resp, const char **buffer, size_t *len)) \
    IMP({                                                                \
        *buffer = resp->key;                                             \
        *len = resp->nkey;                                               \
        return LCB_SUCCESS;                                              \
    })

#define PYCBC_NOKEY_ACCESSORS(DECL, IMP, UC, LC)                     \
    DECL(lcb_STATUS lcb_resp##LC##_cookie(const lcb_RESP##UC *resp,  \
                                          void **dest))              \
    IMP({                                                            \
        *dest = resp->cookie;                                        \
        return LCB_SUCCESS;                                          \
    })                                                               \
    DECL(lcb_STATUS lcb_resp##LC##_status(const lcb_RESP##UC *resp)) \
    IMP({ return resp->rc; })                                        \
    DECL(lcb_STATUS lcb_resp##LC##_cas(const lcb_RESP##UC *resp,     \
                                       lcb_uint64_t *dest))          \
    IMP({                                                            \
        *dest = resp->cas;                                           \
        return LCB_SUCCESS;                                          \
    });

#define PYCBC_VAL_ACCESSORS(DECL, IMP, UC, LC)                            \
    DECL(lcb_STATUS lcb_resp##LC##_value(                                 \
            const lcb_RESP##UC *resp, const char **dest, size_t *length)) \
    IMP({                                                                 \
        *dest = resp->value;                                              \
        *length = resp->nvalue;                                           \
        return LCB_SUCCESS;                                               \
    })

#define PYCBC_LLUVAL_ACCESSORS(DECL, IMP, UC, LC)                  \
    DECL(lcb_STATUS lcb_resp##LC##_value(const lcb_RESP##UC *resp, \
                                         lcb_U64 *dest))           \
    IMP({                                                          \
        *dest = resp->value;                                       \
        return LCB_SUCCESS;                                        \
    })
#define PYCBC_FLAGS_ACCESSORS_U32(DECL, IMP, UC, LC)               \
    DECL(lcb_STATUS lcb_resp##LC##_flags(const lcb_RESP##UC *resp, \
                                         lcb_uint32_t *dest))      \
    IMP({                                                          \
        *dest = resp->rflags;                                      \
        return LCB_SUCCESS;                                        \
    })

#define PYCBC_ITMFLAGS_ACCESSORS_U32(DECL, IMP, UC, LC)            \
    DECL(lcb_STATUS lcb_resp##LC##_flags(const lcb_RESP##UC *resp, \
                                         lcb_uint32_t *dest))      \
    IMP({                                                          \
        *dest = resp->itmflags;                                    \
        return LCB_SUCCESS;                                        \
    })
#define PYCBC_FLAGS_ACCESSORS_U64(DECL, IMP, UC, LC)               \
    DECL(lcb_STATUS lcb_resp##LC##_flags(const lcb_RESP##UC *resp, \
                                         lcb_U64 *dest))           \
    IMP({                                                          \
        *dest = resp->rflags;                                      \
        return LCB_SUCCESS;                                        \
    })

#define PYCBC_HOST_ACCESSORS(DECL, IMP, UC, LC)              \
    DECL(lcb_STATUS lcb_cmd##LC##_host(                      \
            lcb_CMD##UC *cmd, const char *host, size_t len)) \
    IMP({                                                    \
        cmd->host = host;                                    \
        return LCB_SUCCESS;                                  \
    })

#define PYCBC_RESP_ACCESSORS(DECL, IMPL, UC, LC) \
    PYCBC_KEY_ACCESSORS(DECL, IMPL, UC, LC)      \
    PYCBC_NOKEY_ACCESSORS(DECL, IMPL, UC, LC)

#define PYCBC_GET_ACCESSORS(DECL, IMPL, UC, LC) \
    PYCBC_RESP_ACCESSORS(DECL, IMPL, UC, LC)    \
    PYCBC_VAL_ACCESSORS(DECL, IMPL, UC, LC)     \
    PYCBC_ITMFLAGS_ACCESSORS_U32(DECL, IMPL, UC, LC)

#define PYCBC_STATS_ACCESSORS(DECL, IMPL, UC, LC) \
    PYCBC_RESP_ACCESSORS(DECL, IMPL, UC, LC)      \
    PYCBC_VAL_ACCESSORS(DECL, IMPL, UC, LC)       \
    PYCBC_FLAGS_ACCESSORS_U64(DECL, IMPL, UC, LC)

#define PYCBC_COUNT_ACCESSORS(DECL, IMPL, UC, LC) \
    PYCBC_RESP_ACCESSORS(DECL, IMPL, UC, LC)      \
    PYCBC_LLUVAL_ACCESSORS(DECL, IMPL, UC, LC)

#define PYCBC_ENDURE_ACCESSORS(DECL, IMPL, UC, LC) \
    PYCBC_RESP_ACCESSORS(DECL, IMPL, UC, LC)
#define PYCBC_OBSERVE_ACCESSORS(DECL, IMPL, UC, LC) \
    PYCBC_RESP_ACCESSORS(DECL, IMPL, UC, LC)

#define PYCBC_X_FOR_EACH_OP_POSTFIX(POSTFIX, DECL, IMP)             \
    PYCBC_RESP_ACCESSORS_POSTFIX(DECL, IMP, REMOVE, remove)         \
    PYCBC_RESP_ACCESSORS_POSTFIX(DECL, IMP, UNLOCK, unlock)         \
    PYCBC_RESP_ACCESSORS_POSTFIX(DECL, IMP, TOUCH, touch)           \
    PYCBC_GET_ACCESSORS_POSTFIX(DECL, IMP, GET, get)                \
    PYCBC_RESP_ACCESSORS_POSTFIX(DECL, IMP, GETREPLICA, getreplica) \
    PYCBC_COUNT_ACCESSORS_POSTFIX(DECL, IMP, COUNTER, counter)      \
    PYCBC_STATS_ACCESSORS_POSTFIX(DECL, IMP, STATS, stats)          \
    PYCBC_NOKEY_ACCESSORS_POSTFIX(DECL, IMP, PING, ping)            \
    PYCBC_NOKEY_ACCESSORS_POSTFIX(DECL, IMP, DIAG, diag)            \
    PYCBC_HTTP_ACCESSORS_POSTFIX(DECL, IMP, HTTP, http)             \
    PYCBC_ENDURE_ACCESSORS_POSTFIX(DECL, IMP, ENDURE, endure)       \
    PYCBC_OBSERVE_ACCESSORS_POSTFIX(DECL, IMP, OBSERVE, observe)

#define PYCBC_DECL(...) __VA_ARGS__;
#define PYCBC_IMPL(...) __VA_ARGS__
#define PYCBC_DUMMY(...)
PYCBC_X_FOR_EACH_OP_POSTFIX(, PYCBC_DECL, PYCBC_DUMMY)

typedef lcb_SDENTRY pycbc_SDENTRY;

lcb_STATUS pycbc_respsubdoc_status(const pycbc_SDENTRY *ent);
pycbc_strn_base_const pycbc_respsubdoc_value(const pycbc_SDENTRY *ent);

int pycbc_sdresult_next(const lcb_RESPSUBDOC *resp,
                        pycbc_SDENTRY *dest,
                        size_t *index);

void pycbc_cmdsubdoc_flags_from_scv(unsigned int sd_doc_flags, lcb_CMDSUBDOC *cmd);

typedef enum {
    LCB_PING_STATUS_OK = LCB_PINGSTATUS_OK,
    LCB_PING_STATUS_TIMEOUT = LCB_PINGSTATUS_TIMEOUT

} pycbc_ping_STATUS;

struct lcb_SUBDOCOPS;

lcb_STATUS lcb_subdocops_create(lcb_SUBDOCOPS **operations, size_t capacity);
lcb_STATUS lcb_cmdsubdoc_operations(lcb_CMDSUBDOC *cmd,
                                    const lcb_SUBDOCOPS *operations);

lcb_STATUS lcb_subdocops_destroy(lcb_SUBDOCOPS *operations);
#define LCB_PING_GET_TYPE_S(X, Y)  \
    case LCB_PINGSVC_##X: \
        return #Y;

#define PYCBC_PP_ENCRYPT_CONSTANTS(X)\
    X(LCBCRYPTO_KEY_ENCRYPT)\
    X(LCBCRYPTO_KEY_DECRYPT)

typedef lcb_U64 lcb_STORE_OPERATION;



#    define PYCBC_SDSPEC_SET_XX(POSTFIX, DEST, BUF, BUF_LEN)  \
        {                                                     \
            if (BUF && BUF_LEN) {                             \
                LCB_SDSPEC_SET_##POSTFIX(DEST, BUF, BUF_LEN); \
            }                                                 \
        }
#    define PYCBC_SDSPEC_SET_PATH(DEST, BUF, BUF_LEN) \
        PYCBC_SDSPEC_SET_XX(PATH, DEST, BUF, BUF_LEN)
#    define PYCBC_SDSPEC_SET_VALUE(DEST, BUF, BUF_LEN) \
        PYCBC_SDSPEC_SET_XX(VALUE, DEST, BUF, BUF_LEN)
#define PYCBC_PATH_ONLY(UC, LC, EXP_TYPE)                                  \
    DECL_##EXP_TYPE(lcb_subdocops_##LC(lcb_SUBDOCOPS *operations,          \
                                       size_t index,                       \
                                       uint32_t flags,                     \
                                       const char *path,                   \
                                       size_t path_len)) IMPL_##EXP_TYPE({ \
        PYCBC_SDSPEC_SET_PATH(&operations->specs[index], path, path_len);  \
        operations->specs[index].options = flags;                          \
        operations->specs[index].sdcmd = LCB_SDCMD_##UC;                   \
        return LCB_SUCCESS;                                                \
    })
#define PYCBC_COUNTER(UC, LC, EXP_TYPE)                                      \
    DECL_##EXP_TYPE(lcb_subdocops_##LC(lcb_SUBDOCOPS *operations,            \
                                       size_t index,                         \
                                       uint32_t flags,                       \
                                       const char *path,                     \
                                       size_t path_len,                      \
                                       int64_t delta)) IMPL_##EXP_TYPE({     \
        char *value = (char *)calloc(22, sizeof(char));                      \
        size_t value_len = snprintf(value, 21, "%" PRId64, delta);           \
        PYCBC_SDSPEC_SET_PATH(&operations->specs[index], path, path_len);    \
        PYCBC_SDSPEC_SET_VALUE(&operations->specs[index], value, value_len); \
        operations->specs[index].options = flags;                            \
        operations->specs[index].sdcmd = LCB_SDCMD_##UC;                     \
        return LCB_SUCCESS;                                                  \
    })
#define PYCBC_NP(UC, LC, EXP_TYPE)                                    \
    DECL_##EXP_TYPE(lcb_subdocops_##LC(                               \
            lcb_SUBDOCOPS *operations, size_t index, uint32_t flags)) \
            IMPL_##EXP_TYPE({                                         \
                operations->specs[index].options = flags;             \
                operations->specs[index].sdcmd = LCB_SDCMD_##UC;      \
                return LCB_SUCCESS;                                   \
            })
#define PYCBC_VAL_GEN(UC, LC, EXP_TYPE)                                      \
    DECL_##EXP_TYPE(lcb_subdocops_##LC(lcb_SUBDOCOPS *operations,            \
                                       size_t index,                         \
                                       uint32_t flags,                       \
                                       const char *path,                     \
                                       size_t path_len,                      \
                                       const char *value,                    \
                                       size_t value_len)) IMPL_##EXP_TYPE({  \
        PYCBC_SDSPEC_SET_PATH(&operations->specs[index], path, path_len);    \
        PYCBC_SDSPEC_SET_VALUE(&operations->specs[index], value, value_len); \
        operations->specs[index].options = flags;                            \
        operations->specs[index].sdcmd = LCB_SDCMD_##UC;                     \
        return LCB_SUCCESS;                                                  \
    })

#define PYCBC_SDCMD_FN_DEF(UC, LC, FN, EXP_TYPE) lcb_STATUS FN(UC, LC, EXP_TYPE)
#define PYCBC_SDCMD_CASE(UC, LC, EXP_TYPE) \
    PYCBC_SDCMD_FN_DEF(UC, LC, PYCBC_PATH_ONLY, EXP_TYPE)
#define PYCBC_SDCMD_CASE_NP(UC, LC, EXP_TYPE) \
    PYCBC_SDCMD_FN_DEF(UC, LC, PYCBC_NP, EXP_TYPE)
#define PYCBC_SDCMD_CASE_VAL(UC, LC, EXP_TYPE) \
    PYCBC_SDCMD_FN_DEF(UC, LC, PYCBC_VAL_GEN, EXP_TYPE)
#define PYCBC_SDCMD_CASE_MVAL(UC, LC, EXP_TYPE) \
    PYCBC_SDCMD_FN_DEF(UC, LC, PYCBC_VAL_GEN, EXP_TYPE)
#define PYCBC_SDCMD_CASE_COUNTER(UC, LC, EXP_TYPE) \
    PYCBC_SDCMD_FN_DEF(UC, LC, PYCBC_COUNTER, EXP_TYPE)

#    define PYCBC_X_SD_OPS_FULLDOC(X, NP, VAL, MVAL, CTR, ...) \
        NP(GET_FULLDOC, get_fulldoc, __VA_ARGS__)              \
        X(SET_FULLDOC, set_fulldoc, __VA_ARGS__)               \
        NP(REMOVE_FULLDOC, remove_fulldoc, __VA_ARGS__)


#    define lcb_subdoc lcb_subdoc3

#define DUMMY(...)
#    define PYCBC_SD_OPS_GEN
#undef PYCBC_SD_OPS_GEN
#    ifdef PYCBC_SD_OPS_GEN
PYCBC_X_SD_OPS(PYCBC_SDCMD_CASE,
               PYCBC_SDCMD_CASE_NP,
               PYCBC_SDCMD_CASE_VAL,
               PYCBC_SDCMD_CASE_MVAL,
               PYCBC_SDCMD_CASE_COUNTER,
               DECL)
#else
PYCBC_X_SD_OPS(PYCBC_SDCMD_CASE,
               PYCBC_SDCMD_CASE_NP,
               PYCBC_SDCMD_CASE_VAL,
               PYCBC_SDCMD_CASE_MVAL,
               PYCBC_SDCMD_CASE_COUNTER,
               DECL)
#    endif


#define lcb_cmdping_all(CMD) (CMD)->services = LCB_PINGSVC_F_KV | LCB_PINGSVC_F_N1QL | \
LCB_PINGSVC_F_VIEWS | LCB_PINGSVC_F_FTS;

#define lcb_cmdping_encode_json(CMD, ENABLE, PRETTY, DETAILS)\
        CMD->options = (CMD->options&~LCB_PINGOPT_F_JSON) | (ENABLE?LCB_PINGOPT_F_JSON:0);\
        CMD->options = (CMD->options&~LCB_PINGOPT_F_JSONPRETTY) | (PRETTY?LCB_PINGOPT_F_JSONPRETTY:0);\
        CMD->options = (CMD->options&~LCB_PINGOPT_F_JSONDETAILS) | (ENABLE?LCB_PINGOPT_F_JSONDETAILS:0);


struct lcb_CMDHTTP;

void lcb_cmdhttp_path(lcb_CMDHTTP *htcmd, const char *path, size_t length);
typedef lcb_U64 lcb_STORE_OPERATION;

lcb_STATUS pycbc_cmdn1ql_multiauth(lcb_CMDN1QL* cmd, int enable);
lcb_STATUS pycbc_cmdanalytics_host(lcb_CMDANALYTICS *CMD, const char *host);

#define CMDSCOPE_CREATECMD_RAW_V3(UC, LC, CMD, ...) \
    lcb_CMD##UC CMD##_real = {0};                   \
    lcb_CMD##UC *CMD = &CMD##_real;

#define CMDSCOPE_CREATECMD_V3(UC, LC, CMD, ...) \
    lcb_CMD##UC CMD##_real = {0};               \
    lcb_CMD##UC *CMD = &CMD##_real;             \
    lcb_cmd##LC##_create(&(CMD), __VA_ARGS__)

#define CMDSCOPE_DESTROYCMD_V3(UC, LC, CMD, ...) 0

#define CMDSCOPE_DESTROYCMD_RAW_V3(UC, LC, CMD, ...) 0
#define CMDSCOPE_CREATECMD_RAW(UC, LC, CMD, ...) \
    CMDSCOPE_CREATECMD_RAW_V3(UC, LC, CMD, __VA_ARGS__)

#define CMDSCOPE_CREATECMD(UC, LC, CMD, ...) \
    CMDSCOPE_CREATECMD_V3(UC, LC, CMD, __VA_ARGS__)

#define CMDSCOPE_DESTROYCMD(UC, LC, CMD, ...) \
    CMDSCOPE_DESTROYCMD_V3(UC, LC, CMD, __VA_ARGS__)

#define CMDSCOPE_DESTROYCMD_RAW(UC, LC, CMD, ...) \
    CMDSCOPE_DESTROYCMD_RAW_V3(UC, LC, CMD, __VA_ARGS__)

#define PYCBC_CMD_SET_TRACESPAN(TYPE, CMD, SPAN) \
    LCB_CMD_SET_TRACESPAN((CMD), (SPAN));

#define GENERIC_SPAN_OPERAND(SCOPE, INSTANCE, CMD, HANDLE, CONTEXT) \
    lcb_cmd##SCOPE##_parent_span(INSTANCE, HANDLE, (CONTEXT)->span)
#define GENERIC_NULL_OPERAND(SCOPE, INSTANCE, CMD, HANDLE, CONTEXT, COMMAND, ...) \
    lcb_##SCOPE(INSTANCE, __VA_ARGS__);
#define UNSCOPED_OPERAND(SCOPE, INSTANCE, CMD, HANDLE, CONTEXT, COMMAND, ...) lcb_##SCOPE(INSTANCE, __VA_ARGS__)
#define VIEW_SPAN_OPERAND(SCOPE, INSTANCE, CMD, HANDLE, CONTEXT) lcb_cmdview_parent_span(INSTANCE, HANDLE, (CONTEXT)->span)

#define PYCBC_LOG_KEY(CMD, key)                     \
    PYCBC_DEBUG_LOG("setting trace span on %.*s\n", \
                    (int)(CMD)->key.contig.nbytes,  \
                    (const char *)(CMD)->key.contig.bytes);

typedef lcb_RESPVIEWQUERY lcb_RESPVIEW;

typedef lcb_error_t lcb_STATUS;
#    define lcb_cmdremove_durability_observe(...) LCB_EINTERNAL
#    define lcb_cmdstore_durability_observe(...) LCB_EINTERNAL
lcb_STATUS lcb_cmdremove_durability(lcb_CMDREMOVE *cmd,
                                    pycbc_DURABILITY_LEVEL level);

#define PYCBC_DURABILITY 0

#define PYCBC_X_DURLEVEL(X)
#define PYCBX_X_SYNCREPERR(X)
#define PYCBC_LCB_ERRTYPES(X) \
    X(LCB_ERRTYPE_DATAOP);    \
    X(LCB_ERRTYPE_FATAL);     \
    X(LCB_ERRTYPE_INTERNAL);  \
    X(LCB_ERRTYPE_NETWORK);   \
    X(LCB_ERRTYPE_TRANSIENT); \
    X(LCB_ERRTYPE_INPUT);

#endif // COUCHBASE_PYTHON_CLIENT_LCB_V4_BACKPORT_H
