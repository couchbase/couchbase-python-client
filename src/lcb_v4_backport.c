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

#include "lcb_v4_backport.h"
#include "pycbc.h"

lcb_STATUS pycbc_crypto_register(lcb_INSTANCE *instance,
                                 const char *name,
                                 lcbcrypto_PROVIDER *provider)
{
    lcbcrypto_register(instance, name, provider);
    return LCB_SUCCESS;
}

lcb_STATUS pycbc_crypto_unregister(lcb_INSTANCE *instance, const char *name)
{
    lcbcrypto_unregister(instance, name);
    return LCB_SUCCESS;
}

lcb_STATUS pycbc_encrypt_fields(lcb_INSTANCE* instance, lcbcrypto_CMDENCRYPT* cmd)
{
#if PYCBC_LCB_API>0x02FF00
    return lcbcrypto_encrypt_fields(instance, &cmd);
#else
    return lcbcrypto_encrypt_fields(instance, cmd);
#endif
}

lcb_STATUS pycbc_decrypt_fields(lcb_INSTANCE* instance, lcbcrypto_CMDDECRYPT* cmd) {
#if PYCBC_LCB_API>0x02FF00
    return lcbcrypto_decrypt_fields(instance, &cmd);
#else
    return lcbcrypto_decrypt_fields(instance, cmd);
#endif
}

lcb_STATUS pycbc_respsubdoc_status(const pycbc_SDENTRY *ent)
{
    return ent->status;
}

pycbc_strn_base_const pycbc_respsubdoc_value(const pycbc_SDENTRY *ent)
{
    pycbc_strn_base_const result;
    result.buffer = ent->value;
    result.length = ent->nvalue;
    return result;
}

lcb_STATUS lcb_respview_http_response(const lcb_RESPVIEW *resp,
                                      const lcb_RESPHTTP **dest)
{
    *(dest) = resp->htresp;
    return LCB_SUCCESS;
}

int pycbc_sdresult_next(const lcb_RESPSUBDOC *resp,
                        pycbc_SDENTRY *dest,
                        size_t *index)
{
    return lcb_sdresult_next(resp, dest, index);
}

uint64_t pycbc_mutation_token_seqno(const lcb_MUTATION_TOKEN *pToken)
{
    return LCB_MUTATION_TOKEN_SEQ(pToken);
}

uint64_t pycbc_mutation_token_vbid(const lcb_MUTATION_TOKEN *pToken)
{
    return LCB_MUTATION_TOKEN_VB(pToken);
}

uint64_t pycbc_mutation_token_uuid(const lcb_MUTATION_TOKEN *pToken)
{
    return LCB_MUTATION_TOKEN_ID(pToken);
}

int lcb_mutation_token_is_valid(const lcb_MUTATION_TOKEN *pTOKEN)
{
    return LCB_MUTATION_TOKEN_ISVALID(pTOKEN);
}

const lcb_MUTATION_TOKEN *pycbc_get_vbucket_mutation_token(
        lcb_INSTANCE *instance, lcb_KEYBUF *kb, lcb_STATUS *rc)
{
    return lcb_get_mutation_token(instance, kb, rc);
}

PYCBC_X_FOR_EACH_OP_POSTFIX(, PYCBC_IMPL, PYCBC_IMPL)

lcb_STATUS lcb_cmdget_key(lcb_CMDBASE *ctx, pycbc_pybuffer *buf)
{
    LCB_CMD_SET_KEY(ctx, buf->buffer, buf->length);
    return LCB_SUCCESS;
}

GET_ATTRIBS(PYCBC_SCOPE_SET)

void lcb_cmdgetreplica_create(lcb_CMDGETREPLICA **pcmd, int strategy)
{
    (*pcmd)->strategy = strategy;
    switch (strategy) {
    case LCB_REPLICA_MODE_ANY:
        (*pcmd)->strategy = LCB_REPLICA_FIRST;
        break;
    case LCB_REPLICA_MODE_ALL:
        (*pcmd)->strategy = LCB_REPLICA_ALL;
        break;
    case LCB_REPLICA_MODE_IDX0:
        (*pcmd)->strategy = LCB_REPLICA_SELECT;
        (*pcmd)->index = 0;
        break;
    case LCB_REPLICA_MODE_IDX1:
        (*pcmd)->strategy = LCB_REPLICA_SELECT;
        (*pcmd)->index = 1;
        break;
    case LCB_REPLICA_MODE_IDX2:
        (*pcmd)->strategy = LCB_REPLICA_SELECT;
        (*pcmd)->index = 2;
        break;
    default:
        break;
    }
}
#include "pycbc_subdocops.h"
lcb_STATUS lcb_subdocops_create(lcb_SUBDOCOPS **operations, size_t capacity)
{
    lcb_SUBDOCOPS *res = (lcb_SUBDOCOPS *)calloc(1, sizeof(lcb_SUBDOCOPS));
    res->nspecs = capacity;
    res->specs = (lcb_SDSPEC *)calloc(res->nspecs, sizeof(lcb_SDSPEC));
    *operations = res;
    return LCB_SUCCESS;
}
lcb_STATUS lcb_cmdsubdoc_operations(lcb_CMDSUBDOC *cmd,
                                    const lcb_SUBDOCOPS *operations)
{
    cmd->specs = operations->specs;
    cmd->nspecs = operations->nspecs;
#ifdef PYCBC_DEBUG_SUBDOC
    for (size_t i = 0; i < cmd->nspecs; ++i) {
        PYCBC_DEBUG_LOG(
                "Command %d: {.cmd=%d, .options=%d, path=%.*s,value=%.*s}",
                i,
                operations->specs[i].sdcmd,
                operations->specs[i].options,
                operations->specs[i].path.contig.nbytes,
                operations->specs[i].path.contig.bytes,
                operations->specs[i].value.u_buf.contig.nbytes,
                operations->specs[i].value.u_buf.contig.bytes)
    }
#endif
    return LCB_SUCCESS;
}

void pycbc_cmdsubdoc_flags_from_scv(unsigned int sd_doc_flags, lcb_CMDSUBDOC *cmd) {
    cmd->cmdflags |= sd_doc_flags;
}

lcb_STATUS lcb_subdocops_destroy(lcb_SUBDOCOPS *operations)
{
    if (operations) {
        if (operations->specs) {
            size_t ii;
            for (ii = 0; ii < operations->nspecs; ii++) {
                if (operations->specs[ii].sdcmd == LCB_SDCMD_COUNTER) {
                    free((void *)operations->specs[ii]
                                 .value.u_buf.contig.bytes);
                }
            }
        }
        free(operations->specs);
    }
    free(operations);
    return LCB_SUCCESS;
}

lcb_STATUS lcb_cmdremove_durability(lcb_CMDREMOVE *cmd,
                                    pycbc_DURABILITY_LEVEL level)
{
#if PYCBC_LCB_API>0x02FF00
    cmd->dur_level=level;
    return LCB_SUCCESS;
#else
    return LCB_NOT_SUPPORTED;
#endif
}

lcb_STATUS lcb_cmdstore_durability(lcb_CMDSTORE *cmd,
                                   pycbc_DURABILITY_LEVEL level)
{
#if PYCBC_LCB_API>0x02FF00
    cmd->dur_level=level;
    return LCB_SUCCESS;
#else
    return LCB_NOT_SUPPORTED;
#endif
}

/**
 * Implementations of all the LCB API V4 subdocument operations */

PYCBC_X_SD_OPS(PYCBC_SDCMD_CASE,
               PYCBC_SDCMD_CASE_NP,
               PYCBC_SDCMD_CASE_VAL,
               PYCBC_SDCMD_CASE_MVAL,
               PYCBC_SDCMD_CASE_COUNTER,
               IMPL)

void lcb_cmdhttp_path(lcb_CMDHTTP *htcmd, const char *path, size_t length)
{
    {
        pycbc_pybuffer pathbuf = {NULL, path, length};
        PYCBC_CMD_SET_KEY_SCOPE(http, htcmd, pathbuf);
    }
}

lcb_STATUS pycbc_cmdn1ql_multiauth(lcb_CMDN1QL* cmd, int enable) {

    (cmd)->cmdflags = ((cmd)->cmdflags & ~LCB_CMD_F_MULTIAUTH) | ((enable)? LCB_CMD_F_MULTIAUTH: 0);
    return LCB_SUCCESS;
}

lcb_STATUS pycbc_cmdanalytics_host(lcb_CMDANALYTICS *CMD, const char *host) {
    CMD->cmdflags |= LCB_CMDN1QL_F_ANALYTICSQUERY;
    CMD->host = host;
    return LCB_SUCCESS;
}

lcb_STATUS lcb_n1ql(lcb_t instance, const void *cookie, const lcb_CMDN1QL *cmd)
{
    return lcb_n1ql_query(instance, cookie, cmd);
}
lcb_STATUS lcb_analytics(lcb_t instance,
                         const void *cookie,
                         const lcb_CMDN1QL *cmd)
{
    return lcb_n1ql_query(instance, cookie, cmd);
}

lcb_STATUS lcb_respfts_http_response(const lcb_RESPFTS *resp, const lcb_RESPHTTP **ptr) {
    *ptr=resp->htresp;
    return LCB_SUCCESS;
}

lcb_STATUS lcb_respfts_row(const lcb_RESPFTS *resp, const char **pString, size_t *pInt) {
    *pString=resp->row;
    *pInt=resp->nrow;
    return LCB_SUCCESS;
}

int lcb_respfts_is_final(const lcb_RESPFTS *resp) {
    return resp->rflags && LCB_RESP_F_FINAL;
}

lcb_STATUS lcb_respfts_status(const lcb_RESPFTS *resp) {
    return resp->rc;
}


lcb_STATUS lcb_cmdfts_callback(lcb_CMDFTS *cmd, void (*callback)(lcb_t, int, const lcb_RESPFTS *)) {
    cmd->callback=callback;
    return LCB_SUCCESS;
}

lcb_STATUS lcb_cmdfts_query(lcb_CMDFTS *cmd, const void *pVoid, size_t length) {
    cmd->query=pVoid;
    cmd->nquery=length;
    return LCB_SUCCESS;
}

lcb_STATUS lcb_cmdfts_handle(lcb_CMDFTS *cmd, pycbc_FTS_HANDLE *pFTSREQ) {
    cmd->handle=pFTSREQ;
    return LCB_SUCCESS;
}

lcb_STATUS pycbc_cmdview_spatial(lcb_CMDVIEW *pCmdview, int is_spacial)
{
    return lcb_cmdview_spatial(pCmdview, is_spacial);
}

pycbc_strn_base_const pycbc_view_geometry(const lcb_RESPVIEW *ctx)
{
    pycbc_strn_base_const temp;
    lcb_respview_geometry(ctx, &temp.buffer, &temp.length);
    return temp;
};
