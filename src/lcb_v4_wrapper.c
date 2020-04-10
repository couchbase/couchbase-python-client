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

#include "pycbc.h"
#include "lcb_v4_wrapper.h"
#include <libcouchbase/crypto.h>

lcb_STATUS pycbc_crypto_register(lcb_INSTANCE* instance, const char *name, lcbcrypto_PROVIDER *provider){
    lcbcrypto_register(instance,name,provider);
    return LCB_SUCCESS;
}

lcb_STATUS pycbc_crypto_unregister(lcb_INSTANCE* instance, const char *name){
    lcbcrypto_unregister(instance,name);
    return LCB_SUCCESS;
}

lcb_STATUS pycbc_encrypt_fields(lcb_INSTANCE* instance, lcbcrypto_CMDENCRYPT* cmd)
{
    return lcbcrypto_encrypt_fields(instance,cmd);
}

lcb_STATUS pycbc_decrypt_fields(lcb_INSTANCE* instance, lcbcrypto_CMDDECRYPT* cmd) {
    return lcbcrypto_decrypt_fields(instance,cmd);
}


lcb_STATUS pycbc_respsubdoc_status(const pycbc_SDENTRY *ent)
{
    return lcb_respsubdoc_result_status(ent->resp, ent->index);
}

pycbc_strn_base_const pycbc_respsubdoc_value(const pycbc_SDENTRY *ent)
{
    pycbc_strn_base_const result;
    lcb_respsubdoc_result_value(
            ent->resp, ent->index, &result.buffer, &result.length);
    return result;
}

void pycbc_cmdsubdoc_flags_from_scv(unsigned int sd_doc_flags, lcb_CMDSUBDOC *cmd) {

    if  (sd_doc_flags & CMDSUBDOC_F_UPSERT_DOC)
    {
        lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);
    }
    if  (sd_doc_flags & CMDSUBDOC_F_INSERT_DOC)
    {
        lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_INSERT);
    }
}


int pycbc_sdresult_next(const lcb_RESPSUBDOC *resp,
                        pycbc_SDENTRY *dest,
                        size_t *index)
{
    if (((*index) + 1) > lcb_respsubdoc_result_size(resp)) {
        return 0;
    }
    *dest = (pycbc_SDENTRY){.resp = resp, .index = *index};
    ++(*index);
    return 1;
}

uint64_t pycbc_mutation_token_seqno(const lcb_MUTATION_TOKEN *pToken)
{
    return pToken->seqno_;
}

uint64_t pycbc_mutation_token_vbid(const lcb_MUTATION_TOKEN *pToken)
{
    return pToken->vbid_;
}

uint64_t pycbc_mutation_token_uuid(const lcb_MUTATION_TOKEN *pToken)
{
    return pToken->uuid_;
}

/*
 * Support removed from LCB V4 until we find a use-case as per CCBC-1051
 * */

const lcb_MUTATION_TOKEN *pycbc_get_vbucket_mutation_token(
        lcb_INSTANCE *instance, lcb_KEYBUF *kb, lcb_STATUS *rc)
{
    (void)instance;
    (void)kb;
    *rc = LCB_ERR_UNSUPPORTED_OPERATION;
    return NULL;
}

lcb_STATUS pycbc_cmdquery_multiauth(lcb_CMDQUERY *cmd, int enable)
{
    (void)cmd;
    (void)enable;
    return LCB_ERR_UNSUPPORTED_OPERATION;
}

lcb_STATUS pycbc_cmdview_spatial(lcb_CMDVIEW *pCmdview, int is_spatial)
{
    (void)pCmdview;
    (void)is_spatial;
    return LCB_ERR_UNSUPPORTED_OPERATION;
}

