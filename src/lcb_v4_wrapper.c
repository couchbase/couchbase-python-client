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

#include "lcb_v4_wrapper.h"
#include "pycbc.h"
#include <libcouchbase/crypto.h>

// TODO: these crypto functions are now accessible but seem to return an error, so disabled for now

lcb_STATUS pycbc_crypto_register(lcb_INSTANCE* instance, const char *name, lcbcrypto_PROVIDER *provider){
#ifdef PYCBC_CRYPTO_ENABLED
    lcbcrypto_register(instance,name,provider);
    return LCB_SUCCESS;
#else
    (void)instance;
    (void)name;
    (void)provider;
    return LCB_NOT_SUPPORTED;
#endif
}

lcb_STATUS pycbc_crypto_unregister(lcb_INSTANCE* instance, const char *name){
#ifdef PYCBC_CRYPTO_ENABLED
    lcbcrypto_unregister(instance,name);
    return LCB_SUCCESS;
#else
    (void)instance;
    (void)name;
    return LCB_NOT_SUPPORTED;
#endif
}

lcb_STATUS pycbc_encrypt_fields(lcb_INSTANCE* instance, lcbcrypto_CMDENCRYPT* cmd)
{
#ifdef PYCBC_CRYPTO_ENABLED
    return lcbcrypto_encrypt_fields(instance,cmd);
#else
    (void)instance;
    (void)cmd;
    return LCB_NOT_SUPPORTED;
#endif
}

lcb_STATUS pycbc_decrypt_fields(lcb_INSTANCE* instance, lcbcrypto_CMDDECRYPT* cmd) {
#ifdef PYCBC_CRYPTO_ENABLED
    return lcbcrypto_decrypt_fields(instance,cmd);
#else
    (void)instance;
    (void)cmd;
    return LCB_NOT_SUPPORTED;
#endif
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

    lcb_cmdsubdoc_create_if_missing(cmd, (sd_doc_flags & CMDSUBDOC_F_UPSERT_DOC) ? 1 : 0);
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
    *rc = LCB_NOT_SUPPORTED;
    return NULL;
}

lcb_STATUS pycbc_cmdn1ql_multiauth(lcb_CMDN1QL* cmd, int enable) {
    (void)cmd;
    (void)enable;
    return LCB_NOT_SUPPORTED;
}

lcb_STATUS pycbc_cmdanalytics_host(lcb_CMDANALYTICS* CMD, const char* host)
{
    (void)CMD;
    (void)host;
    return LCB_NOT_SUPPORTED;
}

lcb_STATUS pycbc_cmdview_spatial(lcb_CMDVIEW *pCmdview, int is_spatial)
{
    (void)pCmdview;
    (void)is_spatial;
    return LCB_NOT_SUPPORTED;
}
