//
// Created by Ellis Breen on 2019-06-28.
//

#include "util_wrappers.h"
#include <stdlib.h>
const char *pycbc_strn_buf(pycbc_strn buf)
{
    return buf.buffer;
}

int pycbc_strn_valid(pycbc_strn buf)
{
    return buf.buffer ? 1 : 0;
}

size_t pycbc_strn_len(pycbc_strn_base_const buf)
{
    return buf.length;
}

char *pycbc_strn_buf_psz(pycbc_strn_unmanaged buf)
{
    return buf.content.buffer;
}

void pycbc_strn_free(pycbc_strn_unmanaged buf)
{
    if (pycbc_strn_valid(buf.content)) {
        free((void *)buf.content.buffer);
    }
}

pycbc_generic_array pycbc_strn_base_const_array(pycbc_strn_base_const orig)
{
    return (pycbc_generic_array){orig.buffer, orig.length};
}