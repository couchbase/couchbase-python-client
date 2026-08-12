/*
 *   Copyright 2016-2026. Couchbase, Inc.
 *   All Rights Reserved.
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
 */

#pragma once

#include "Python.h"

// PyMemberDef type/flag aliases.
// The Py_T_* / Py_READONLY names were added to descrobject.h in Python 3.12.
// On earlier versions the legacy names live in structmember.h.  Always normalize
// on the Py_-prefixed names so call sites can avoid version guards.
#if !defined(Py_T_OBJECT_EX)
#include "structmember.h"
#define Py_T_OBJECT_EX T_OBJECT_EX
#define Py_T_INT T_INT
#define Py_T_LONG T_LONG
#define Py_T_DOUBLE T_DOUBLE
#define Py_T_STRING T_STRING
#define Py_T_BOOL T_BOOL
#define Py_READONLY READONLY
#endif

// PyUnicode_AsUTF8 was only added to the limited API in CPython 3.13.  Below
// that floor, route call sites through PyUnicode_AsUTF8AndSize (limited API
// since 3.10), which returns the same const char* and accepts a nullptr size.
// This is a macro so existing call sites compile unchanged.
#if defined(Py_LIMITED_API) && Py_LIMITED_API < 0x030D0000
#define PyUnicode_AsUTF8(obj) PyUnicode_AsUTF8AndSize((obj), nullptr)
#endif

namespace pycbc
{

// Heap-type instances hold a reference to their type (PyType_GenericAlloc increfs it), so every
// tp_dealloc must free via the type's own Py_tp_free slot and then release that reference.
// Going through the slot also keeps a Py_TPFLAGS_BASETYPE type correct when Python subclasses it:
// the subclass is GC-allocated, so it needs PyObject_GC_Del rather than PyObject_Free.
inline void
free_heap_type_instance(PyObject* self)
{
  PyTypeObject* self_type = Py_TYPE(self);
  freefunc tp_free = (freefunc)PyType_GetSlot(self_type, Py_tp_free);
  if (tp_free != nullptr) {
    tp_free(self);
  }
  Py_DECREF(self_type);
}

} // namespace pycbc

// Registering a heap type with a module is now just PyModule_AddType(module,
// (PyTypeObject*)type_obj) at the call site: it derives the module attribute name from the
// type's own tp_name (e.g. "pycbc_core.pycbc_result" -> "pycbc_result"), so there's no longer
// a separate name argument that could drift from the spec, and no manual INCREF/DECREF dance
// -- PyModule_AddType and PyModule_AddObjectRef (for non-type objects) never steal the caller's
// reference. Both are limited-API safe from our 3.10 floor. PYCBC-1854 (v4.7.0) dropped the
// register_pytype/register_heap_type wrappers this replaced, along with the last static
// PyTypeObject they served.
