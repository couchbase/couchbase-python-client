#  Copyright 2016-2026. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypedDict

# ============================================================================
# bindings.yaml config types
# ============================================================================


class BindingConfigOpRequest(TypedDict, total=False):
    core_struct: str
    header_path: str | None = None
    required_fields: list[str] | None = None
    skip_if_empty_fields: list[str] | None = None
    with_legacy_durability: bool | None = None


class BindingConfigOpResponse(TypedDict, total=False):
    core_struct: str
    header_path: str | None = None
    input_missing_err_msg: str | None = None
    response_override: str | None = None


class BindingConfigOp(TypedDict, total=False):
    op_name: str
    header_file: str
    request: BindingConfigOpRequest
    response: BindingConfigOpResponse
    header_path: str | None = None
    skip_response: bool | None = None
    is_mutation: bool | None = None
    has_multi: bool | None = None
    is_streaming_op: bool | None = None
    cpp_request_templates: list[str] | None = None
    streaming_op_name: str | None = None

# override is the same as BindingOpConfig, but all except op_name are optional


class BindingConfigOpOverride(TypedDict, total=False):
    op_name: str
    header_file: str | None = None
    request: BindingConfigOpRequest | None = None
    response: BindingConfigOpResponse | None = None
    header_path: str | None = None
    skip_response: bool | None = None
    is_mutation: bool | None = None
    has_multi: bool | None = None
    is_streaming_op: bool | None = None
    cpp_request_templates: list[str] | None = None
    streaming_op_name: str | None = None


class BindingConfigKeyValueOperations(TypedDict):
    header_path: str
    structs_prefix: str
    ops_prefix: str
    mutation_operation_headers: list[str]
    operation_headers: list[str]
    with_legacy_durability: list[str]
    operation_overrides: list[BindingConfigOpOverride] | None = None
    required_cpp_request_fields: list[str] | None = None
    ignored_cpp_request_fields: list[str] | None = None
    ignored_cpp_response_fields: list[str] | None = None


class BindingConfigStreamingOperations(TypedDict):
    header_path: str
    structs_prefix: str
    ops_prefix: str
    operation_headers: list[str]
    operation_overrides: list[BindingConfigOpOverride] | None = None
    ignored_cpp_request_fields: list[str] | None = None
    ignored_cpp_response_fields: list[str] | None = None


class BindingConfigMgmtOperations(TypedDict):
    operation_headers: list[str]
    operation_overrides: list[BindingConfigOpOverride] | None = None
    header_path: str | None = None
    structs_prefix: str | None = None
    ignored_cpp_request_fields: list[str] | None = None
    ignored_cpp_response_fields: list[str] | None = None


class BindingConfigMgmt(TypedDict):
    header_path: str
    structs_prefix: str
    analytics: BindingConfigMgmtOperations
    bucket: BindingConfigMgmtOperations
    cluster: BindingConfigMgmtOperations
    collection: BindingConfigMgmtOperations
    ignored_cpp_request_fields: list[str] | None = None
    ignored_cpp_response_fields: list[str] | None = None


class BindingConfigCppType(TypedDict, total=False):
    core_struct: str
    header_file: str
    py_type: str | None = None
    required_fields: list[str] | None = None
    ignored_fields: list[str] | None = None
    skip_if_empty_fields: list[str] | None = None
    input_missing_err_msg: str | None = None
    skip_request: bool | None = None
    allow_empty_fields: bool | None = None


class BindingConfigCppTypes(TypedDict):
    types: list[BindingConfigCppType]
    ignored_fields: list[str] | None = None


class BindingConfigCppVariantAlternative(TypedDict, total=False):
    tag: str
    core_struct: str


class BindingConfigCppVariant(TypedDict, total=False):
    # Addressed either by core_type (a `using X = std::variant<...>` alias) or by
    # core_struct + field (an inline std::variant field declaration).  Exactly one form.
    core_type: str
    core_struct: str
    field: str
    header_file: str
    discriminator: str
    alternatives: list[BindingConfigCppVariantAlternative]
    input_missing_err_msg: str | None = None


class BindingConfigCppEnumType(TypedDict, total=False):
    core_enum: str
    header_file: str
    default: str | int
    enum_type: str
    py_type: str | None = None
    mapping_name: str | None = None
    use_hex: bool | None = False


class BindingConfigFileOutput:
    name: str
    filename: str
    templates: list[str] | str
    output_path: str


class BindingConfigMetadata(TypedDict):
    version: str
    description: str
    file_outputs: list[BindingConfigFileOutput]


class BindingConfigSchema(TypedDict):
    metadata: BindingConfigMetadata
    operation_headers: list[str]
    key_value: BindingConfigKeyValueOperations
    streaming: BindingConfigStreamingOperations
    management: BindingConfigMgmt
    cpp_core_type_headers: list[str]
    cpp_core_types: BindingConfigCppTypes
    cpp_core_variants: list[BindingConfigCppVariant]
    cpp_core_enums: list[BindingConfigCppEnumType]
    cpp_core_enum_headers: list[str]


# ============================================================================
# C++ parsed Types
# ============================================================================

class CppType(TypedDict):
    name: str


class CppField(TypedDict):
    name: str
    cpp_type: CppType | CppNestedField


class CppEnumValue(TypedDict):
    name: str
    value: int | str

# field can recurse


class CppNestedField(TypedDict):
    name: Literal['std::optional', 'std::shared_ptr', 'std::vector', 'std::map', 'std::variant']
    of: CppType | CppField
    to: CppType | CppField | None


class CppParsedType(TypedDict):
    struct_name: str
    fields: list[CppField]


class CppParsedEnum(TypedDict):
    name: str
    type: CppType
    values: list[CppEnumValue]

# ============================================================================
# Binding types (ready to go into the template generator)
# ============================================================================


@dataclass
class PyType:
    name: str
    module: str | None = None


@dataclass
class BindingMultiOpField:
    py_name: str
    py_type: str
    is_ignored: bool = False


@dataclass
class BindingOpField:
    cpp_name: str
    cpp_type: str
    py_name: str
    py_type: str
    is_required: bool = False
    is_ignored: bool = False
    skip_if_empty: bool = False


@dataclass
class BindingKeyValueOp:
    name: str                               # e.g. get
    pretty_name: str                        # e.g. GetWithLegacyDurability
    request_struct: str                     # e.g. get_request
    request_struct_full: str                # e.g. couchbase::core::operations::get_request
    request_fields: list[BindingOpField]
    response_struct: str                    # e.g. get_response
    response_struct_full: str               # e.g. couchbase::core::operations::get_response
    response_fields: list[BindingOpField]
    requires_doc_id: bool = True
    response_override: str | None = None
    is_mutation: bool | None = None
    request_only: bool | None = None
    has_multi: bool | None = None
    is_streaming_op: bool | None = None


@dataclass
class BindingKeyValueMultiOp:
    name: str                                 # e.g. get
    pretty_name: str
    options_name: str
    request_fields: list[BindingMultiOpField]
    option_fields: list[BindingMultiOpField]
    is_streaming_op: bool | None = None


@dataclass
class BindingStreamingOp:
    name: str
    pretty_name: str
    streaming_op_name: str
    request_struct: str
    request_struct_full: str
    request_fields: list[BindingOpField]
    request_only: bool | None = True


@dataclass
class BindingMgmtOp:
    name: str
    pretty_name: str
    request_struct: str
    request_struct_full: str
    request_fields: list[BindingOpField]
    response_struct: str
    response_struct_full: str
    response_fields: list[BindingOpField]
    request_only: bool | None = None
    is_templated: bool | None = None
    templated_pretty_name: str | None = None


@dataclass
class BindingMgmtGroup:
    name: str
    pretty_name: str
    operations: list[BindingMgmtOp]


@dataclass
class BindingCppType:
    name: str
    full_name: str
    fields: list[BindingOpField]
    header: str
    py_type: str
    input_missing_err_msg: str | None = None
    skip_request: bool | None = None


@dataclass
class BindingVariantAlternative:
    tag: str
    full_name: str                          # e.g. couchbase::core::management::eventing::function_url_auth_basic


@dataclass
class BindingCppVariant:
    name: str                               # the alias name, or <struct>_<field> for an inline variant
    full_name: str                          # the alias, or the expanded std::variant<...> for an inline variant
    header: str
    discriminator: str                      # Python dict key holding the alternative tag
    alternatives: list[BindingVariantAlternative]
    input_missing_err_msg: str | None = None


@dataclass
class BindingEnumValue:
    name: str
    content: str


@dataclass
class BindingEnumType:
    name: str
    full_name: str
    default: str
    enum_type: str  # "ENUM", "INT_ENUM" or "INT16_ENUM"
    mapping_name: str
    header: str
    enum_values: list[BindingEnumValue]
    py_type: str
