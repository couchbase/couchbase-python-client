from __future__ import annotations

from dataclasses import dataclass
from typing import (List,
                    Literal,
                    Optional,
                    TypedDict,
                    Union)

# ============================================================================
# bindings.yaml config types
# ============================================================================


class BindingConfigOpRequest(TypedDict, total=False):
    core_struct: str
    header_path: Optional[str] = None
    required_fields: Optional[List[str]] = None
    skip_if_empty_fields: Optional[List[str]] = None
    with_legacy_durability: Optional[bool] = None


class BindingConfigOpResponse(TypedDict, total=False):
    core_struct: str
    header_path: Optional[str] = None
    input_missing_err_msg: Optional[str] = None
    response_override: Optional[str] = None


class BindingConfigOp(TypedDict, total=False):
    op_name: str
    header_file: str
    request: BindingConfigOpRequest
    response: BindingConfigOpResponse
    header_path: Optional[str] = None
    skip_response: Optional[bool] = None
    is_mutation: Optional[bool] = None
    has_multi: Optional[bool] = None
    is_streaming_op: Optional[bool] = None
    cpp_request_templates: Optional[List[str]] = None
    streaming_op_name: Optional[str] = None

# override is the same as BindingOpConfig, but all except op_name are optional


class BindingConfigOpOverride(TypedDict, total=False):
    op_name: str
    header_file: Optional[str] = None
    request: Optional[BindingConfigOpRequest] = None
    response: Optional[BindingConfigOpResponse] = None
    header_path: Optional[str] = None
    skip_response: Optional[bool] = None
    is_mutation: Optional[bool] = None
    has_multi: Optional[bool] = None
    is_streaming_op: Optional[bool] = None
    cpp_request_templates: Optional[List[str]] = None
    streaming_op_name: Optional[str] = None


class BindingConfigKeyValueOperations(TypedDict):
    header_path: str
    structs_prefix: str
    ops_prefix: str
    mutation_operation_headers: List[str]
    operation_headers: List[str]
    with_legacy_durability: List[str]
    operation_overrides: Optional[List[BindingConfigOpOverride]] = None
    required_cpp_request_fields: Optional[List[str]] = None
    ignored_cpp_request_fields: Optional[List[str]] = None
    ignored_cpp_response_fields: Optional[List[str]] = None


class BindingConfigStreamingOperations(TypedDict):
    header_path: str
    structs_prefix: str
    ops_prefix: str
    operation_headers: List[str]
    operation_overrides: Optional[List[BindingConfigOpOverride]] = None
    ignored_cpp_request_fields: Optional[List[str]] = None
    ignored_cpp_response_fields: Optional[List[str]] = None


class BindingConfigMgmtOperations(TypedDict):
    operation_headers: List[str]
    operation_overrides: Optional[List[BindingConfigOpOverride]] = None
    header_path: Optional[str] = None
    structs_prefix: Optional[str] = None
    ignored_cpp_request_fields: Optional[List[str]] = None
    ignored_cpp_response_fields: Optional[List[str]] = None


class BindingConfigMgmt(TypedDict):
    header_path: str
    structs_prefix: str
    analytics: BindingConfigMgmtOperations
    bucket: BindingConfigMgmtOperations
    cluster: BindingConfigMgmtOperations
    collection: BindingConfigMgmtOperations
    ignored_cpp_request_fields: Optional[List[str]] = None
    ignored_cpp_response_fields: Optional[List[str]] = None


class BindingConfigCppType(TypedDict, total=False):
    core_struct: str
    header_file: str
    py_type: Optional[str] = None
    required_fields: Optional[List[str]] = None
    ignored_fields: Optional[List[str]] = None
    skip_if_empty_fields: Optional[List[str]] = None
    input_missing_err_msg: Optional[str] = None
    skip_request: Optional[bool] = None


class BindingConfigCppTypes(TypedDict):
    types: List[BindingConfigCppType]
    ignored_fields: Optional[List[str]] = None


class BindingConfigCppEnumType(TypedDict, total=False):
    core_enum: str
    header_file: str
    default: Union[str, int]
    enum_type: str
    py_type: Optional[str] = None
    mapping_name: Optional[str] = None
    use_hex: Optional[bool] = False


class BindingConfigFileOutput:
    name: str
    filename: str
    templates: Union[List[str], str]
    output_path: str


class BindingConfigMetadata(TypedDict):
    version: str
    description: str
    file_outputs: List[BindingConfigFileOutput]


class BindingConfigSchema(TypedDict):
    metadata: BindingConfigMetadata
    operation_headers: List[str]
    key_value: BindingConfigKeyValueOperations
    streaming: BindingConfigStreamingOperations
    management: BindingConfigMgmt
    cpp_core_type_headers: List[str]
    cpp_core_types: BindingConfigCppTypes
    cpp_core_enums: List[BindingConfigCppEnumType]
    cpp_core_enum_headers: List[str]


# ============================================================================
# C++ parsed Types
# ============================================================================

class CppType(TypedDict):
    name: str


class CppField(TypedDict):
    name: str
    cpp_type: Union[CppType, CppNestedField]


class CppEnumValue(TypedDict):
    name: str
    value: Union[int, str]

# field can recurse


class CppNestedField(TypedDict):
    name: Literal['std::optional', 'std::shared_ptr', 'std::vector', 'std::map', 'std::variant']
    of: Union[CppType, CppField]
    to: Optional[Union[CppType, CppField]]


class CppParsedType(TypedDict):
    struct_name: str
    fields: List[CppField]


class CppParsedEnum(TypedDict):
    name: str
    type: CppType
    values: List[CppEnumValue]

# ============================================================================
# Binding types (ready to go into the template generator)
# ============================================================================


@dataclass
class PyType:
    name: str
    module: Optional[str] = None


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
    request_fields: List[BindingOpField]
    response_struct: str                    # e.g. get_response
    response_struct_full: str               # e.g. couchbase::core::operations::get_response
    response_fields: List[BindingOpField]
    requires_doc_id: bool = True
    response_override: Optional[str] = None
    is_mutation: Optional[bool] = None
    request_only: Optional[bool] = None
    has_multi: Optional[bool] = None
    is_streaming_op: Optional[bool] = None


@dataclass
class BindingKeyValueMultiOp:
    name: str                                 # e.g. get
    pretty_name: str
    options_name: str
    request_fields: List[BindingMultiOpField]
    option_fields: List[BindingMultiOpField]
    is_streaming_op: Optional[bool] = None


@dataclass
class BindingStreamingOp:
    name: str
    pretty_name: str
    streaming_op_name: str
    request_struct: str
    request_struct_full: str
    request_fields: List[BindingOpField]
    request_only: Optional[bool] = True


@dataclass
class BindingMgmtOp:
    name: str
    pretty_name: str
    request_struct: str
    request_struct_full: str
    request_fields: List[BindingOpField]
    response_struct: str
    response_struct_full: str
    response_fields: List[BindingOpField]
    request_only: Optional[bool] = None
    is_templated: Optional[bool] = None
    templated_pretty_name: Optional[str] = None


@dataclass
class BindingMgmtGroup:
    name: str
    pretty_name: str
    operations: List[BindingMgmtOp]


@dataclass
class BindingCppType:
    name: str
    full_name: str
    fields: List[BindingOpField]
    header: str
    py_type: str
    input_missing_err_msg: Optional[str] = None
    skip_request: Optional[bool] = None


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
    enum_values: List[BindingEnumValue]
    py_type: str
