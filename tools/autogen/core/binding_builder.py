from __future__ import annotations

import os
from copy import deepcopy
from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Union)

from tools.autogen.core.binding_autogen_types import (BindingConfigCppEnumType,
                                                      BindingConfigCppTypes,
                                                      BindingConfigKeyValueOperations,
                                                      BindingConfigMgmt,
                                                      BindingConfigMgmtOperations,
                                                      BindingConfigOp,
                                                      BindingConfigOpOverride,
                                                      BindingConfigOpRequest,
                                                      BindingConfigOpResponse,
                                                      BindingConfigStreamingOperations,
                                                      BindingCppType,
                                                      BindingEnumType,
                                                      BindingEnumValue,
                                                      BindingKeyValueMultiOp,
                                                      BindingKeyValueOp,
                                                      BindingMgmtGroup,
                                                      BindingMgmtOp,
                                                      BindingMultiOpField,
                                                      BindingStreamingOp,
                                                      CppField,
                                                      CppParsedType,
                                                      CppType)
from tools.autogen.core.cpp_type_parser import CppTypeParser, CppTypeParserConfig


class BindingBuilder:

    CPP_PY_INT_TYPES = [
        'std::chrono::milliseconds',
        'std::chrono::microseconds',
        'std::chrono::nanoseconds',
        'std::chrono::seconds',
        'std::byte',
        'std::size_t',
        'std::int8_t',
        'std::uint8_t',
        'std::int16_t',
        'std::uint16_t',
        'std::int32_t',
        'std::uint32_t',
        'std::int64_t',
        'std::uint64_t',
    ]

    MGMT_GROUP_KEYS = [
        'analytics',
        'bucket',
        'cluster',
        'collection',
        'eventing_function',
        'query_index',
        'search_index',
        'user',
        'view_index'
    ]

    def __init__(self, cpp_parser_config: CppTypeParserConfig) -> None:
        self._cpp_parser = CppTypeParser(cpp_parser_config)
        self._cpp_types: List[BindingCppType] = []
        self._cpp_enums: List[BindingEnumType] = []
        self._kv_ops = []
        self._kv_multi_ops = []
        self._streaming_ops = []
        self._mgmt_op_groups = []

    @property
    def cpp_enums(self) -> List[BindingEnumType]:
        return self._cpp_enums

    @property
    def cpp_types(self) -> List[BindingCppType]:
        return self._cpp_types

    @property
    def kv_ops(self) -> List[BindingKeyValueOp]:
        return self._kv_ops

    @property
    def kv_multi_ops(self) -> List[BindingKeyValueMultiOp]:
        return self._kv_multi_ops

    @property
    def mgmt_op_groups(self) -> List[BindingMgmtGroup]:
        return self._mgmt_op_groups

    @property
    def streaming_ops(self) -> List[BindingStreamingOp]:
        return self._streaming_ops

    def get_cpp_type(self, cpp_ft: Union[CppField, CppType]) -> str:  # noqa: C901
        cpp_type = ''
        if 'cpp_type' not in cpp_ft:
            if 'of' in cpp_ft:
                cpp_type += f"{cpp_ft['name']}<"
                cpp_type += self.get_cpp_type(cpp_ft['of'])
                if 'to' in cpp_ft:
                    cpp_type += f", {self.get_cpp_type(cpp_ft['to'])}"
                if 'comparator' in cpp_ft:
                    cpp_type += f", {self.get_cpp_type(cpp_ft['comparator'])}"
                if 'size' in cpp_ft:
                    cpp_ft += f", {cpp_ft['size']}"
                cpp_type += '>'
            else:
                cpp_type += cpp_ft['name']
        elif 'of' in cpp_ft['cpp_type']:
            cpp_type += f"{cpp_ft['cpp_type']['name']}<"
            if cpp_ft['cpp_type']['name'] == 'std::variant':
                sub_types = []
                for cpp_sub_type in cpp_ft['cpp_type']['of']:
                    sub_types.append(self.get_cpp_type(cpp_sub_type))
                cpp_type += ', '.join(sub_types)
            else:
                cpp_type += self.get_cpp_type(cpp_ft['cpp_type']['of'])
                if 'to' in cpp_ft['cpp_type']:
                    cpp_type += f", {self.get_cpp_type(cpp_ft['cpp_type']['to'])}"
                if 'comparator' in cpp_ft['cpp_type']:
                    cpp_type += f", {self.get_cpp_type(cpp_ft['cpp_type']['comparator'])}"
                if 'size' in cpp_ft['cpp_type']:
                    cpp_type += f", {cpp_ft['cpp_type']['size']}"
            cpp_type += '>'
        else:
            cpp_type += cpp_ft['cpp_type']['name']

        return cpp_type

    def get_couchbase_special_py_type(self, cpp_type: str) -> Optional[str]:  # noqa: C901
        if cpp_type == 'couchbase::core::json_string':
            return 'bytes'
        if cpp_type == 'couchbase::cas':
            return 'int'
        if cpp_type == 'couchbase::core::impl::subdoc::opcode':
            return 'int'
        if cpp_type == 'couchbase::core::document_id':
            return 'CppDocumentId'
        if cpp_type == 'couchbase::mutation_token':
            return 'CppMutationToken'
        if cpp_type == 'couchbase::core::query_context':
            return 'CppQueryContext'

        if cpp_type == 'std::monostate':
            return 'None'
        if cpp_type == 'std::error_code':
            return 'int'
        if cpp_type == 'couchbase::core::tracing::wrapper_sdk_span':
            raise RuntimeError('Python type for couchbase::core::tracing::wrapper_sdk_span is not supported.')
        if cpp_type == 'couchbase::tracing::request_span':
            raise RuntimeError('Python type vor couchbase::tracing::request_span is not supported.')

    def get_pretty_name(self, op_name: str) -> str:
        name_tokens = op_name.split('_')
        return ''.join([t.capitalize() for t in name_tokens])

    def get_py_type_primitive(self, cpp_type: str) -> Optional[str]:
        if cpp_type in self.CPP_PY_INT_TYPES:
            return 'int'
        if cpp_type == 'std::string':
            return 'str'
        if cpp_type in ['std::bool', 'bool']:
            return 'bool'
        if cpp_type in ['std::float', 'std::double', 'double']:
            return 'float'

        return None

    def get_py_type_helper(self, cpp_type: str) -> str:

        py_type = self.get_py_type_primitive(cpp_type)
        if py_type:
            return py_type

        py_type = self.get_couchbase_special_py_type(cpp_type)
        if py_type:
            return py_type

        enum_match = next((e for e in self._cpp_enums if e.full_name == cpp_type), None)
        if enum_match:
            return enum_match.py_type

        cpp_core_match = next((t for t in self._cpp_types if t.full_name == cpp_type), None)
        if cpp_core_match:
            return cpp_core_match.py_type

        raise RuntimeError(f'Unable to find py type match for: {cpp_type}')

    def get_py_type(self, cpp_ft: Union[CppField, CppType]) -> str:  # noqa: C901
        py_type = ''
        if ('cpp_type' in cpp_ft and cpp_ft['cpp_type']['name'] == 'std::vector'
                and 'of' in cpp_ft['cpp_type'] and cpp_ft['cpp_type']['of']['name'] == 'std::byte'):
            return 'bytes'

        if 'cpp_type' not in cpp_ft:
            if 'of' not in cpp_ft:
                returned_type = self.get_py_type_helper(cpp_ft['name'])
                if returned_type is None:
                    print(f"bad return type for {cpp_ft['name']}")
                py_type += returned_type
            else:
                of_key = 'of'
                if cpp_ft['name'] == 'std::optional':
                    py_type += "Optional["
                elif cpp_ft['name'] in ['std::vector', 'std::array']:
                    py_type += "List["
                elif cpp_ft['name'] in 'std::set':
                    py_type += "Set["
                elif cpp_ft['name'] == 'std::map':
                    key = self.get_py_type_helper(cpp_ft['of']['name'])
                    py_type += f'Dict[{key}, '
                    of_key = 'to'
                elif cpp_ft['name'] == 'std::variant':
                    raise NotImplementedError('Nested std::variant is not yet implement.')
                py_type += self.get_py_type(cpp_ft[of_key])

                if cpp_ft['name'] == 'std::shared_ptr':
                    pass
                elif cpp_ft['name'] == 'std::optional':
                    # for TypedDict, we don't want to set an Optional[...] = None;
                    # this is okay (and sometimes necessary) for dataclasses
                    py_type += "]"
                else:
                    py_type += ']'
        elif 'of' in cpp_ft['cpp_type']:
            of_key = 'of'
            if cpp_ft['cpp_type']['name'] == 'std::optional':
                py_type += "Optional["
            elif cpp_ft['cpp_type']['name'] in ['std::vector', 'std::array']:
                py_type += "List["
            elif cpp_ft['cpp_type']['name'] in 'std::set':
                py_type += "Set["
            elif cpp_ft['cpp_type']['name'] == 'std::map':
                key = self.get_py_type_helper(cpp_ft['cpp_type']['of']['name'])
                py_type += f'Dict[{key}, '
                of_key = 'to'
            elif cpp_ft['cpp_type']['name'] == 'std::variant':
                py_sub_type = 'Union['
                sub_types = []
                for sub_type in cpp_ft['cpp_type']['of']:
                    next_sub_type = self.get_py_type(sub_type)
                    if next_sub_type == 'None':
                        py_sub_type = f'Optional[{py_sub_type}'
                    else:
                        sub_types.append(next_sub_type)
                py_sub_type += ', '.join(sub_types)
                if py_sub_type.startswith('Optional'):
                    py_sub_type += ']'
                py_sub_type += ']'
                py_type += py_sub_type
                return py_type

            py_type += self.get_py_type(cpp_ft['cpp_type'][of_key])

            if cpp_ft['cpp_type']['name'] == 'std::shared_ptr':
                pass
            elif cpp_ft['cpp_type']['name'] == 'std::optional':
                # for TypedDict, we don't want to set an Optional[...] = None;
                # this is okay (and sometimes necessary) for dataclasses
                py_type += "]"
            else:
                py_type += ']'
        else:
            returned_type = self.get_py_type_helper(cpp_ft['cpp_type']['name'])
            if returned_type is None:
                print(f"bad return type for {cpp_ft['cpp_type']['name']}")
            py_type += returned_type

        return py_type

    def build_binding_req_fields(self,
                                 cpp_type: CppParsedType,
                                 required_fields: List[str],
                                 ignored_fields: List[str],
                                 binding_dict: Dict[str, Any],
                                 is_kv_op: Optional[bool] = None,
                                 skip_if_empty_fields: List[str] = None) -> List[Dict[str, Any]]:
        fields = []
        for field in cpp_type['fields']:
            # we override parent_span w/ a std::shared_ptr<couchbase::core::tracing::wrapper_sdk_span>
            # when tracing is enabled
            if field['name'] == 'parent_span':
                continue
            skip_py_type = False
            new_field = {'cpp_name': field['name']}
            # ignored fields
            if field['name'] in ignored_fields:
                new_field['is_ignored'] = True
                skip_py_type = True
            # required fields
            if field['name'] in required_fields:
                if is_kv_op is True and field['name'] == 'id':
                    binding_dict['requires_doc_id'] = True
                new_field['is_required'] = True
            # skip if empty fields
            if skip_if_empty_fields and field['name'] in skip_if_empty_fields:
                new_field['skip_if_empty'] = True
            # TODO: handle py_name overrides
            if field['name'].endswith('_'):
                new_field['py_name'] = field['name'][:-1]
            else:
                new_field['py_name'] = field['name']
            new_field['cpp_type'] = self.get_cpp_type(field)
            if not skip_py_type:
                new_field['py_type'] = self.get_py_type(field)
            else:
                new_field['py_type'] = ''

            # NOTE: we handle fields having the name of a Python keyword when we build context for the templates
            fields.append(new_field)

        binding_dict['fields'] = fields

    def update_binding_resp_fields(self,
                                   cpp_type: CppParsedType,
                                   ignored_fields: List[str],
                                   binding_dict: Dict[str, Any],
                                   response_override: Optional[str] = None) -> None:

        fields = []
        for field in cpp_type['fields']:
            skip_py_type = False
            new_field = {'cpp_name': field['name']}
            if field['name'] in ignored_fields or response_override is not None:
                skip_py_type = True
                new_field['is_ignored'] = True
            if field['name'].endswith('_'):
                new_field['py_name'] = field['name'][:-1]
            else:
                new_field['py_name'] = field['name']
            new_field['cpp_type'] = self.get_cpp_type(field)
            if not skip_py_type:
                new_field['py_type'] = self.get_py_type(field)
            else:
                new_field['py_type'] = ''
            fields.append(new_field)

        if response_override is not None:
            binding_dict['response_override'] = response_override
        binding_dict['fields'] = fields

    def build_binding_dict(self,
                           header_path: str,
                           struct_name_full: str,
                           required_fields: List[str],
                           ignored_fields: List[str],
                           use_req_fields: bool,
                           is_kv_op: Optional[bool] = None,
                           response_override: Optional[str] = None,
                           skip_if_empty_fields: Optional[str] = None) -> Dict[str, Any]:

        cpp_types = self._cpp_parser.parse_op(header_path)
        cpp_type_match = next((cpp_type for cpp_type in cpp_types if struct_name_full == cpp_type['struct_name']), None)
        if cpp_type_match is None:
            raise RuntimeError(f'Unable to find C++ type for {struct_name_full}')

        struct_name = struct_name_full.split('::')[-1]
        binding_dict = {
            'struct_name': struct_name,
            'struct_name_full': struct_name_full,
        }
        if use_req_fields:
            self.build_binding_req_fields(cpp_type_match,
                                          required_fields,
                                          ignored_fields,
                                          binding_dict,
                                          is_kv_op=is_kv_op,
                                          skip_if_empty_fields=skip_if_empty_fields)
        else:
            self.update_binding_resp_fields(cpp_type_match,
                                            ignored_fields,
                                            binding_dict,
                                            response_override=response_override)

        return binding_dict

    def build_binding_kv_op(self,
                            header_path: str,
                            op: BindingConfigOp,
                            ignored_request_fields: List[str],
                            ignored_response_fields: List[str]) -> List[BindingKeyValueOp]:
        op_header_path = os.path.join(op.get('header_path', header_path), op['header_file'])
        # build request side of operation
        op_req_dict = self.build_binding_dict(op_header_path,
                                              op['request']['core_struct'],
                                              op['request'].get('required_fields', []),
                                              ignored_request_fields,
                                              True,
                                              is_kv_op=True)
        op_req_dict['request_struct'] = op_req_dict.pop('struct_name')
        op_req_dict['request_struct_full'] = op_req_dict.pop('struct_name_full')
        op_req_dict['request_fields'] = op_req_dict.pop('fields')
        # handle legacy durability
        op_req_dur_dict = {}
        if op['request'].get('with_legacy_durability', False) is True:
            op_req_dur_dict = self.build_binding_dict(op_header_path,
                                                      f"{op['request']['core_struct']}_with_legacy_durability",
                                                      op['request'].get('required_fields', []),
                                                      ignored_request_fields,
                                                      True,
                                                      is_kv_op=True)
            struct_name = op_req_dur_dict.pop('struct_name')
            op_req_dur_dict['name'] = f"{op['op_name']}_with_legacy_durability"
            pretty_name = self.get_pretty_name(op['op_name'])
            op_req_dur_dict['pretty_name'] = f"{pretty_name}WithLegacyDurability"
            op_req_dur_dict['request_struct'] = struct_name
            op_req_dur_dict['request_struct_full'] = op_req_dur_dict.pop('struct_name_full')
            op_req_dur_dict['request_fields'] = op_req_dur_dict.pop('fields')

        # build response side of operation
        op_resp_dict = self.build_binding_dict(op_header_path,
                                               op['response']['core_struct'],
                                               op['response'].get('required_fields', []),
                                               ignored_response_fields,
                                               False,
                                               is_kv_op=True,
                                               response_override=op['response'].get('response_override', None))
        op_resp_dict['response_struct'] = op_resp_dict.pop('struct_name')
        op_resp_dict['response_struct_full'] = op_resp_dict.pop('struct_name_full')
        op_resp_dict['response_fields'] = op_resp_dict.pop('fields')
        # op is not a mutation unless explicitly specified
        op_req_dict['is_mutation'] = op.get('is_mutation', False)
        # op is not a streaming unless explicitly specified
        op_req_dict['is_streaming_op'] = op.get('is_streaming_op', False)
        # op is has a multi variant unless explicitly specified
        op_req_dict['has_multi'] = op.get('has_multi', True)

        binding_ops = []
        # handle legacy durability
        if op_req_dur_dict:
            op_req_dur_dict.update(request_only=True,
                                   response_struct='',
                                   response_struct_full='',
                                   response_fields=[],
                                   is_mutation=op_req_dict['is_mutation'],
                                   has_multi=op_req_dict['has_multi'])
            binding_ops.append(BindingKeyValueOp(**op_req_dur_dict))
        op_req_dict.update(**op_resp_dict)
        op_req_dict['pretty_name'] = self.get_pretty_name(op['op_name'])
        binding_ops.append(BindingKeyValueOp(op['op_name'], **op_req_dict))
        return binding_ops

    def merge_op_overrides(self, op_dict: BindingConfigOp, overrides: BindingConfigOpOverride) -> None:
        if not overrides:
            return
        for key, value in overrides.items():
            if value is None:
                continue

            # If both are dicts, merge the nested level
            if key in op_dict and isinstance(op_dict[key], dict) and isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if sub_value is not None:
                        op_dict[key][sub_key] = sub_value
            else:
                # Otherwise, just update the top level
                op_dict[key] = value

    def build_kv_op(self,
                    header_path: str,
                    op_header: str,
                    ops_prefix: str,
                    structs_prefix: str,
                    op_with_legacy_durability: List[str],
                    op_overrides: List[BindingConfigOp],
                    required_request_fields: List[str],
                    ignored_request_fields: List[str],
                    ignored_response_fields: List[str],
                    is_mutation_op: bool) -> None:
        op_name = op_header[len(ops_prefix):-4]  # remove .hxx
        req_struct_name = f'{structs_prefix}{op_name}_request'

        op_req_dict: BindingConfigOpRequest = {'core_struct': req_struct_name}
        if required_request_fields:
            op_req_dict['required_fields'] = required_request_fields

        if op_name in op_with_legacy_durability:
            op_req_dict['with_legacy_durability'] = True

        resp_struct_name = f'{structs_prefix}{op_name}_response'
        op_resp_dict: BindingConfigOpResponse = {'core_struct': resp_struct_name}

        has_multi = True
        if 'lookup_in' in op_name or 'mutate_in' in op_name or 'projected' in op_name:
            has_multi = False

        op_dict: BindingConfigOp = {
            'op_name': op_name,
            'header_file': op_header,
            'request': op_req_dict,
            'response': op_resp_dict,
            'is_mutation': is_mutation_op,
            'has_multi': has_multi
        }

        # handle any overrides
        op_override = next((op for op in op_overrides if op['op_name'] == op_name), {})
        self.merge_op_overrides(op_dict, op_override)

        print(f'    Processing {op_dict["op_name"]}')
        ops = self.build_binding_kv_op(header_path,
                                       op_dict,
                                       ignored_request_fields,
                                       ignored_response_fields)
        self._kv_ops.extend(ops)

    def build_kv_operations(self, kv_ops: BindingConfigKeyValueOperations) -> None:
        print('\nProcessing key-value operations...')
        header_path = kv_ops['header_path']
        required_request_fields = kv_ops.get('required_cpp_request_fields', [])
        ignored_request_fields = kv_ops.get('ignored_cpp_request_fields', [])
        ignored_response_fields = kv_ops.get('ignored_cpp_response_fields', [])
        op_overrides = kv_ops.get('operation_overrides', [])
        op_with_legacy_durability = kv_ops['with_legacy_durability']
        ops_prefix = kv_ops['ops_prefix']
        structs_prefix = kv_ops['structs_prefix']
        for op_header in kv_ops['mutation_operation_headers']:
            self.build_kv_op(header_path,
                             op_header,
                             ops_prefix,
                             structs_prefix,
                             op_with_legacy_durability,
                             op_overrides,
                             required_request_fields,
                             ignored_request_fields,
                             ignored_response_fields,
                             True)
        for op_header in kv_ops['operation_headers']:
            self.build_kv_op(header_path,
                             op_header,
                             ops_prefix,
                             structs_prefix,
                             op_with_legacy_durability,
                             op_overrides,
                             required_request_fields,
                             ignored_request_fields,
                             ignored_response_fields,
                             False)

        print('Finished processing key-value operations.')

    def build_kv_multi_operations(self) -> None:
        print('\nProcessing key-value multi operations...')
        for op in self.kv_ops:
            if not op.has_multi:
                continue
            option_fields = []
            optional_option_fields = []
            print(f'    Processing {op.name}_multi')
            for f in op.request_fields:
                if f['py_name'] in ['id', 'value', 'flags']:
                    continue
                if f['py_type'].startswith('Optional'):
                    optional_option_fields.append(BindingMultiOpField(f['py_name'],
                                                                      f['py_type'],
                                                                      f.get('is_ignored', False)))
                else:
                    option_fields.append(BindingMultiOpField(f['py_name'], f['py_type'], f.get('is_ignored', False)))

            if op.is_mutation:
                doc_list_type = 'Tuple[str, Tuple[bytes, int]]'
            else:
                doc_list_type = 'List[str]'

            options_name = f'Cpp{op.pretty_name}MultiOptions'

            request_fields = [
                BindingMultiOpField('bucket_name', 'str', False),
                BindingMultiOpField('scope_name', 'str', False),
                BindingMultiOpField('collection_name', 'str', False),
                BindingMultiOpField('doc_list', doc_list_type, False),
                BindingMultiOpField('op_args', options_name, False),
                BindingMultiOpField('per_key_args', f'Dict[str, {options_name}]', False),
            ]

            self._kv_multi_ops.append(BindingKeyValueMultiOp(op.name,
                                                             op.pretty_name,
                                                             options_name,
                                                             request_fields,
                                                             option_fields + optional_option_fields,
                                                             is_streaming_op=op.is_streaming_op))

        print('Finished processing key-value multi operations.')

    def build_binding_streaming_op(self,
                                   header_path: str,
                                   op: BindingConfigOp,
                                   ignored_request_fields: List[str]) -> BindingStreamingOp:
        op_header_path = os.path.join(op.get('header_path', header_path), op['header_file'])
        # build request side of operation
        op_req_dict = self.build_binding_dict(op_header_path,
                                              op['request']['core_struct'],
                                              op['request'].get('required_fields', []),
                                              ignored_request_fields,
                                              True)
        op_req_dict['request_struct'] = op_req_dict.pop('struct_name')
        op_req_dict['request_struct_full'] = op_req_dict.pop('struct_name_full')
        op_req_dict['request_fields'] = op_req_dict.pop('fields')
        op_req_dict['pretty_name'] = self.get_pretty_name(op['op_name'])
        op_req_dict['streaming_op_name'] = op.get('streaming_op_name', op['op_name'])
        # streaming ops don't have a response (it is handled via a different path)
        self._streaming_ops.append(BindingStreamingOp(op['op_name'], **op_req_dict))

    def build_streaming_op(self,
                           header_path: str,
                           op_header: str,
                           ops_prefix: str,
                           structs_prefix: str,
                           op_overrides: List[BindingConfigOp],
                           ignored_request_fields: List[str]) -> None:
        # views are weird...or annoying
        if op_header.startswith('document_view'):
            op_name = op_header[:-4]  # remove .hxx
        else:
            op_name = op_header[len(ops_prefix):-4]  # remove .hxx
        req_struct_name = f'{structs_prefix}{op_name}_request'

        print(f'    Processing {op_name}')
        op_req_dict: BindingConfigOpRequest = {'core_struct': req_struct_name}

        op_dict: BindingConfigOp = {
            'op_name': op_name,
            'header_file': op_header,
            'request': op_req_dict,
            'response': {'core_struct': ''},
            'skip_response': True,
        }

        op_override = next((op for op in op_overrides if op['op_name'] == op_name), {})
        self.merge_op_overrides(op_dict, op_override)

        self.build_binding_streaming_op(header_path, op_dict, ignored_request_fields)

    def build_streaming_operations(self, streaming_ops: BindingConfigStreamingOperations) -> None:
        print('\nProcessing streaming operations...')
        header_path = streaming_ops['header_path']
        ignored_request_fields = streaming_ops.get('ignored_cpp_request_fields', [])
        op_overrides = streaming_ops.get('operation_overrides', [])
        ops_prefix = streaming_ops['ops_prefix']
        structs_prefix = streaming_ops['structs_prefix']
        for op_header in streaming_ops['operation_headers']:
            self.build_streaming_op(header_path,
                                    op_header,
                                    ops_prefix,
                                    structs_prefix,
                                    op_overrides,
                                    ignored_request_fields)
        print('Finished processing streaming operations.')

    def build_binding_mgmt_op(self,
                              header_path: str,
                              op: BindingConfigOp,
                              ignored_request_fields: List[str],
                              ignored_response_fields: List[str]) -> BindingMgmtOp:
        op_header_path = os.path.join(op.get('header_path', header_path), op['header_file'])
        # build request side of operation
        op_req_dict = self.build_binding_dict(op_header_path,
                                              op['request']['core_struct'],
                                              op['request'].get('required_fields', []),
                                              ignored_request_fields,
                                              True)
        op_req_dict['request_struct'] = op_req_dict.pop('struct_name')
        op_req_dict['request_struct_full'] = op_req_dict.pop('struct_name_full')
        op_req_dict['request_fields'] = op_req_dict.pop('fields')

        # build response side of operation
        op_resp_dict = self.build_binding_dict(op_header_path,
                                               op['response']['core_struct'],
                                               op['response'].get('required_fields', []),
                                               ignored_response_fields,
                                               False,
                                               response_override=op['response'].get('response_override', None))
        op_resp_dict['response_struct'] = op_resp_dict.pop('struct_name')
        op_resp_dict['response_struct_full'] = op_resp_dict.pop('struct_name_full')
        op_resp_dict['response_fields'] = op_resp_dict.pop('fields')

        op_req_dict.update(**op_resp_dict)
        op_req_dict['pretty_name'] = self.get_pretty_name(op['op_name'])
        return BindingMgmtOp(op['op_name'], **op_req_dict)

    def build_mgmt_templated_ops(self,
                                 header_path: str,
                                 op: BindingConfigOp,
                                 cpp_request_templates: List[str],
                                 ignored_request_fields: List[str],
                                 ignored_response_fields: List[str]) -> List[BindingMgmtOp]:
        templated_ops = []
        for idx, template in enumerate(cpp_request_templates):
            op_copy = deepcopy(op)
            print(f'    Processing {op_copy["op_name"]} w/ template: {template}')
            op_copy['request']['core_struct'] = f"{op_copy['request']['core_struct']}<{template}>"
            mgmt_op = self.build_binding_mgmt_op(header_path,
                                                 op_copy,
                                                 ignored_request_fields,
                                                 ignored_response_fields)
            if mgmt_op.request_struct.endswith('>') and not mgmt_op.request_struct.startswith('<'):
                mgmt_op.request_struct = mgmt_op.request_struct[:-1]
            # all templates except the last one should be request only (they all share the same response)
            mgmt_op.request_only = idx != len(cpp_request_templates) - 1
            mgmt_op.is_templated = True
            mgmt_op.templated_pretty_name = mgmt_op.pretty_name
            mgmt_op.name = f'{mgmt_op.name}_{mgmt_op.request_struct}'
            mgmt_op.pretty_name = self.get_pretty_name(mgmt_op.name)
            for field in mgmt_op.request_fields:
                if 'template' in field['cpp_type']:
                    field['cpp_type'] = field['cpp_type'].replace('template<', '')
                    if field['cpp_type'].endswith('>'):
                        field['cpp_type'] = field['cpp_type'][:-1]
                    if field['py_type'].endswith(']'):
                        field['py_type'] = field['py_type'][:-1]

            templated_ops.append(mgmt_op)
        return templated_ops

    def build_mgmt_op(self,
                      header_path: str,
                      op_header: str,
                      structs_prefix: str,
                      op_overrides: List[BindingConfigOp],
                      ignored_request_fields: List[str],
                      ignored_response_fields: List[str]) -> Union[BindingMgmtOp, List[BindingMgmtOp]]:
        op_name = op_header[:-4]  # remove .hxx
        req_struct_name = f'{structs_prefix}{op_name}_request'

        op_req_dict: BindingConfigOpRequest = {'core_struct': req_struct_name}
        resp_struct_name = f'{structs_prefix}{op_name}_response'
        op_resp_dict: BindingConfigOpResponse = {'core_struct': resp_struct_name}

        op_dict: BindingConfigOp = {
            'op_name': op_name,
            'header_file': op_header,
            'request': op_req_dict,
            'response': op_resp_dict
        }

        # handle any overrides
        op_override = next((op for op in op_overrides if op['op_name'] == op_name), {})
        self.merge_op_overrides(op_dict, op_override)

        cpp_request_templates = op_dict.get('cpp_request_templates', None)
        if cpp_request_templates:
            return self.build_mgmt_templated_ops(header_path,
                                                 op_dict,
                                                 cpp_request_templates,
                                                 ignored_request_fields,
                                                 ignored_response_fields)

        print(f'    Processing {op_dict["op_name"]}')
        return self.build_binding_mgmt_op(header_path,
                                          op_dict,
                                          ignored_request_fields,
                                          ignored_response_fields)

    def build_mgmt_group_operations(self,
                                    mgmt_group_ops: BindingConfigMgmtOperations,
                                    base_header_path: str,
                                    base_structs_prefix: str,
                                    base_ignored_request_fields: List[str],
                                    base_ignored_response_fields: List[str]) -> List[BindingMgmtOp]:
        header_path = mgmt_group_ops.get('header_path', base_header_path)

        ignored_request_fields = mgmt_group_ops.get('ignored_cpp_request_fields', base_ignored_request_fields)
        ignored_response_fields = mgmt_group_ops.get('ignored_cpp_response_fields', base_ignored_response_fields)
        structs_prefix = mgmt_group_ops.get('structs_prefix', base_structs_prefix)
        op_overrides = mgmt_group_ops.get('operation_overrides', [])
        mgmt_ops = []
        for op_header in mgmt_group_ops['operation_headers']:
            build_result = self.build_mgmt_op(header_path,
                                              op_header,
                                              structs_prefix,
                                              op_overrides,
                                              ignored_request_fields,
                                              ignored_response_fields)
            if isinstance(build_result, list):
                mgmt_ops.extend(build_result)
            else:
                mgmt_ops.append(build_result)

        return mgmt_ops

    def build_mgmt_operations(self, mgmt: BindingConfigMgmt) -> None:
        header_path = mgmt['header_path']
        ignored_request_fields = mgmt.get('ignored_cpp_request_fields', [])
        ignored_response_fields = mgmt.get('ignored_cpp_response_fields', [])
        structs_prefix = mgmt['structs_prefix']

        for mgmt_key in self.MGMT_GROUP_KEYS:
            mgmt_group_config = mgmt.get(mgmt_key, None)
            if mgmt_group_config is None:
                raise RuntimeError(f'Unable to find config for mgmt group: {mgmt_key}')

            print(f'\nProcessing {mgmt_key} mgmt operations...')
            mgmt_group_ops = self.build_mgmt_group_operations(mgmt_group_config,
                                                              header_path,
                                                              structs_prefix,
                                                              ignored_request_fields,
                                                              ignored_response_fields)
            pretty_name = self.get_pretty_name(mgmt_key)
            self._mgmt_op_groups.append(BindingMgmtGroup(mgmt_key, pretty_name, mgmt_group_ops))
            print(f'Finished processing {mgmt_key} mgmt operations.')

    def set_cpp_core_types(self, config_cpp_types: BindingConfigCppTypes) -> None:
        print('\nProcessing C++ core types...')
        ignored_fields = config_cpp_types.get('ignored_fields', [])
        for config_cpp_type in config_cpp_types['types']:
            skip_if_empty_fields = config_cpp_type.get('skip_if_empty_fields', [])
            type_req_dict = self.build_binding_dict(config_cpp_type['header_file'],
                                                    config_cpp_type['core_struct'],
                                                    config_cpp_type.get('required_fields', []),
                                                    config_cpp_type.get('ignored_fields', ignored_fields),
                                                    True,
                                                    skip_if_empty_fields=skip_if_empty_fields)
            type_req_dict['name'] = type_req_dict.pop('struct_name')
            type_req_dict['full_name'] = type_req_dict.pop('struct_name_full')
            if 'skip_request' in config_cpp_type:
                type_req_dict['skip_request'] = config_cpp_type['skip_request']
            type_req_dict['header'] = config_cpp_type['header_file']
            py_type = config_cpp_type.get('py_type', type_req_dict['name'])
            if '_' in py_type:
                py_type = ''.join((s.capitalize() for s in py_type.split('_')))
            elif py_type == type_req_dict['name']:
                py_type = py_type.capitalize()
            type_req_dict['py_type'] = f'Cpp{py_type}'
            self._cpp_types.append(BindingCppType(**type_req_dict))

        print('Finished processing C++ core types.')

    def set_cpp_core_enum_types(self, cpp_config_enums: List[BindingConfigCppEnumType]) -> None:
        print('\nProcessing C++ core enums...')
        for cpp_config_enum in cpp_config_enums:
            full_name = cpp_config_enum['core_enum']
            enum_name = full_name.split('::')[-1]
            cpp_enums = self._cpp_parser.parse_enum(cpp_config_enum['header_file'])
            cpp_enum_match = next((cppe for cppe in cpp_enums if full_name == cppe['name']), None)
            if cpp_enum_match is None:
                raise RuntimeError(f'Unable to find C++ type for {full_name}')

            enum_type = 'ENUM'
            if cpp_config_enum['enum_type'] == 'int':
                enum_type = 'INT_ENUM'
            elif cpp_config_enum['enum_type'] == 'int16':
                enum_type = 'INT16_ENUM'

            py_type = 'str' if enum_type == 'ENUM' else 'int'

            allowed_names = [val['name'] for val in cpp_enum_match['values']]
            if cpp_config_enum['default'] not in allowed_names:
                raise RuntimeError((f"Unable to find default value ({cpp_config_enum['default']}) from the config"
                                    f"in the matching C++ enum names: {allowed_names}"))

            binding_dict = {
                'name': enum_name,
                'full_name': full_name,
                'enum_type': enum_type,
                'default': cpp_config_enum['default'],
                'mapping_name': cpp_config_enum.get('mapping_name', enum_name).upper(),
                'header': cpp_config_enum['header_file'],
                'py_type': py_type
            }
            enum_vals = []
            use_hex = cpp_config_enum.get('use_hex', False)
            for enum_val in cpp_enum_match['values']:
                if enum_type != 'ENUM':
                    if use_hex and int(enum_val['value']) == -1:
                        val = '0xffff' if enum_type == 'INT16_ENUM' else '0xff'
                    elif use_hex:
                        unsigned_val = enum_val['value'] & (0xFFFF if enum_type == 'INT16_ENUM' else 0xFF)
                        val = f"{unsigned_val:#04x}"
                    else:
                        val = f"{enum_val['value']}"
                else:
                    val = f'"{enum_val["name"]}"'
                enum_vals.append(BindingEnumValue(enum_val['name'], val))

            self._cpp_enums.append(BindingEnumType(**binding_dict, enum_values=enum_vals))

        print('Finished processing C++ core enums.')
