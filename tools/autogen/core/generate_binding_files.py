from __future__ import annotations

import hashlib
import re
from dataclasses import asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Pattern,
                    Tuple,
                    Union)

import yaml
from jinja2 import Environment, FileSystemLoader

from tools.autogen.core.binding_autogen_types import BindingConfigSchema
from tools.autogen.core.binding_builder import BindingBuilder


class BindingFileType(Enum):
    CppCoreEnums = 'cpp_core_enums'
    CppCoreTypes = 'cpp_core_types'
    PycbcOperations = 'pycbc_operations'
    PycbcConnection = 'pycbc_connection'
    PycbcCore = '_core'
    BindingCppTypes = 'binding_cpp_types'
    BindingMapTypes = 'binding_map_types'
    OperationTypes = 'operation_types'
    BindingMap = 'binding_map'


class BindingGenerator:

    def __init__(self,
                 schema: BindingConfigSchema,
                 binding_builder: BindingBuilder,
                 templates_dir: Path,
                 output_root: Path,
                 dry_run: bool = False) -> None:
        self._schema = schema
        self._binding_builder = binding_builder
        self._templates_dir = templates_dir
        self._output_root = output_root
        self._dry_run = dry_run
        self._jinja_env = Environment(loader=FileSystemLoader(templates_dir))

    @staticmethod
    def load_binding_schema_yaml(schema_file: Path) -> BindingConfigSchema:
        if not schema_file.exists():
            raise FileNotFoundError(f'Schema file not found: {schema_file}')

        with open(schema_file, 'r') as f:
            schema = yaml.safe_load(f)

        return schema

    def _build_operations_context(self) -> Dict[str, Any]:
        kv_ops = []
        for op in self._binding_builder.kv_ops:
            new_op = {
                'name': op.name,
                'request': f'Cpp{op.pretty_name}Request',
                'response': f'Cpp{op.pretty_name}Response',
                'is_streaming_op': True if op.is_streaming_op else False
            }
            if 'WithLegacyDurability' in op.pretty_name:
                new_op['response'] = new_op['response'].replace('WithLegacyDurability', '')
            kv_ops.append(new_op)

        kv_multi_ops = []
        for op in self._binding_builder.kv_multi_ops:
            # responses are on a per-key basis and therefore are the base op response
            new_op = {
                'name': op.name,
                'request': f'Cpp{op.pretty_name}MultiRequest',
                'response': f'Cpp{op.pretty_name}Response',
                'is_streaming_op': True if op.is_streaming_op else False
            }
            if 'WithLegacyDurability' in op.pretty_name:
                new_op['response'] = new_op['response'].replace('WithLegacyDurability', '')
            kv_multi_ops.append(new_op)

        streaming_ops = []
        for op in self._binding_builder.streaming_ops:
            streaming_ops.append({
                'name': op.name,
                'streaming_op_name': op.streaming_op_name,
                'request': f'Cpp{op.pretty_name}Request',
            })

        mgmt_groups = []
        for grp in self._binding_builder.mgmt_op_groups:
            mgmt_ops = []
            for op in grp.operations:
                if op.is_templated:
                    mgmt_ops.append({
                        'name': op.name,
                        'request': f'Cpp{op.pretty_name}Request',
                        'response': f'Cpp{op.templated_pretty_name}Response',
                    })
                else:
                    mgmt_ops.append({
                        'name': op.name,
                        'request': f'Cpp{op.pretty_name}Request',
                        'response': f'Cpp{op.pretty_name}Response',
                    })

            mgmt_groups.append({
                'name': grp.name,
                'pretty_name': grp.pretty_name,
                'operations': sorted(mgmt_ops, key=lambda o: o['name'])
            })

        return {
            'key_value_operations': sorted(kv_ops, key=lambda o: o['name']),
            'key_value_multi_operations': sorted(kv_multi_ops, key=lambda o: o['name']),
            'streaming_operations': sorted(streaming_ops, key=lambda o: o['streaming_op_name']),
            'mgmt_groups': sorted(mgmt_groups, key=lambda g: g['pretty_name'])
        }

    def _build_operations_context_simple(self) -> Dict[str, Any]:
        kv_ops = []
        for op in self._binding_builder.kv_ops:
            kv_ops.append({
                'name': op.name,
                'pretty_name': op.pretty_name,
            })

        kv_multi_ops = []
        for op in self._binding_builder.kv_multi_ops:
            kv_multi_ops.append({
                'name': op.name,
                'pretty_name': op.pretty_name,
            })

        streaming_ops = []
        for op in self._binding_builder.streaming_ops:
            streaming_ops.append({
                'name': op.streaming_op_name,
                'pretty_name': self._binding_builder.get_pretty_name(op.streaming_op_name),
            })

        mgmt_groups = []
        for grp in self._binding_builder.mgmt_op_groups:
            mgmt_ops = []
            for op in grp.operations:
                mgmt_ops.append({
                    'name': op.name,
                    'pretty_name': op.pretty_name,
                })

            mgmt_groups.append({
                'name': grp.name,
                'pretty_name': grp.pretty_name,
                'operations': mgmt_ops
            })

        return {
            'key_value_operations': sorted(kv_ops, key=lambda o: o['pretty_name']),
            'key_value_multi_operations': sorted(kv_multi_ops, key=lambda o: o['pretty_name']),
            'streaming_operations': sorted(streaming_ops, key=lambda o: o['pretty_name']),
            'mgmt_groups': sorted(mgmt_groups, key=lambda g: g['pretty_name'])
        }

    def _get_output_file(self, binding_file_type: BindingFileType) -> Path:
        file_output = next((fo for fo in self._schema['metadata']
                           ['file_outputs'] if fo['name'] == binding_file_type.value), None)
        if file_output is None:
            raise RuntimeError(f'Unable to find file output info for BindingFileType: {binding_file_type}')

        output_dir = self._output_root / file_output['output_path']
        if not output_dir.exists():
            raise RuntimeError(f'Cannot find output dir ({output_dir}) for BindingFileType: {binding_file_type}')

        return output_dir / file_output['filename']

    def _get_rendered_content(self, template_name: str, context: Dict[str, Any]) -> str:
        template = self._jinja_env.get_template(template_name)
        return template.render(**context)

    def _get_section_pattern(self,
                             section_pattern: str,
                             comment_style: Optional[str] = None) -> Pattern[str]:
        # Escaping spaces just in case
        pattern_esc = re.escape(section_pattern)

        if comment_style is None:
            comment_style = '#'

        return re.compile(
            rf"(?P<header>.*)"
            rf"{comment_style} =+[\s\S]*?AUTOGENERATED {pattern_esc} START[\s\S]*?{comment_style} =+.*?\n"
            rf"(?P<old_body>.*?)\n"
            rf"{comment_style} =+[\s\S]*?AUTOGENERATED {pattern_esc} END[\s\S]*?{comment_style} =+"
            rf"(?P<footer>.*)",
            re.DOTALL  # DO NOT use MULTILINE
        )

    def _get_start_end_markers(self,
                               section_pattern: str,
                               timestamp: str,
                               checksum: str,
                               num_equals: Optional[int] = None,
                               comment_style: Optional[str] = None) -> Tuple[str, str]:
        if comment_style is None:
            comment_style = '#'

        if num_equals is None:
            num_equals = 100

        start_marker = (
            f"{comment_style} " + "="*num_equals + "\n"
            f"{comment_style} AUTOGENERATED {section_pattern} START - DO NOT EDIT MANUALLY\n"
            f"{comment_style} Generated-On: {timestamp}\n"
            f"{comment_style} Content-Hash: {checksum}\n"
            f"{comment_style} " + "="*num_equals
        )

        end_marker = (
            f"{comment_style} " + "="*num_equals + "\n"
            f"{comment_style} AUTOGENERATED {section_pattern} END - DO NOT EDIT MANUALLY\n"
            f"{comment_style} Generated-On: {timestamp}\n"
            f"{comment_style} Content-Hash: {checksum}\n"
            f"{comment_style} " + "="*num_equals
        )

        return start_marker, end_marker

    def _get_template(self,
                      binding_file_type: BindingFileType,
                      index: Optional[int] = None) -> Union[List[str], str]:
        file_output = next((fo for fo in self._schema['metadata']
                           ['file_outputs'] if fo['name'] == binding_file_type.value), None)
        if file_output is None:
            raise RuntimeError(f'Unable to find file output info for BindingFileType: {binding_file_type}')

        if index is not None:
            if not isinstance(file_output['templates'], list):
                raise RuntimeError(f'Expected a list of templates for BindingFileType: {binding_file_type}')
            return file_output['templates'][index]

        return file_output['templates']

    def _process_python_cpp_type_fields(self, ops: List[Dict[str, Any]]) -> None:
        for op in ops:
            ordered_ops = []
            optional_ops = []
            for f in op['fields']:
                # don't normally want _ at the end, but we cannot have names that are Python keywords
                if f['py_name'] == 'from':
                    f['py_name'] = f"{f['py_name']}_"

                if f['py_type'].startswith('Optional'):
                    optional_ops.append(f)
                else:
                    ordered_ops.append(f)

            op['fields'] = ordered_ops + optional_ops

    def _process_python_op_fields(self, ops: List[Dict[str, Any]]) -> None:  # noqa: C901
        for op in ops:
            ordered_ops = []
            optional_ops = []
            request_fields = op.pop('request_fields', None)
            if request_fields:
                for f in request_fields:
                    # don't normally want _ at the end, but we cannot have names that are Python keywords
                    if f['py_name'] == 'from':
                        f['py_name'] = f"{f['py_name']}_"
                    if f['py_type'].startswith('Optional'):
                        optional_ops.append(f)
                    else:
                        ordered_ops.append(f)

                # add async callback/errback
                for name in {'callback', 'errback'}:
                    optional_ops.append({
                        'py_name': name,
                        'py_type': 'Optional[Callable[..., None]]'
                    })
                # observability params
                optional_ops.append({
                    'py_name': 'wrapper_span_name',
                    'py_type': 'Optional[str]'
                })
                optional_ops.append({
                    'py_name': 'parent_span',
                    'py_type': 'Optional[Any]'
                })

                op['request_fields'] = ordered_ops + sorted(optional_ops, key=lambda o: o['py_name'])

            ordered_ops = []
            optional_ops = []
            response_fields = op.pop('response_fields', None)
            if response_fields:
                all_ignored = True
                for f in response_fields:
                    if f['py_type'].startswith('Optional'):
                        optional_ops.append(f)
                    else:
                        ordered_ops.append(f)
                    if 'is_ignored' not in f or not f['is_ignored']:
                        all_ignored = False

                op['response_fields'] = ordered_ops + optional_ops
                if all_ignored:
                    op['request_only'] = True

    def _substitute_content(self,
                            output_file: Path,
                            section_pattern: str,
                            new_content: str,
                            start_marker: str,
                            end_marker: str,
                            comment_style: Optional[str] = None,
                            use_rstrip: Optional[bool] = None) -> None:
        if not output_file.exists():
            raise RuntimeError(f'Output file not found: {output_file}')

        with open(output_file, 'r') as f:
            content = f.read()

        binding_pattern = self._get_section_pattern(section_pattern, comment_style=comment_style)
        match = binding_pattern.search(content)
        if not match:
            raise RuntimeError(f'Could not find autogenerated import markers in {output_file}')

        if use_rstrip is True:
            stripped_content = new_content.rstrip()
        else:
            stripped_content = new_content.strip()

        new_file_content = (
            match.group('header').strip() +
            "\n\n" +
            start_marker +
            "\n" +
            stripped_content +
            "\n\n" +
            end_marker +
            match.group('footer')
        )

        if self._dry_run:
            print(f'[DRY RUN] Would update autogenerated section(s) for: {output_file.name}')
        else:
            with open(output_file, 'w') as f:
                f.write(new_file_content)
            print(f'Updated autogenerated section(s) for: {output_file.name}')

    def _write_file(self, output_file: Path, content: str) -> None:
        if self._dry_run:
            print(f'[DRY RUN] Would write autogenerated file: {output_file.name}')
        else:
            with open(output_file, 'w') as f:
                f.write(content)
            print(f'Wrote autogenerated file: {output_file.name}')

    def build_types(self) -> None:
        self._binding_builder.set_cpp_core_enum_types(self._schema['cpp_core_enums'])
        self._binding_builder.set_cpp_core_types(self._schema['cpp_core_types'])

    def build_operations(self) -> None:
        self._binding_builder.build_kv_operations(self._schema['key_value'])
        self._binding_builder.build_kv_multi_operations()
        self._binding_builder.build_streaming_operations(self._schema['streaming'])
        self._binding_builder.build_mgmt_operations(self._schema['management'])

    def generate_cpp_core_enums_template(self) -> None:
        enum_headers = set([f'<{t.header}>' for t in self._binding_builder.cpp_enums])
        timestamp = datetime.now()
        context = {
            'current_year': timestamp.year,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'enum_headers': self._schema['cpp_core_enum_headers'] + list(sorted(enum_headers)),
            'cpp_enums': [asdict(e) for e in self._binding_builder.cpp_enums],
        }
        template_name = self._get_template(BindingFileType.CppCoreEnums)
        code = self._get_rendered_content(template_name, context)
        output_file = self._get_output_file(BindingFileType.CppCoreEnums)
        self._write_file(output_file, code)

    def generate_cpp_core_types_template(self) -> None:
        type_headers = set([f'<{t.header}>' for t in self._binding_builder.cpp_types])
        timestamp = datetime.now()
        context = {
            'current_year': timestamp.year,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'type_headers': self._schema['cpp_core_type_headers'] + list(sorted(type_headers)),
            'cpp_types': [asdict(t) for t in self._binding_builder.cpp_types],
        }
        template_name = self._get_template(BindingFileType.CppCoreTypes)
        code = self._get_rendered_content(template_name, context)
        output_file = self._get_output_file(BindingFileType.CppCoreTypes)
        self._write_file(output_file, code)

    def generate_operations_template(self) -> None:
        have_mutation_ops = any(map(lambda o: o.is_mutation, self._binding_builder.kv_ops))
        have_streaming_kv_ops = any(map(lambda o: o.is_streaming_op, self._binding_builder.kv_ops))

        kv_ops = [asdict(op) for op in sorted(self._binding_builder.kv_ops, key=lambda o: o.name)]
        streaming_ops = [asdict(op) for op in sorted(self._binding_builder.streaming_ops, key=lambda o: o.name)]
        mgmt_grps = []
        for grp in sorted(self._binding_builder.mgmt_op_groups, key=lambda g: g.name):
            mgmt_ops = [asdict(op) for op in sorted(grp.operations, key=lambda o: o.name)]
            mgmt_grps.append({
                'name': grp.name,
                'pretty_name': grp.pretty_name,
                'operations': mgmt_ops
            })

        timestamp = datetime.now()
        context = {
            'current_year': timestamp.year,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'operation_headers': self._schema['operation_headers'],
            'key_value_operations': kv_ops,
            'streaming_operations': streaming_ops,
            'mgmt_groups': mgmt_grps,
            'have_mutation_ops': have_mutation_ops,
            'have_streaming_kv_ops': have_streaming_kv_ops,
        }
        template_name = self._get_template(BindingFileType.PycbcOperations)
        code = self._get_rendered_content(template_name, context)
        output_file = self._get_output_file(BindingFileType.PycbcOperations)
        self._write_file(output_file, code)

    def generate_pycbc_connection_template(self) -> None:
        kv_ops = [asdict(op) for op in sorted(self._binding_builder.kv_ops, key=lambda o: o.name)]
        streaming_ops = [asdict(op) for op in sorted(self._binding_builder.streaming_ops, key=lambda o: o.name)]
        mgmt_grps = []
        for grp in sorted(self._binding_builder.mgmt_op_groups, key=lambda g: g.name):
            mgmt_ops = [asdict(op) for op in sorted(grp.operations, key=lambda o: o.name)]
            mgmt_grps.append({
                'name': grp.name,
                'pretty_name': grp.pretty_name,
                'operations': mgmt_ops
            })

        context = {
            'key_value_operations': kv_ops,
            'streaming_operations': streaming_ops,
            'mgmt_groups': mgmt_grps,
        }
        template_name = self._get_template(BindingFileType.PycbcConnection)
        code = self._get_rendered_content(template_name, context)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        checksum = hashlib.md5(code.encode('utf-8')).hexdigest()

        output_file = self._get_output_file(BindingFileType.PycbcConnection)
        section_pattern = 'SECTION'
        start_marker, end_marker = self._get_start_end_markers(section_pattern,
                                                               timestamp,
                                                               checksum,
                                                               comment_style='//')
        self._substitute_content(output_file,
                                 section_pattern,
                                 code,
                                 start_marker,
                                 end_marker,
                                 comment_style='//')

    def generate_binding_cpp_types_template(self) -> None:
        cpp_types = [asdict(t) for t in sorted(self._binding_builder.cpp_types, key=lambda t: t.py_type)]
        self._process_python_cpp_type_fields(cpp_types)
        kv_ops = [asdict(op) for op in sorted(self._binding_builder.kv_ops, key=lambda o: o.name)]
        self._process_python_op_fields(kv_ops)
        kv_multi_ops = [asdict(op) for op in sorted(self._binding_builder.kv_multi_ops, key=lambda o: o.name)]
        streaming_ops = [asdict(op) for op in sorted(self._binding_builder.streaming_ops, key=lambda o: o.name)]
        self._process_python_op_fields(streaming_ops)
        mgmt_groups = []
        for grp in sorted(self._binding_builder.mgmt_op_groups, key=lambda o: o.name):
            grp_ops = [asdict(op) for op in sorted(grp.operations, key=lambda o: o.name)]
            self._process_python_op_fields(grp_ops)
            mgmt_groups.append({
                'name': grp.name,
                'pretty_name': grp.pretty_name,
                'operations': grp_ops
            })

        context = {
            'cpp_types': cpp_types,
            'key_value_operations': kv_ops,
            'key_value_multi_operations': kv_multi_ops,
            'streaming_operations': streaming_ops,
            'mgmt_groups': mgmt_groups,
        }
        template_name = self._get_template(BindingFileType.BindingCppTypes)
        code = self._get_rendered_content(template_name, context)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        checksum = hashlib.md5(code.encode('utf-8')).hexdigest()

        output_file = self._get_output_file(BindingFileType.BindingCppTypes)
        section_pattern = 'SECTION'
        start_marker, end_marker = self._get_start_end_markers(section_pattern, timestamp, checksum)
        self._substitute_content(output_file,
                                 section_pattern,
                                 code,
                                 start_marker,
                                 end_marker)

    def generate_pycbc_core_pyi_template(self) -> None:
        context = self._build_operations_context()
        template_name = self._get_template(BindingFileType.PycbcCore)
        code = self._get_rendered_content(template_name, context)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        checksum = hashlib.md5(code.encode('utf-8')).hexdigest()

        section_pattern = 'SECTION'
        start_marker, end_marker = self._get_start_end_markers(section_pattern, timestamp, checksum)
        output_file = self._get_output_file(BindingFileType.PycbcCore)
        self._substitute_content(output_file,
                                 section_pattern,
                                 code,
                                 start_marker,
                                 end_marker,
                                 use_rstrip=True)

    def generate_operation_types_template(self) -> None:
        context = self._build_operations_context_simple()
        template_name = self._get_template(BindingFileType.OperationTypes)
        code = self._get_rendered_content(template_name, context)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        checksum = hashlib.md5(code.encode('utf-8')).hexdigest()

        section_pattern = 'SECTION'
        start_marker, end_marker = self._get_start_end_markers(section_pattern, timestamp, checksum)
        output_file = self._get_output_file(BindingFileType.OperationTypes)
        self._substitute_content(output_file,
                                 section_pattern,
                                 code,
                                 start_marker,
                                 end_marker)

    def generate_binding_map_types_template(self) -> None:
        context = self._build_operations_context()
        template_name = self._get_template(BindingFileType.BindingMapTypes)
        code = self._get_rendered_content(template_name, context)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        checksum = hashlib.md5(code.encode('utf-8')).hexdigest()

        section_pattern = 'SECTION'
        start_marker, end_marker = self._get_start_end_markers(section_pattern, timestamp, checksum)
        output_file = self._get_output_file(BindingFileType.BindingMapTypes)
        self._substitute_content(output_file,
                                 section_pattern,
                                 code,
                                 start_marker,
                                 end_marker,
                                 use_rstrip=True)

    def generate_binding_map_template(self) -> None:
        context = self._build_operations_context_simple()
        template_name = self._get_template(BindingFileType.BindingMap)
        code = self._get_rendered_content(template_name, context)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        checksum = hashlib.md5(code.encode('utf-8')).hexdigest()

        section_pattern = 'SECTION'
        start_marker, end_marker = self._get_start_end_markers(section_pattern, timestamp, checksum)
        output_file = self._get_output_file(BindingFileType.BindingMap)
        self._substitute_content(output_file,
                                 section_pattern,
                                 code,
                                 start_marker,
                                 end_marker,
                                 use_rstrip=True)

    def run(self) -> None:
        self.build_types()
        self.build_operations()
        self.generate_cpp_core_enums_template()
        self.generate_cpp_core_types_template()
        self.generate_operations_template()
        self.generate_pycbc_connection_template()
        self.generate_binding_cpp_types_template()
        self.generate_pycbc_core_pyi_template()
        self.generate_operation_types_template()
        self.generate_binding_map_types_template()
        self.generate_binding_map_template()
