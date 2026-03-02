from __future__ import annotations

import os
import re
import subprocess
import sys
from functools import reduce
from pathlib import Path
from typing import (Dict,
                    List,
                    Optional,
                    Tuple,
                    TypedDict)

import clang.cindex

from tools.autogen.core.binding_autogen_types import CppParsedEnum, CppParsedType

CXX_CLIENT_ROOT = os.path.join(Path(__file__).parent.parent.parent.parent, 'deps', 'couchbase-cxx-client')
CXX_CLIENT_CACHE = os.path.join(Path(__file__).parent.parent.parent.parent, 'deps', 'couchbase-cxx-cache')

CXX_DEPS_INCLUDE_PATHS = {
    'asio': ['-I{0}/asio/{1}/asio/asio/include'],
    'gsl': ['-I{0}/gsl/{1}/gsl/include'],
    'json': ['-I{0}/json/{1}/json/include',
             '-I{0}/json/{1}/json/external/PEGTL/include'
             ],
    'spdlog': ['-I{0}/spdlog/{1}/spdlog/include'],
}

STD_COMPARATOR_TEMPLATES = ["std::less<{0}>", "std::greater<{0}>", "std::less_equal<{0}>", "std::greater_equal<{0}>"]
# flatten the list of lists
STD_COMPARATORS = list(reduce(lambda a, b: a + b,
                              list(map(lambda c: [c.format(s) for s in ['', 'void']], STD_COMPARATOR_TEMPLATES)),
                              []))

INTERNAL_STRUCTS = []
UNNAMED_STRUCT_DELIM = '::(unnamed struct'

# IMPORTANT: the templates must exist in the TYPE_LIST
TEMPLATED_REQUESTS = {
    'analytics_link_create_request': {
        'template_name': 'analytics_link_type',
        'templates': ['couchbase::core::management::analytics::azure_blob_external_link',
                      'couchbase::core::management::analytics::couchbase_remote_link',
                      'couchbase::core::management::analytics::s3_external_link']
    },
    'analytics_link_replace_request': {
        'template_name': 'analytics_link_type',
        'templates': ['couchbase::core::management::analytics::azure_blob_external_link',
                      'couchbase::core::management::analytics::couchbase_remote_link',
                      'couchbase::core::management::analytics::s3_external_link']
    },
}


class CppTypeParserConfig(TypedDict, total=False):
    llvm_clang_version: Optional[str] = None
    llvm_libdir: Optional[str] = None
    llvm_includedir: Optional[str] = None
    system_headers: Optional[str] = None
    verbose: Optional[bool] = None


class CppTypeParser:

    def __init__(self, config: CppTypeParserConfig) -> None:
        self._verbose = config.get('verbose', None)
        self.configure_generator(config)

    def configure_generator(self, config: CppTypeParserConfig) -> None:  # noqa: C901
        version = config.get('llvm_clang_version', os.environ.get('PYCBC_LLVM_VERSION'))
        if version is None:
            CppTypeParser.find_llvm()
            version = CppTypeParser.get_llvm_version()
        if version is None:
            raise ValueError('Missing LLVM version.')

        print(f'Using LLVM version={version}')
        llvm_root_dir = f'/opt/homebrew/Cellar/llvm/{version}'
        if not os.path.exists(llvm_root_dir):
            print(f'LLVM root directory ({llvm_root_dir}) does not exist. Attempting to find it.')
            llvm_root_dir = None
            for d in os.listdir('/opt/homebrew/Cellar/llvm'):
                if d.startswith(version):
                    llvm_root_dir = os.path.join('/opt/homebrew/Cellar/llvm', d)
                    print(f'Found LLVM root directory: {llvm_root_dir}')
                    break
            if llvm_root_dir is None:
                raise ValueError((f'Unable to find LLVM root directory for version {version}.'
                                  'Please use CN_LLVM_VERSION to override.'))

        includedir = config.get('llvm_includedir', os.environ.get('PYCBC_LLVM_INCLUDE'))
        if includedir is None:
            includedir = CppTypeParser.get_llvm_includedir()
        if includedir is None:
            raise ValueError('Missing LLVM include directory.')

        libdir = config.get('llvm_libdir', os.environ.get('PYCBC_LLVM_LIB'))
        if libdir is None:
            libdir = CppTypeParser.get_llvm_libdir()
        if libdir is None:
            raise ValueError('Missing LLVM lib directory.')

        system_headers = config.get('system_headers', os.environ.get('PYCBC_SYS_HEADERS'))
        if system_headers is None:
            system_headers = CppTypeParser.get_system_headers()
        if system_headers is None:
            raise ValueError('Missing system headers path.')

        if self._verbose:
            print(f'Using libdir={libdir}')
        clang.cindex.Config.set_library_path(libdir)

        self._include_paths = [
            '-I/opt/homebrew/opt/llvm/include/c++/v1',
            f'-I{CXX_CLIENT_ROOT}/',
            f'-I{llvm_root_dir}/lib/clang/{version[:2]}/include',
            f'-I{system_headers}/usr/include'
        ]

        for dep, inc in CXX_DEPS_INCLUDE_PATHS.items():
            self._include_paths.extend(self.set_cxx_deps_include_paths(dep, inc))

        if self._verbose:
            print(f'Include paths={self._include_paths}')

    def parse_op(self, file_path: str) -> List[CppParsedType]:
        header_path = os.path.join(CXX_CLIENT_ROOT, file_path)
        index = clang.cindex.Index.create()
        suppress_warnings = ['-Wno-nullability-completeness', '-Wno-deprecated-literal-operator']
        if self._verbose is True:
            args = ['-std=c++17', '-v', f'-isysroot{os.getcwd()}'] + self._include_paths + suppress_warnings
        else:
            args = ['-std=c++17', f'-isysroot{os.getcwd()}'] + self._include_paths + suppress_warnings
        translation_unit = index.parse(header_path, args=args)

        self._op_types = []
        self._op_enums = []
        self.traverse(translation_unit.cursor, [], header_path)
        return self._op_types

    def parse_enum(self, file_path: str) -> List[CppParsedEnum]:
        header_path = os.path.join(CXX_CLIENT_ROOT, file_path)
        index = clang.cindex.Index.create()
        suppress_warnings = ['-Wno-nullability-completeness', '-Wno-deprecated-literal-operator']
        if self._verbose is True:
            args = ['-std=c++17', '-v', f'-isysroot{os.getcwd()}'] + self._include_paths + suppress_warnings
        else:
            args = ['-std=c++17', f'-isysroot{os.getcwd()}'] + self._include_paths + suppress_warnings
        translation_unit = index.parse(header_path, args=args)

        self._op_types = []
        self._op_enums = []
        self.traverse(translation_unit.cursor, [], header_path)
        return self._op_enums

    def traverse(self, node: clang.cindex.Cursor, namespace, main_file: str) -> None:  # noqa: C901
        # only scan the elements of the file we parsed
        if node.location.file is not None and node.location.file.name != main_file:
            return

        if node.kind == clang.cindex.CursorKind.STRUCT_DECL or node.kind == clang.cindex.CursorKind.CLASS_DECL:
            full_struct_name = "::".join([*namespace, node.displayname])
            if full_struct_name.endswith('::') or UNNAMED_STRUCT_DELIM in full_struct_name:
                if full_struct_name.endswith('::'):
                    struct_name = full_struct_name
                else:
                    struct_name = full_struct_name.split(UNNAMED_STRUCT_DELIM)[0]
                match = next((s for s in INTERNAL_STRUCTS if struct_name in s), None)
                if match:
                    full_struct_name = match

            if (self.is_included_type(full_struct_name)
                    or full_struct_name in INTERNAL_STRUCTS):
                struct_fields = []
                parents = []
                for child in node.get_children():
                    if child.kind == clang.cindex.CursorKind.FIELD_DECL:
                        struct_type = self.parse_type(child.type)
                        type_str = child.type.get_canonical().spelling
                        if 'unnamed' in type_str:
                            name_tokens = type_str.split('::')
                            name_override = '::'.join(name_tokens[:-1] + [child.displayname])
                            struct_type['name'] = name_override
                            INTERNAL_STRUCTS.append(name_override)

                        struct_fields.append({
                            "name": child.displayname,
                            "cpp_type": struct_type,
                        })
                    elif child.kind == clang.cindex.CursorKind.CXX_BASE_SPECIFIER:
                        parents.append("::".join([*namespace, child.displayname]))

                if len(parents) > 0:
                    part_op_types = [ot for ot in self._op_types if ot['struct_name'] in parents]
                    for sot in part_op_types:
                        struct_fields.extend(sot['fields'])

                # replica read changes introduced duplicate get requests
                if any(map(lambda op: op['struct_name'] == full_struct_name, self._op_types)):
                    return

                self._op_types.append({
                    "struct_name": full_struct_name,
                    "fields": struct_fields,
                })
        if node.kind == clang.cindex.CursorKind.TYPE_ALIAS_DECL:
            full_struct_name = "::".join([*namespace, node.displayname])
            if self.is_included_type(full_struct_name, with_durability=True):
                type_ref = next((c for c in node.get_children() if c.kind == clang.cindex.CursorKind.TYPE_REF), None)
                if type_ref:
                    base_request_name = type_ref.displayname.replace('struct', '').strip()
                    base_request = next((op for op in self._op_types if op['struct_name'] == base_request_name), None)
                    if base_request:
                        new_fields = [f for f in base_request['fields'] if f['name'] != 'durability_level']
                        new_fields.extend([
                            {"name": "persist_to", "cpp_type": {"name": "couchbase::persist_to"}},
                            {"name": "replicate_to", "cpp_type": {"name": "couchbase::replicate_to"}}
                        ])

                        self._op_types.append({
                            "struct_name": full_struct_name,
                            "fields": new_fields
                        })
        if node.kind == clang.cindex.CursorKind.ENUM_DECL:
            full_enum_name = "::".join([*namespace, node.displayname])
            if self.is_included_type(full_enum_name):
                enumValues = []

                for child in node.get_children():
                    if child.kind == clang.cindex.CursorKind.ENUM_CONSTANT_DECL:
                        enumValues.append({
                            "name": child.displayname,
                            "value": child.enum_value,
                        })
                self._op_enums.append({
                    "name": full_enum_name,
                    "type": self.parse_type(node.enum_type),
                    "values": enumValues,
                })

        if node.kind == clang.cindex.CursorKind.NAMESPACE:
            namespace = [*namespace, node.displayname]
        if node.kind == clang.cindex.CursorKind.CLASS_DECL:
            namespace = [*namespace, node.displayname]
        if node.kind == clang.cindex.CursorKind.STRUCT_DECL:
            namespace = [*namespace, node.displayname]
        if node.kind == clang.cindex.CursorKind.CLASS_TEMPLATE:
            name_tokens = node.displayname.split('<')
            if len(name_tokens) == 2 and name_tokens[0] in TEMPLATED_REQUESTS:
                req = TEMPLATED_REQUESTS.get(name_tokens[0])
                full_struct_name = "::".join([*namespace, node.displayname])
                for template in req['templates']:
                    struct_fields = []
                    for child in node.get_children():
                        if child.kind == clang.cindex.CursorKind.FIELD_DECL:
                            type_str = child.type.get_canonical().spelling
                            if 'type-parameter' in type_str:
                                struct_type = {
                                    'name': 'template',
                                    'of': {'name': template}}
                            else:
                                struct_type = self.parse_type(child.type)

                            struct_fields.append({
                                "name": child.displayname,
                                "cpp_type": struct_type,
                            })
                    self._op_types.append({
                        "struct_name": full_struct_name.replace(req['template_name'], template),
                        "fields": struct_fields,
                    })
                # return

        for child in node.get_children():
            self.traverse(child, namespace, main_file)

    def is_included_type(self, name: str, with_durability: Optional[bool] = False) -> bool:

        # TODO(brett19): This should be generalized somehow...
        if "is_compound_operation" in name:
            return False

        if "replica_context" in name:
            return False

        if with_durability is True and '_with_legacy_durability' not in name:
            return False

        if 'couchbase::core::io::mcbp_traits' in name:
            return False

        # for x in type_list_re:
        #     if re.fullmatch(x, name):
        #         return True
        return True

    def parse_type(self, type_: str) -> Dict[str, str]:
        type_str = type_.get_canonical().spelling
        return self.parse_type_str(type_str)

    def parse_type_str(self, type_str: str) -> Dict[str, str]:  # noqa: C901
        if type_str == "std::mutex":
            return {"name": "std::mutex"}
        if type_str == "std::string":
            return {"name": "std::string"}
        if type_str == "std::chrono::duration<long long>":
            return {"name": "std::chrono::seconds"}
        if type_str == "std::chrono::duration<long long, std::ratio<1, 1000>>":
            return {"name": "std::chrono::milliseconds"}
        if type_str == "std::chrono::duration<long long, std::ratio<1, 1000000>>":
            return {"name": "std::chrono::microseconds"}
        if type_str == "std::chrono::duration<long long, std::ratio<1, 1000000000>>":
            return {"name": "std::chrono::nanoseconds"}
        if type_str == "std::error_code":
            return {"name": "std::error_code"}
        if type_str == "std::monostate":
            return {"name": "std::monostate"}
        if type_str == "std::byte":
            return {"name": "std::byte"}
        if type_str == "unsigned long":
            return {"name": "std::size_t"}
        if type_str == "char":
            return {"name": "std::int8_t"}
        if type_str == "unsigned char":
            return {"name": "std::uint8_t"}
        if type_str == "short":
            return {"name": "std::int16_t"}
        if type_str == "unsigned short":
            return {"name": "std::uint16_t"}
        if type_str == "int":
            return {"name": "std::int32_t"}
        if type_str == "unsigned int":
            return {"name": "std::uint32_t"}
        if type_str == "long long":
            return {"name": "std::int64_t"}
        if type_str == "unsigned long long":
            return {"name": "std::uint64_t"}
        if type_str == "bool":
            # return {"name": "std::bool"}
            return {"name": "bool"}
        if type_str == "float":
            return {"name": "std::float"}
        if type_str == "double":
            # return {"name": "std::double"}
            return {"name": "double"}
        if type_str == "std::nullptr_t":
            return {"name": "std::nullptr_t"}
        if type_str in STD_COMPARATORS:
            if 'void' in type_str:
                return {"name": type_str.replace("void", "")}
            return {"name": type_str}

        tplParts = type_str.split("<", 1)
        if len(tplParts) > 1:
            tplClassName = tplParts[0]
            tplParams = tplParts[1][:-1]
            if tplClassName == "std::function":
                return {
                    "name": "std::function"
                }
            if tplClassName == "std::optional":
                return {
                    "name": "std::optional",
                    "of": self.parse_type_str(tplParams)
                }
            if tplClassName == "std::vector":
                return {
                    "name": "std::vector",
                    "of": self.parse_type_str(tplParams)
                }
            if tplClassName == "std::set":
                return {
                    "name": "std::set",
                    "of": self.parse_type_str(tplParams)
                }
            if tplClassName == "std::variant":
                variantParts = tplParams.split(", ")
                variantTypes = []
                for variantPart in variantParts:
                    variantTypes.append(self.parse_type_str(variantPart))
                return {
                    "name": "std::variant",
                    "of": variantTypes
                }
            if tplClassName == "std::array":
                variantParts = tplParams.split(", ")
                if len(variantParts) != 2:
                    print("FAILED TO PARSE ARRAY TYPES: " + type_str)
                    return {"name": "unknown", "str": type_str}
                return {
                    "name": "std::array",
                    "of": self.parse_type_str(variantParts[0]),
                    "size": int(variantParts[1])
                }
            if tplClassName == "std::map":
                variantParts = tplParams.split(", ")
                if len(variantParts) < 2 or len(variantParts) > 3:
                    print("FAILED TO PARSE MAP TYPES: " + type_str)
                    return {"name": "unknown", "str": type_str}

                if len(variantParts) == 2:
                    return {
                        "name": "std::map",
                        "of": self.parse_type_str(variantParts[0]),
                        "to": self.parse_type_str(variantParts[1])
                    }
                else:
                    return {
                        "name": "std::map",
                        "of": self.parse_type_str(variantParts[0]),
                        "to": self.parse_type_str(variantParts[1]),
                        "comparator": self.parse_type_str(variantParts[2])
                    }

            if tplClassName == "std::shared_ptr":
                return {
                    "name": "std::shared_ptr",
                    "of": self.parse_type_str(tplParams)
                }

        if not type_str.startswith("couchbase::"):
            print("FAILED TO PARSE STRING TYPE: " + type_str)
            return {"name": "unknown", "str": type_str}

        if 'unnamed struct' in type_str and self._verbose:
            print("WARNING:  Found unnamed struct: " + type_str)

        return {"name": type_str}

    def set_cxx_deps_include_paths(self, dep, includes):
        cpm_path = os.path.join(CXX_CLIENT_CACHE, dep)
        dir_patterns = [r'[0-9a-z]{40}', r'[0-9a-z]{4}']
        cpm_hash_dir = None
        for dir_pattern in dir_patterns:
            cpm_hash_dir = next((d for d in os.listdir(cpm_path)
                                if os.path.isdir(os.path.join(cpm_path, d)) and re.match(dir_pattern, d)),
                                None)
            if cpm_hash_dir:
                break
        if not cpm_hash_dir:
            raise Exception(f'Unable to find CPM hash directory for path: {cpm_path}.')
        return list(map(lambda p: p.format(CXX_CLIENT_CACHE, cpm_hash_dir), includes))

    @staticmethod
    def list_headers_in_dir(path: str, file_startswith: Optional[str] = None) -> List[str]:
        # enumerates a folder but keeps the full pathing for the files returned
        # and removes certain files we don't want (like non-hxx, _json.hxx or _fmt.hxx)

        # list all the files in the folder
        files = os.listdir(path)

        if file_startswith is not None:
            files = list(filter(lambda f: f.endswith('.hxx') and f.startswith(file_startswith), files))
            # add the folder path back on
            files = list(map(lambda f: os.path.join(path, f), files))
        else:
            # only include .hxx files
            files = list(filter(lambda f: f.endswith('.hxx'), files))
            # add the folder path back on
            files = list(map(lambda f: path + f, files))
        return files

    @staticmethod
    def sh(command: str, piped: Optional[bool] = False) -> Tuple[str, int]:
        try:
            if piped is True:
                proc = subprocess.Popen(command,
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        shell=True)
            else:
                proc = subprocess.Popen(command,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        shell=True)
            stdout, stderr = proc.communicate()
            stderr = stderr.decode('utf-8')
            if stderr != '':
                return stderr, 1
            return stdout.decode('utf-8'), 0
        except FileNotFoundError:
            return "Error: Command not found.", 1

    @staticmethod
    def find_llvm() -> None:
        if sys.platform == 'darwin':
            output, err = CppTypeParser.sh('which clang')
            if err:
                raise Exception(f'Unable to determine clang binary. Error code: {err}.')
            if 'llvm' not in output.strip():
                # TODO: aarch64 v. x86_64
                os_path = os.environ.get('PATH').split(':')
                os_path = ['/opt/homebrew/opt/llvm/bin'] + os_path
                os.environ.update(**{'PATH': ':'.join(os_path)})
            output, err = CppTypeParser.sh('which clang')
            if err:
                raise Exception(f'Unable to determine clang binary. Error code: {err}.')
            if 'llvm' not in output.strip():
                raise Exception('Unable to set LLVM as default.')
        elif sys.platform == 'linux':
            print('Under construction')
        else:
            raise ValueError('Unsupported platform')

    @staticmethod
    def get_llvm_version() -> str:
        output, err = CppTypeParser.sh('llvm-config --version')
        if err:
            raise Exception(f'Unable to determine LLVM version. Error code: {err}')
        return output.strip()

    @staticmethod
    def get_llvm_includedir() -> str:
        output, err = CppTypeParser.sh('llvm-config --includedir')
        if err:
            raise Exception(f'Unable to determine LLVM includedir. Error code: {err}')
        return output.strip()

    @staticmethod
    def get_llvm_libdir() -> str:
        output, err = CppTypeParser.sh('llvm-config --libdir')
        if err:
            raise Exception(f'Unable to determine LLVM libdir. Error code: {err}')
        return output.strip()

    @staticmethod
    def get_system_headers() -> str:
        if sys.platform == 'darwin':
            output, err = CppTypeParser.sh('xcrun --show-sdk-path')
            if err:
                raise Exception(f'Unable to determine system header path. Error code: {err}.')
            return output.strip()
        elif sys.platform == 'linux':
            print('Under construction')
        else:
            raise ValueError('Unsupported platform')
