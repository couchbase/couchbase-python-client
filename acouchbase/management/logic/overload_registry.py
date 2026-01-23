import builtins
import sys
from dataclasses import dataclass
from inspect import (BoundArguments,
                     Parameter,
                     Signature,
                     signature)
from typing import (Any,
                    Callable,
                    Coroutine,
                    Dict,
                    List,
                    Mapping,
                    Optional,
                    Union,
                    get_origin,
                    get_type_hints)

from couchbase.logic.supportability import Supportability
from couchbase.logic.validation import validate_type


@dataclass
class AsyncMethodSignature:
    fn: Callable[..., Coroutine[Any, Any, Any]]
    type_hints: Dict[str, Any] = None


class AsyncOverloadedMethod:
    """**INTERNAL**
    Private class.  Do not use directly.
    """

    def __init__(self,
                 fn: Callable[..., Coroutine[Any, Any, Any]],
                 is_default: Optional[bool] = False,
                 overload_types: Optional[List[str]] = None,
                 issue_warning_if_deprecated: Optional[bool] = True
                 ) -> None:
        self._name: str = fn.__qualname__
        self._sig_map: Dict[Signature, AsyncMethodSignature] = {}
        self._default_fn: Callable[..., Coroutine[Any, Any, Any]] = None
        self._default_sig: Signature = None
        self._overload_types: List[str] = overload_types
        self._issue_warning_if_deprecated: bool = issue_warning_if_deprecated
        self.register(fn, is_default=is_default)

    @property
    def name(self) -> str:
        return self._name

    def issue_warning(self, used_sig: Signature) -> None:
        if self._issue_warning_if_deprecated is False:
            return
        if self._default_fn is None:
            raise TypeError(f'Default not registered {self.name}.')
        if used_sig != self._default_sig:
            Supportability.method_signature_deprecated(self.name, used_sig, self._default_sig)

    def register(self,
                 fn: Callable[..., Coroutine[Any, Any, Any]],
                 is_default: Optional[bool] = False,
                 overload_types: Optional[List[str]] = None) -> None:
        sig: Signature = signature(fn)
        if sig in self._sig_map:
            raise TypeError(f'Already registered {self._name} with signature {sig}.')
        self._sig_map[sig] = AsyncMethodSignature(fn, get_type_hints(fn))
        if is_default is True:
            self.register_default(fn)
        self._add_overload_types(overload_types)

    def register_default(self, fn: Callable[..., Coroutine[Any, Any, Any]]) -> None:
        if self._default_fn is not None:
            raise TypeError(f'Default function already registered for {self._name}.')
        self._default_fn = fn
        self._default_sig = signature(fn)

    def _add_overload_types(self, overload_types: Optional[List[str]] = None) -> None:
        if overload_types:
            if self._overload_types:
                self._overload_types.extend([t for t in overload_types if t not in self._overload_types])
            else:
                self._overload_types = overload_types

    def _get_annotation(self, annotation: Any) -> Any:
        if not self._overload_types:
            return annotation

        if annotation in ['bool', 'bytes', 'bytearray', 'complex', 'float', 'int', 'str']:
            return getattr(builtins, annotation)

        if get_origin(annotation) is Union and type(None):
            pass
        overload_match = None
        for t in self._overload_types:
            type_tokens = t.split('.')
            if type_tokens[len(type_tokens) - 1] == annotation:
                overload_match = t
                break

        if overload_match:
            overload_type = getattr(sys.modules['.'.join(
                overload_match.split('.')[:-1])], overload_match.split('.')[-1])
            return overload_type

        return annotation

    def _validate_params(self,
                         params: Mapping[str, Parameter],
                         bound_args: BoundArguments,
                         type_hints: Dict[str, Any]) -> bool:
        valid = True
        for arg_name, arg in bound_args.arguments.items():
            # not worried about matching self, this will happen often as our fn == class.method
            if params[arg_name].annotation is Signature.empty and params[arg_name].name == 'self':
                continue
            # not worried about positional only or keyword only b/c w/in the scope of the Python SDK
            # this means we are at the options/kwargs portion of the method signature and therefore
            # we know we are only dealing w/ options (and keyword overrides for said options)
            if params[arg_name].kind in [Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD]:
                continue
            # annotation = self._get_annotation(params[arg_name].annotation)
            annotation = type_hints.get(arg_name)
            if not validate_type(arg, annotation):
                valid = False
                break

        return valid

    async def __call__(self, *args: Any, **kwargs: Dict[str, Any]) -> Any:
        for sig, meth_sig in self._sig_map.items():
            params = sig.parameters
            try:
                bound_args = sig.bind(*args, **kwargs)
            except TypeError:
                continue
            else:
                if self._validate_params(params, bound_args, meth_sig.type_hints):
                    self.issue_warning(sig)
                    return await meth_sig.fn(*args, **kwargs)

        # None of the functions in the signature map can be called, call the default one if it exists
        if self._default_fn is None:
            raise TypeError(f'Unable to find appropriate registered overload for {self._name}.')
        return await self._default_fn(*args, **kwargs)


class AsyncOverloadRegistry(dict):
    """**INTERNAL**
    Private class.  Do not use directly.
    """

    def __init__(self) -> None:
        super().__init__()

    def register_method(self,
                        fn: Callable[..., Coroutine[Any, Any, Any]],
                        is_default: Optional[bool] = False,
                        overload_types: Optional[List[str]] = None,
                        issue_warning_if_deprecated: Optional[bool] = False) -> None:
        meth = AsyncOverloadedMethod(fn,
                                     is_default=is_default,
                                     overload_types=overload_types,
                                     issue_warning_if_deprecated=issue_warning_if_deprecated)
        self[meth.name] = meth

    def add_overload(self,
                     fn: Callable[..., Coroutine[Any, Any, Any]],
                     is_default: Optional[bool] = False,
                     overload_types: Optional[List[str]] = None) -> None:
        meth: AsyncOverloadedMethod = self.get(fn.__qualname__)
        meth.register(fn, is_default=is_default, overload_types=overload_types)

    def __getitem__(self, key: str) -> AsyncOverloadedMethod:
        return super().__getitem__(key)

    def __setitem__(self, key: str, item: AsyncOverloadedMethod) -> None:
        return super().__setitem__(key, item)
