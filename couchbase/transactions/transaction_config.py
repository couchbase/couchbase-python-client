from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional,
                    overload)

from couchbase.pycbc_core import per_transaction_config, transaction_config

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase.collection import Collection
    from couchbase.durability import ServerDurability
    from couchbase.logic.n1ql import QueryScanConsistency


class TransactionConfig:
    _TXN_ALLOWED_KEYS = {"durability_level", "cleanup_window", "kv_timeout",
                         "expiration_time", "cleanup_lost_attempts", "cleanup_client_attempts",
                         "custom_metadata_collection", "scan_consistency"}

    @overload
    def __init__(self,
                 durability=None,   # type: Optional[ServerDurability]
                 cleanup_window=None,  # type: Optional[timedelta]
                 kv_timeout=None,  # type: Optional[timedelta]
                 expiration_time=None,  # type: Optional[timedelta]
                 cleanup_lost_attempts=None,  # type: Optional[bool]
                 cleanup_client_attempts=None,  # type: Optional[bool]
                 custom_metadata_collection=None,  # type: Optional[Collection]
                 scan_consistency=None  # type: Optional[QueryScanConsistency]
                 ):
        pass

    def __init__(self,
                 **kwargs  # type: dict[str, Any]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if k in TransactionConfig._TXN_ALLOWED_KEYS}
        # convert everything here...
        if kwargs.get("durability_level", None):
            kwargs["durability_level"] = kwargs["durability_level"].level.value
        for k in ["cleanup_window", "kv_timeout", "expiration_time"]:
            if kwargs.get(k, None):
                kwargs[k] = int(kwargs[k].total_seconds() * 1000000)
        coll = kwargs.pop("custom_metadata_collection", None)
        if coll:
            kwargs["metadata_bucket"] = coll._scope.bucket.name
            kwargs["metadata_scope"] = coll._scope.name
            kwargs["metadata_colleciton"] = coll.name

        # don't pass None
        for key in [k for k, v in kwargs.items() if v is None]:
            del(kwargs[key])

        # TODO: handle scan consistency
        print(f'creating transaction_config with {kwargs}')
        self._base = transaction_config(**kwargs)

    def __str__(self):
        return f'TransactionConfig{{{self._base}}}'


class PerTransactionConfig:
    _TXN_ALLOWED_KEYS = {"durability_level", "kv_timeout", "expiration_time", "scan_consistency"}

    @overload
    def __init__(self,
                 durability=None,   # type: Optional[ServerDurability]
                 cleanup_window=None,  # type: Optional[timedelta]
                 kv_timeout=None,  # type: Optional[timedelta]
                 expiration_time=None,  # type: Optional[timedelta]
                 cleanup_lost_attempts=None,  # type: Optional[bool]
                 cleanup_client_attempts=None,  # type: Optional[bool]
                 custom_metadata_collection=None,  # type: Optional[Collection]
                 scan_consistency=None  # type: Optional[QueryScanConsistency]
                 ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        print(f'creating new PerTransactionConfig from {kwargs}')
        kwargs = {k: v for k, v in kwargs.items() if k in TransactionConfig._TXN_ALLOWED_KEYS}
        # convert everything here...
        if kwargs.get("durability_level", None):
            kwargs["durability_level"] = kwargs["durability_level"].level.value
        for k in ["kv_timeout", "expiration_time"]:
            if kwargs.get(k, None):
                kwargs[k] = int(kwargs[k].total_seconds() * 1000000)

        # don't pass None
        for key in [k for k, v in kwargs.items() if v is None]:
            del(kwargs[key])

        # TODO: handle scan consistency
        print(f'creating per_transaction_config with {kwargs}')
        self._base = per_transaction_config(**kwargs)

    def __str__(self):
        return f'PerTransactionConfig(base_:{self._base}'
