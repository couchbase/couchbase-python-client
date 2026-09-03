#  Copyright 2016-2024. Couchbase, Inc.
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

"""
EnvironmentFeatures: single source of truth for feature-flag checks.

A feature is gated on a minimum server version (FEATURE_MINIMUM_VERSIONS, mapping the
feature to that version as a float), optionally refined by a patch level
(FEATURE_PATCH_MINIMUMS).  Mock support is gated separately, through FEATURES_NOT_IN_MOCK
and FEATURES_IN_MOCK, because the mock either implements a feature or does not: there is no
version to compare against.

Deployment is a third axis, FEATURE_SERVICES, and it is independent of the other two: a
7.6 cluster is new enough for analytics and still cannot serve it if no node runs the
analytics service.  Version answers "is the software recent enough", deployment answers
"was this service asked for at all", and the two are not derivable from each other.  It is
checked last, so a cluster failing both still reports the version.

All four facts travel together as a ServerContext, which every entry point takes as its
single argument after the feature.  Callers build one through `cb_env.server_context`
rather than assembling it themselves.

Features may be looked up by ServerFeatures member or by its string value
(e.g. 'kv_range_scan').

Skip reasons are built through tests_v2.skips, so every skip this module produces carries a
category and parses, and reports the detected version and mock type.  Two conditions that
resemble an absent feature are not one, and raise rather than skip: an unreadable server
version is a broken connection, not an old server.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import NoReturn

import pytest

from couchbase.diagnostics import ServiceType
from tests_v2.mock_server import MockServerType
from tests_v2.skips import (SkipCategory,
                            env_degraded,
                            server_label,
                            skip_feature_unsupported,
                            skip_reason)


class ServerFeatures(Enum):
    KeyValue = 'kv'
    SSL = 'ssl'
    Views = 'views'
    SpatialViews = 'spatial_views'
    Diagnostics = 'diagnostics'
    SynchronousDurability = 'sync_durability'
    Query = 'query'
    Subdoc = 'subdoc'
    Xattr = 'xattr'
    Search = 'search'
    Analytics = 'analytics'
    Collections = 'collections'
    Replicas = 'replicas'
    UserManagement = 'user_mgmt'
    BasicBucketManagement = 'basic_bucket_mgmt'
    BucketManagement = 'bucket_mgmt'
    BucketMinDurability = 'bucket_min_durability'
    BucketStorageBackend = 'bucket_storage_backend'
    CustomConflictResolution = 'custom_conflict_resolution'
    QueryIndexManagement = 'query_index_mgmt'
    QueryContext = 'query_context'
    QueryUserDefinedFunctions = 'query_user_defined_functions'
    SearchIndexManagement = 'search_index_mgmt'
    ViewIndexManagement = 'view_index_mgmt'
    GetMeta = 'get_meta'
    AnalyticsPendingMutations = 'analytics_pending_mutations'
    AnalyticsLinkManagement = 'analytics_link_mgmt'
    AnalyticsScopeQueryFqdn = 'analytics_scope_query_fqdn'
    AnalyticsDataversePathNames = 'analytics_dataverse_path_names'
    UserGroupManagement = 'user_group_mgmt'
    PreserveExpiry = 'preserve_expiry'
    SearchDisableScoring = 'search_disable_scoring'
    SearchAnalyzeDocument = 'search_analyze_document'
    Eventing = 'eventing'
    EventingFunctionManagement = 'eventing_function_mgmt'
    RateLimiting = 'rate_limiting'
    Txns = 'txns'
    TxnQueries = 'txn_queries'
    KeyValueRangeScan = 'kv_range_scan'
    SubdocReplicaRead = 'subdoc_replica_read'
    UpdateCollection = 'update_collection'
    UpdateCollectionMaxExpiry = 'update_collection_max_expiry'
    NegativeCollectionMaxExpiry = 'negative_collection_max_expiry'
    NonDedupedHistory = 'non_deduped_history'
    QueryWithoutIndex = 'query_without_index'
    NotLockedKVStatus = 'kv_not_locked'
    ScopeSearch = 'scope_search'
    ScopeSearchIndexManagement = 'scope_search_index_mgmt'
    ScopeEventingFunctionManagement = 'scope_eventing_function_mgmt'
    BinaryTxns = 'binary_txns'
    ServerGroups = 'server_groups'
    Magma128Buckets = 'magma_128_buckets'


class TestProtocol(Enum):
    """Which wire protocol a test run is targeting.

    Local to tests_v2/, not imported from the SDK.  The SDK's own couchbase_protocol is
    an internal dispatch key naming how a request is encoded; this names which backend the
    test run targets, which is a property of the run and not of any request in it.  The two
    would be the same enum only by coincidence, and sharing one would make the test tree
    depend on an internal that is free to change.
    """
    CLASSIC = 'classic'
    COUCHBASE2 = 'couchbase2'  # reserved; the SDK cannot connect over it yet


@dataclass(frozen=True)
class ServerContext:
    """Everything the feature gate knows about the server under test.

    One argument rather than four separate ones, and that is not only brevity.  The patch
    level is the fact a caller is most likely to leave out, and the patch path reads a
    missing patch as -1: omitting it skips the test on exactly the boundary version,
    reporting a 7.6.2 server as too old for a 7.6.2 feature.  Nothing distinguishes a call
    site that judged the patch irrelevant from one that forgot it, and the gate cannot tell
    either, because both arrive as None.  A context carries all four facts or none, so the
    question stops being per-call-site.

    Frozen because a gate that could observe one of these change mid-call would be reporting
    on two different servers.  It is not a cache, though: `TestEnvironment.server_context`
    builds a fresh one per access from the live cluster attributes, so freezing constrains
    the object handed to the gate and makes no claim about when the version was read.

    `services` is None when the question was not asked rather than answered empty.  See
    TestEnvironment.available_services for which targets are not asked and why, and
    _service_gate for what None does here.
    """
    version: float | None
    patch: int | None = None
    mock_type: MockServerType | None = None
    services: frozenset[ServiceType] | None = None

    @property
    def is_mock(self) -> bool:
        return self.mock_type is not None

    @property
    def label(self) -> str:
        """`server=` value for a skip reason: 'mock' or 'real'."""
        return server_label(self.is_mock)


class EnvironmentFeatures:
    """Feature-flag system for test infrastructure.

    Single import path: from tests_v2.test_features import EnvironmentFeatures, ServerFeatures
    """

    # Features always present regardless of server version or mock type
    BASIC_FEATURES: list[ServerFeatures] = [
        ServerFeatures.KeyValue,
        ServerFeatures.Diagnostics,
        ServerFeatures.SSL,
        ServerFeatures.SpatialViews,
        ServerFeatures.Subdoc,
        ServerFeatures.Views,
        ServerFeatures.Replicas,
        ServerFeatures.BasicBucketManagement,
    ]

    # Features no mock server supports
    FEATURES_NOT_IN_MOCK: list[ServerFeatures] = [
        ServerFeatures.Analytics,
        ServerFeatures.AnalyticsScopeQueryFqdn,
        ServerFeatures.BucketManagement,
        ServerFeatures.EventingFunctionManagement,
        ServerFeatures.GetMeta,
        ServerFeatures.Query,
        ServerFeatures.QueryIndexManagement,
        ServerFeatures.QueryUserDefinedFunctions,
        ServerFeatures.RateLimiting,
        ServerFeatures.Search,
        ServerFeatures.SearchIndexManagement,
        ServerFeatures.SearchAnalyzeDocument,
        ServerFeatures.TxnQueries,
        ServerFeatures.UserGroupManagement,
        ServerFeatures.UserManagement,
        ServerFeatures.ViewIndexManagement,
        ServerFeatures.NonDedupedHistory,
        ServerFeatures.UpdateCollection,
        ServerFeatures.ScopeEventingFunctionManagement,
        ServerFeatures.BinaryTxns,
        ServerFeatures.ServerGroups,
    ]

    # Features that GoCAVES mock supports (real server also supports these)
    FEATURES_IN_MOCK: list[ServerFeatures] = [
        ServerFeatures.Txns,
    ]

    # ---------------------------------------------------------------------------
    # Version-gated features
    # Maps ServerFeatures → minimum server version (as float, e.g. 7.1 means >= 7.1.0)
    # ---------------------------------------------------------------------------
    FEATURE_MINIMUM_VERSIONS: dict[ServerFeatures, float] = {
        # 5.5+
        ServerFeatures.BucketManagement:        5.5,
        ServerFeatures.GetMeta:                 5.5,
        ServerFeatures.Query:                   5.5,
        ServerFeatures.QueryIndexManagement:    5.5,
        ServerFeatures.Search:                  5.5,
        ServerFeatures.SearchIndexManagement:   5.5,
        ServerFeatures.ViewIndexManagement:     5.5,
        # 6.0+
        ServerFeatures.Analytics:               6.0,
        ServerFeatures.AnalyticsScopeQueryFqdn: 6.0,
        ServerFeatures.UserManagement:          6.0,
        # 6.5+
        ServerFeatures.AnalyticsPendingMutations: 6.5,
        ServerFeatures.UserGroupManagement:     6.5,
        ServerFeatures.SynchronousDurability:   6.5,
        ServerFeatures.SearchDisableScoring:    6.5,
        ServerFeatures.SearchAnalyzeDocument:   6.5,
        # 6.6+
        ServerFeatures.BucketMinDurability:     6.6,
        ServerFeatures.Txns:                    6.6,
        # 7.0+
        ServerFeatures.Collections:             7.0,
        ServerFeatures.AnalyticsLinkManagement: 7.0,
        # 7.0 is where a dataverse name may be a `a/b` path rather than a single
        # identifier, which changes both what the analytics manager accepts and how a
        # name has to be spelled to address a scope.  Named as a feature because three
        # sites in analyticsmgmt_t branch on it: two tests that assert the accepted
        # syntax on either side of the boundary, and the fixture that picks a dataverse
        # name the server under test will take.
        ServerFeatures.AnalyticsDataversePathNames: 7.0,
        ServerFeatures.TxnQueries:              7.0,
        # The query service only understands a query context, and therefore scope- and
        # collection-level index statements, from 7.0 on.  The SDK's query index manager
        # builds every statement through one, so on 6.6 the manager cannot be used at all
        # and callers have to fall back to raw CREATE/DROP PRIMARY INDEX statements.
        # QueryIndexManagement above says "the service exists"; this says "the manager API
        # works", which is a strictly later version and a separate question.
        ServerFeatures.QueryContext:            7.0,
        # 7.1+
        ServerFeatures.RateLimiting:            7.1,
        ServerFeatures.BucketStorageBackend:    7.1,
        ServerFeatures.CustomConflictResolution: 7.1,
        ServerFeatures.EventingFunctionManagement: 7.1,
        ServerFeatures.PreserveExpiry:          7.1,
        ServerFeatures.QueryUserDefinedFunctions: 7.1,
        ServerFeatures.ScopeEventingFunctionManagement: 7.1,
        # 7.2+
        ServerFeatures.NonDedupedHistory:       7.2,
        ServerFeatures.UpdateCollection:        7.2,
        # 7.5+
        ServerFeatures.KeyValueRangeScan:       7.5,
        ServerFeatures.SubdocReplicaRead:       7.5,
        ServerFeatures.UpdateCollectionMaxExpiry: 7.5,
        ServerFeatures.QueryWithoutIndex:       7.5,
        # 7.6+
        ServerFeatures.NotLockedKVStatus:       7.6,
        ServerFeatures.NegativeCollectionMaxExpiry: 7.6,
        ServerFeatures.ScopeSearch:             7.6,
        ServerFeatures.ScopeSearchIndexManagement: 7.6,
        # 8.0+
        ServerFeatures.Magma128Buckets:         8.0,
    }

    # Features with patch-level minimum versions.
    # Maps ServerFeatures → (minor_version_float, minimum_patch_int)
    # e.g. (7.6, 2) means >= 7.6.2
    FEATURE_PATCH_MINIMUMS: dict[ServerFeatures, tuple[float, int]] = {
        ServerFeatures.BinaryTxns:   (7.6, 2),
        ServerFeatures.ServerGroups: (7.6, 2),
    }

    # Features not supported above a certain server version
    FEATURE_MAXIMUM_VERSIONS: dict[ServerFeatures, float] = {
        ServerFeatures.RateLimiting: 7.2,  # not supported > 7.2
        # The analytics service stopped accepting a three-part `bucket`.`scope`.`collection`
        # name from a scope-level query at 7.1, so this is the last version that runs it.
        ServerFeatures.AnalyticsScopeQueryFqdn: 7.0,
    }

    # Version-gated on a real server, but GoCAVES supports them whatever version it reports
    GOCAVES_ONLY_FEATURES: list[ServerFeatures] = [
        ServerFeatures.Collections,
        ServerFeatures.TxnQueries,
    ]

    # ---------------------------------------------------------------------------
    # Deployment-gated features
    # Maps ServerFeatures -> the ServiceType that must be deployed on the cluster.
    #
    # A feature absent from this map has no deployment gate, which is a claim and not a
    # default: key-value, views and the management API ride on nodes the cluster cannot be
    # without, so there is nothing to check.  Views in particular are served by the data
    # nodes rather than by a service of their own, so `ServerFeatures.Views` is deliberately
    # unmapped rather than mapped to KeyValue, which would report a service name the
    # cluster's own service list never uses.
    #
    # Known limit: the GSI index service (`index` in the cluster's service list) has no
    # ServiceType member, so QueryIndexManagement gates on the query service alone.  A
    # cluster running n1ql with no index service passes this gate and fails at index
    # creation.  That deployment is possible and nobody runs it; modelling it would mean
    # widening the map's value type for a case with no reported instance.
    # ---------------------------------------------------------------------------
    FEATURE_SERVICES: dict[ServerFeatures, ServiceType] = {
        # query service (`n1ql`)
        ServerFeatures.Query:                     ServiceType.Query,
        ServerFeatures.QueryContext:              ServiceType.Query,
        ServerFeatures.QueryIndexManagement:      ServiceType.Query,
        ServerFeatures.QueryUserDefinedFunctions: ServiceType.Query,
        ServerFeatures.QueryWithoutIndex:         ServiceType.Query,
        ServerFeatures.TxnQueries:                ServiceType.Query,
        # analytics service (`cbas`)
        ServerFeatures.Analytics:                 ServiceType.Analytics,
        ServerFeatures.AnalyticsLinkManagement:   ServiceType.Analytics,
        ServerFeatures.AnalyticsPendingMutations: ServiceType.Analytics,
        ServerFeatures.AnalyticsScopeQueryFqdn:   ServiceType.Analytics,
        ServerFeatures.AnalyticsDataversePathNames: ServiceType.Analytics,
        # search service (`fts`)
        ServerFeatures.Search:                    ServiceType.Search,
        ServerFeatures.SearchDisableScoring:      ServiceType.Search,
        ServerFeatures.SearchIndexManagement:     ServiceType.Search,
        ServerFeatures.SearchAnalyzeDocument:     ServiceType.Search,
        ServerFeatures.ScopeSearch:               ServiceType.Search,
        ServerFeatures.ScopeSearchIndexManagement: ServiceType.Search,
        # eventing service (`eventing`)
        ServerFeatures.Eventing:                  ServiceType.Eventing,
        ServerFeatures.EventingFunctionManagement: ServiceType.Eventing,
        ServerFeatures.ScopeEventingFunctionManagement: ServiceType.Eventing,
    }

    @staticmethod
    def _resolve_feature(feature: ServerFeatures | str) -> ServerFeatures:
        """Coerce a string feature name to ServerFeatures enum, or pass through if already enum."""
        if isinstance(feature, ServerFeatures):
            return feature
        # Try matching by value (e.g. 'kv_range_scan')
        for f in ServerFeatures:
            if f.value == feature:
                return f
        raise ValueError(f"Unknown feature: {feature!r}")

    @staticmethod
    def mock_type_label(mock_server_type: MockServerType | None) -> str:
        return 'real server' if mock_server_type is None else 'GoCAVES mock'

    @staticmethod
    def is_feature_supported(feature: ServerFeatures | str, ctx: ServerContext) -> bool:
        """Return True when the feature is available, False when it is legitimately absent.

        Deliberately *not* exception-proof.  Wrapping the body in `except Exception: return
        False` would make every failure (an unreachable cluster, a typo in a feature name, a
        bug in the gate itself) indistinguishable from "the server does not have this
        feature".  A caller that drops a manager or skips a test on the strength of that
        answer loses coverage silently, which is the exact false-clean this taxonomy exists
        to prevent.  A broken environment or a bad lookup propagates instead.
        """
        return EnvironmentFeatures.supports_feature(feature, ctx) is None

    @staticmethod
    def check_if_feature_supported(features: ServerFeatures | str | list,
                                   ctx: ServerContext) -> None:
        """Skip the current test if any of the features is unsupported.

        The skip reason is a parseable `SKIP[...]` string naming the category, the feature,
        the required and observed versions, and whether the run is against a mock.

        Nothing here catches TypeError to skip on.  Doing so turns a broken connection into
        a benign-looking skip, and a `TypeError` anywhere in the gate into the same thing.
        Both propagate instead: supports_feature raises env-degraded when it cannot read the
        version, and a TypeError is a bug that should surface as one.
        """
        features_list: list = []
        if isinstance(features, (str, ServerFeatures)):
            features_list.append(features)
        else:
            features_list.extend(features)

        for feature in features_list:
            reason = EnvironmentFeatures.supports_feature(feature, ctx)
            if reason is not None:
                pytest.skip(reason)

    @staticmethod
    def check_if_feature_not_supported(features: ServerFeatures | str | list,
                                       ctx: ServerContext) -> None:
        """Skip if any of the features IS supported (inverse of above).

        Same category as the forward gate: both directions are the feature gate deciding the
        test does not apply, and both are countable from the server version and mock type
        alone.  `gate=requires-absent` records which direction fired, so the two are still
        distinguishable in a parsed run.
        """
        features_list: list = []
        if isinstance(features, (str, ServerFeatures)):
            features_list.append(features)
        else:
            features_list.extend(features)

        for feature in features_list:
            reason = EnvironmentFeatures.supports_feature(feature, ctx)
            if reason is None:
                f = EnvironmentFeatures._resolve_feature(feature)
                skip_feature_unsupported(feature=f,
                                         gate='requires-absent',
                                         observed=ctx.version,
                                         server=ctx.label,
                                         note=f'test only applies where {f.name} is absent')

    @staticmethod
    def supports_feature(feature: ServerFeatures | str, ctx: ServerContext) -> str | None:  # noqa: C901
        """Return None when supported; return a parseable `SKIP[...]` reason when not.

        Raises CouchbaseTestEnvironmentException when the answer is unknowable rather than
        negative: an unreadable server version means the connection is broken, and reporting
        that as an unsupported feature is what lets a run lose coverage and stay green.

        Args:
            feature: A ServerFeatures member or its string .value.
            ctx:     The server under test, normally `cb_env.server_context`.
        """
        ef = EnvironmentFeatures
        f = ef._resolve_feature(feature)
        is_mock = ctx.is_mock
        srv = ctx.label

        # ---- Always available ----
        if f in ef.BASIC_FEATURES:
            return None

        # ---- Never available on any mock ----
        if f in ef.FEATURES_NOT_IN_MOCK:
            if is_mock:
                return skip_reason(SkipCategory.MOCK_LIMITATION,
                                   feature=f,
                                   server=srv,
                                   mock=ctx.mock_type,
                                   note='requires a real Couchbase Server')

        # ---- Available on the mock regardless of reported server version ----
        if f in ef.FEATURES_IN_MOCK:
            if is_mock:
                return None

        # ---- Maximum version check (feature removed after a version) ----
        if f in ef.FEATURE_MAXIMUM_VERSIONS:
            max_ver = ef.FEATURE_MAXIMUM_VERSIONS[f]
            if ctx.version is not None and ctx.version > max_ver:
                return skip_reason(SkipCategory.FEATURE_UNSUPPORTED,
                                   feature=f,
                                   max=max_ver,
                                   observed=ctx.version,
                                   server=srv,
                                   note='feature was removed after this version')

        # ---- Patch-level minimum version check ----
        if f in ef.FEATURE_PATCH_MINIMUMS:
            if is_mock:
                return skip_reason(SkipCategory.MOCK_LIMITATION,
                                   feature=f,
                                   server=srv,
                                   mock=ctx.mock_type,
                                   note='requires a real Couchbase Server')
            min_minor, min_patch = ef.FEATURE_PATCH_MINIMUMS[f]
            if ctx.version is None:
                # Not a fall-through to `return None`, which would let an unreadable
                # version silently *allow* the feature and leave the test failing for an
                # unrelated-looking reason, and not a skip either.  The answer is not
                # knowable here; the connection is broken.
                ef._unknown_version(f, srv, required=f'{min_minor}.{min_patch}')
            if ctx.version < min_minor:
                return skip_reason(SkipCategory.FEATURE_UNSUPPORTED,
                                   feature=f,
                                   required=f'{min_minor}.{min_patch}',
                                   observed=ctx.version,
                                   server=srv)
            if ctx.version == min_minor:
                # An unreadable patch level is treated as too old, which is the safe
                # direction: the alternative is running a test the server cannot serve and
                # reading the failure as a defect.  It is reported as `?` rather than as the
                # -1 the comparison uses, because a version string ending in `.-1` reads as a
                # parser bug in the gate rather than as a fact about the server.
                patch = ctx.patch if ctx.patch is not None else -1
                if patch < min_patch:
                    observed = f'{ctx.version}.{ctx.patch if ctx.patch is not None else "?"}'
                    return skip_reason(SkipCategory.FEATURE_UNSUPPORTED,
                                       feature=f,
                                       required=f'{min_minor}.{min_patch}',
                                       observed=observed,
                                       server=srv)
            return ef._service_gate(f, ctx)

        # ---- Standard minimum version check ----
        if f in ef.FEATURE_MINIMUM_VERSIONS:
            min_ver = ef.FEATURE_MINIMUM_VERSIONS[f]
            if is_mock:
                # GoCAVES supports Collections / TxnQueries regardless of its reported version
                if f in ef.GOCAVES_ONLY_FEATURES:
                    return None
                # All other version-gated features are real-server-only
                return skip_reason(SkipCategory.MOCK_LIMITATION,
                                   feature=f,
                                   required=min_ver,
                                   server=srv,
                                   mock=ctx.mock_type,
                                   note='version-gated feature is real-server-only')
            if ctx.version is None:
                # Not a skip.  A real server that cannot report its version is not an old
                # server, it is an unreachable one, and skipping here would quietly drop
                # every test gated on any version-gated feature.
                ef._unknown_version(f, srv, required=min_ver)
            if ctx.version < min_ver:
                return skip_reason(SkipCategory.FEATURE_UNSUPPORTED,
                                   feature=f,
                                   required=min_ver,
                                   observed=ctx.version,
                                   server=srv)
            return ef._service_gate(f, ctx)

        # No version gate applies.  Still ask the deployment: a feature can be old enough to
        # need no minimum and still ride on a service the cluster does not run.  An unknown
        # feature name reaches here too and is conservatively allowed; it is absent from
        # FEATURE_SERVICES, so the gate below waves it through.
        return ef._service_gate(f, ctx)

    @staticmethod
    def _service_gate(feature: ServerFeatures, ctx: ServerContext) -> str | None:
        """Return a `service-absent` reason, or None when the deployment can serve the feature.

        Reached only from the tail of a version path, never before one, and that ordering is
        the decision worth knowing.  A 6.0 cluster with no analytics node fails both gates,
        and the version is the more actionable of the two answers: it names something an
        upgrade fixes, where the service list describes how that same cluster was
        provisioned.

        Two ways to get None, and they are not the same thing.  A feature absent from
        FEATURE_SERVICES has no deployment gate, so there is nothing to ask.  `ctx.services`
        of None means the question was not asked at all, on the targets
        TestEnvironment.available_services names.  Waving the feature through is right for
        the mock, where FEATURES_NOT_IN_MOCK has already answered for every feature this map
        covers, and is the widest of the couchbase2 gate gaps.
        """
        service = EnvironmentFeatures.FEATURE_SERVICES.get(feature)
        if service is None or ctx.services is None:
            return None
        if service in ctx.services:
            return None
        return skip_reason(SkipCategory.SERVICE_ABSENT,
                           feature=feature,
                           service=service.value,
                           deployed=','.join(sorted(s.value for s in ctx.services)),
                           server=ctx.label)

    @staticmethod
    def _unknown_version(feature: ServerFeatures, srv: str, *, required: float | str) -> NoReturn:
        """Fail because the gate cannot be evaluated, not because the answer is no."""
        env_degraded(stage='feature-gate',
                     feature=feature,
                     required=required,
                     observed=None,
                     server=srv,
                     note='could not read the server version; check the connection')
