from __future__ import annotations

import datetime
import random
from typing import Any

from faker import Faker
from faker.providers import BaseProvider
from faker_vehicle import VehicleProvider

fake = Faker('en_US')
fake.add_provider(VehicleProvider)

# Fraction of vehicle docs that will contain the FTS keyword phrase.
# Fixed at 25% so at least one doc is always FTS-matchable in any reasonable
# batch size (>= 4 docs).
_FTS_KEYWORD_FRACTION = 0.25
_FTS_KEYWORD = 'great auto deal'


class DealershipProvider(BaseProvider):
    def dealership(
        self, dealer_uuid: str, vehicle_ids: list[str], idx: int
    ) -> dict[str, Any]:
        dealer: dict[str, Any] = {
            'batch': dealer_uuid,
            'id': f'{dealer_uuid}::{idx}',
            'name': fake.company(),
            'address': fake.street_address(),
            'city': fake.city(),
            'country': fake.current_country(),
            'country_code': fake.current_country_code(),
            'geo': self._get_geo(),
            'motto': fake.catch_phrase(),
            'general_manager': {
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'phone': fake.phone_number(),
                'email': fake.company_email(),
            },
        }

        today = datetime.datetime.now()
        prev_year = today - datetime.timedelta(days=365)
        num_reps = fake.random_int(min=2, max=5)
        reps: list[dict[str, Any]] = []
        for _ in range(num_reps):
            rep: dict[str, Any] = {
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'phone': fake.phone_number(),
                'email': fake.company_email(),
                'rating': fake.random_int(min=1, max=5),
                'reviews': [],
            }
            num_reviews = fake.random_int(min=2, max=5)
            for _ in range(num_reviews):
                rep['reviews'].append(
                    {
                        'date': fake.date_time_between(prev_year).isoformat(),
                        'comment': fake.sentence(nb_words=10),
                        'categories': {
                            'service': fake.random_int(min=1, max=5),
                            'friendliness': fake.random_int(min=1, max=5),
                            'honesty': fake.random_int(min=1, max=5),
                            'knowledgeable': fake.random_int(min=1, max=5),
                            'responsive': fake.random_int(min=1, max=5),
                        },
                    }
                )
            reps.append(rep)

        dealer['sales_reps'] = reps

        num_vehicles = fake.random_int(min=5, max=10)
        vehicles = random.choices(vehicle_ids, k=num_vehicles)  # nosec

        prev_30_days = today - datetime.timedelta(days=30)
        dealer['inventory'] = {
            'num_vehicles': num_vehicles,
            'last_updated': fake.date_time_between(prev_30_days).strftime(
                '%Y-%m-%dT%H:%M:%SZ'
            ),
            'vehicles': vehicles,
        }
        dealer['type'] = 'dealership'
        return dealer

    def vehicle(self, vehicle_uuid: str, idx: int) -> dict[str, Any]:
        vehicle: dict[str, Any] = {
            'batch': vehicle_uuid,
            'id': f'{vehicle_uuid}::{idx}',
        }
        base = fake.vehicle_object()
        vehicle.update(**{k.lower(): v for k, v in base.items()})
        manufacturer = {
            'address': fake.street_address(),
            'city': fake.city(),
            'country': fake.current_country(),
            'country_code': fake.current_country_code(),
            'geo': self._get_geo(),
        }
        vehicle['manufacturer'] = manufacturer
        vehicle['description'] = fake.sentence(nb_words=10)
        # FTS keyword injection is applied deterministically in _build_vehicles().
        vehicle['rating'] = fake.random_int(min=1, max=10)
        now = datetime.datetime.now()
        today = datetime.datetime(year=now.year, month=now.month, day=now.day)
        prev_year = today - datetime.timedelta(days=365)
        vehicle['last_updated'] = fake.date_time_between(prev_year).strftime(
            '%Y-%m-%dT%H:%M:%SZ'
        )
        vehicle['type'] = 'vehicle'
        return vehicle

    def _get_geo(self) -> dict[str, Any]:
        # local_latlng() can return None for an unsupported country code; 'US' always
        # resolves for this provider's locale, but guard against it changing later.
        latlng = fake.local_latlng()
        if latlng is None:
            latlng = ('0.0', '0.0', 'Unknown', 'US', 'UTC')
        lat, ln, loc, country_code, tz = latlng
        return {
            'lat': lat,
            'ln': ln,
            'accuracy': random.choice(['ROOFTOP', 'APPROXIMATE', 'RANGE_INTERPOLATED']),  # nosec
            'location': {
                'loc': loc,
                'country_code': country_code,
                'tz': tz,
            },
        }


fake.add_provider(DealershipProvider)


def _batch_id_with_distinct_view_keys() -> str:
    """Return an 8-character batch id whose four 2-character slices are all distinct.

    The views design document emits one 2-character slice of this id per docid decade, so two
    equal slices merge two decades under one view key and invalidate every docid expectation
    the views suites hold (PYCBC-1898).  A raw uuid4 prefix collides about 2.3% of the time,
    which is why this draws in a loop rather than taking the first id it is given; the mean
    cost is 1.02 draws.  Seeding does not help: a seeded run that draws a colliding id fails
    every time instead of one run in 43.
    """
    while True:
        batch_id = fake.uuid4()[:8]
        if len({batch_id[i:i + 2] for i in range(0, 8, 2)}) == 4:
            return batch_id


class DataProvider:
    """Factory for test documents.

    Args:
        num_docs: How many vehicle documents to build.
        seed: Seeds both ``Faker`` and ``random``, making the generated set reproducible.
            Worth passing when what a document contains decides the outcome, as an FTS
            keyword search does.
    """

    def __init__(self, num_docs: int = 100, seed: int | None = None) -> None:
        if seed is not None:
            fake.seed_instance(seed)
            random.seed(seed)

        dealer_uuid = fake.uuid4()

        self._dealer_list: list[dict[str, Any]] = []
        self._dealer_uuid: str = dealer_uuid[:8]
        self._new_dealer_list: list[dict[str, Any]] = []
        self._new_vehicle_list: list[dict[str, Any]] = []
        self._num_docs: int = num_docs
        self._vehicle_list: list[dict[str, Any]] = []
        self._vehicle_uuid: str = _batch_id_with_distinct_view_keys()

        # Build all vehicle documents immediately so get_vehicles() is always safe.
        self._build_vehicles()

    # ------------------------------------------------------------------
    # Key generation
    # ------------------------------------------------------------------

    def generate_keys(self, num_keys: int) -> list[str]:
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        return [f'{batch_uuid}::{i}' for i in range(num_keys)]

    # ------------------------------------------------------------------
    # Document factories: generic types
    # ------------------------------------------------------------------

    def get_array_docs(self, num_docs: int) -> list[dict[str, Any]]:
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs: list[dict[str, Any]] = []
        for i in range(num_docs):
            docs.append(
                {
                    'batch': batch_uuid,
                    'id': f'{batch_uuid}::{i}',
                    'array': [1, 2, 3, 4, 5],
                    'type': 'array',
                }
            )
        return docs

    def get_list_only_docs(self, num_docs: int) -> dict[str, list[Any]]:
        """Return a key to bare-list dict, the shape ``ListCollection`` operations take."""
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs: dict[str, list[Any]] = {}
        for i in range(num_docs):
            key = f'{batch_uuid}::{i}'
            docs[key] = []
        return docs

    def get_bytes_docs(
        self, num_docs: int, start_value: bytes | None = None
    ) -> dict[str, bytes]:
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs: dict[str, bytes] = {}
        for i in range(num_docs):
            # Distinct key infixes so a caller can hold both a valued and an empty batch in
            # one collection without the second overwriting the first.
            if start_value is not None:
                key = f'{batch_uuid}::bytes::{i}'
                docs[key] = start_value
            else:
                key = f'{batch_uuid}::bytes_empty::{i}'
                docs[key] = b''
        return docs

    def get_count_docs(self, num_docs: int) -> list[dict[str, Any]]:
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs: list[dict[str, Any]] = []
        for i in range(num_docs):
            docs.append(
                {
                    'batch': batch_uuid,
                    'id': f'{batch_uuid}::{i}',
                    'count': 100,
                    'type': 'count',
                }
            )
        return docs

    def get_counter_docs(
        self, num_docs: int, start_value: int
    ) -> dict[str, int]:
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs: dict[str, int] = {}
        for i in range(num_docs):
            key = f'{batch_uuid}::counter::{i}'
            docs[key] = start_value
        return docs

    def get_counter_doc_key(self) -> str:
        """Return one key naming a document that does not exist.

        A key and not a document on purpose: what an increment's ``initial`` is for is the
        key nothing has written yet, so seeding one would defeat it.
        """
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        return f'{batch_uuid}::counter_empty'

    def get_simple_docs(self, num_docs: int) -> list[dict[str, Any]]:
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs: list[dict[str, Any]] = []
        for i in range(num_docs):
            docs.append(
                {'batch': batch_uuid, 'id': f'{batch_uuid}::{i}', 'type': 'simple'}
            )
        return docs

    def get_utf8_docs(
        self, num_docs: int, start_value: str | None = None
    ) -> dict[str, str]:
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs: dict[str, str] = {}
        for i in range(num_docs):
            if start_value is not None:
                key = f'{batch_uuid}::utf8::{i}'
                docs[key] = start_value
            else:
                key = f'{batch_uuid}::utf8_empty::{i}'
                docs[key] = ''
        return docs

    # ------------------------------------------------------------------
    # Vehicle / dealership factories
    # ------------------------------------------------------------------

    @property
    def vehicle_batch_id(self) -> str:
        """The batch id every vehicle document carries, seeded and minted alike.

        A property of the provider rather than of what happened to be loaded, so it answers for
        a corpus that went somewhere other than the shared collection.  It is the value
        ``_batch_id_with_distinct_view_keys`` draws, so a caller slicing it into view keys is
        slicing the same string the design document does.
        """
        return self._vehicle_uuid

    def get_vehicles(self) -> list[dict[str, Any]]:
        return list(self._vehicle_list)

    def get_new_vehicle(self) -> dict[str, Any]:
        """Mint one unseeded vehicle document.

        Deliberately *not* FTS-matchable: the keyword injection applies to a fixed fraction of
        a batch, and a batch of one cannot carry a fraction.  A caller that needs a matchable
        document wants get_new_vehicles().
        """
        idx = len(self._vehicle_list) + len(self._new_vehicle_list)
        vehicle = fake.vehicle(self._vehicle_uuid, idx)
        self._new_vehicle_list.append(vehicle)
        return vehicle

    def get_new_vehicles(self, num_docs: int) -> list[dict[str, Any]]:
        """Mint num_docs unseeded vehicle documents, a fixed fraction FTS-matchable.

        The FTS keyword is the whole reason this exists alongside get_new_vehicle().  Documents
        loaded outside the seeded corpus and expected to be findable by a term query have to go
        through the same injection, or they carry a plain faker sentence and match nothing.
        """
        idx = len(self._vehicle_list) + len(self._new_vehicle_list)
        vehicles = [fake.vehicle(self._vehicle_uuid, idx + i) for i in range(num_docs)]
        self._inject_fts_keywords(vehicles)
        self._new_vehicle_list.extend(vehicles)
        return vehicles

    def get_dealerships(self, full_doc: bool = False) -> list[dict[str, Any]]:
        if full_doc:
            return list(self._dealer_list)
        return [{k: v for k, v in d.items() if k != 'sales_reps'} for d in self._dealer_list]

    def get_new_dealership(self, full_doc: bool = False) -> dict[str, Any]:
        idx = len(self._dealer_list) + len(self._new_dealer_list)
        vehicle_ids = [v['id'] for v in self._vehicle_list + self._new_vehicle_list]
        dealer = fake.dealership(self._dealer_uuid, vehicle_ids, idx)
        self._new_dealer_list.append(dealer)
        if full_doc:
            return dealer
        return {k: v for k, v in dealer.items() if k != 'sales_reps'}

    # ------------------------------------------------------------------
    # Internal builders
    # ------------------------------------------------------------------

    def _build_vehicles(self) -> None:
        vehicles: list[dict[str, Any]] = []
        for i in range(self._num_docs):
            vehicles.append(fake.vehicle(self._vehicle_uuid, i))

        self._inject_fts_keywords(vehicles)
        self._vehicle_list = vehicles

    @staticmethod
    def _inject_fts_keywords(docs: list[dict[str, Any]]) -> None:
        """Put ``_FTS_KEYWORD`` into a fixed fraction of ``docs``, in place.

        A fixed fraction rather than a coin flip, so at least one document is always matchable
        and a search assertion can name a row count at all.
        """
        num_fts = max(1, int(len(docs) * _FTS_KEYWORD_FRACTION))
        fts_indices = set(random.sample(range(len(docs)), num_fts))  # nosec

        for i, doc in enumerate(docs):
            if i in fts_indices:
                tokens = doc['description'].split(' ')
                pos = random.randint(0, max(0, len(tokens) - 1))  # nosec
                tokens.insert(pos, _FTS_KEYWORD)
                doc['description'] = ' '.join(tokens)
