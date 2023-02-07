import datetime
import random
from typing import Any, Dict

from faker import Faker
from faker.providers import BaseProvider
from faker_vehicle import VehicleProvider

fake = Faker('en_US')

# first, import a similar Provider or use the default one

fake.add_provider(VehicleProvider)


class DealershipProvider(BaseProvider):
    def dealership(self, dealer_uuid, vehicle_ids, idx) -> Dict[str, Any]:
        dealer = {
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
            }
        }

        today = datetime.datetime.now()
        prev_year = (today - datetime.timedelta(days=365))
        num_reps = fake.random_int(min=2, max=5)
        reps = []
        for _ in range(num_reps):
            rep = {
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'phone': fake.phone_number(),
                'email': fake.company_email(),
                'rating': fake.random_int(min=1, max=5),
                'reviews': [],
            }
            num_reviews = fake.random_int(min=2, max=5)
            for _ in range(num_reviews):
                rep['reviews'].append({
                    'date': fake.date_time_between(prev_year).isoformat(),
                    'comment': fake.sentence(nb_words=10),
                    'categories': {
                        'service': fake.random_int(min=1, max=5),
                        'friendliness': fake.random_int(min=1, max=5),
                        'honesty': fake.random_int(min=1, max=5),
                        'knowledgeable': fake.random_int(min=1, max=5),
                        'responsive': fake.random_int(min=1, max=5),
                    }
                })
            reps.append(rep)

        dealer['sales_reps'] = reps

        num_vehicles = fake.random_int(min=5, max=10)
        vehicles = random.choices(vehicle_ids, k=num_vehicles)  # nosec

        prev_30_days = (today - datetime.timedelta(days=30))

        dealer['inventory'] = {
            'num_vehicles': num_vehicles,
            'last_updated': fake.date_time_between(prev_30_days).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'vehicles': vehicles,
        }
        dealer['type'] = 'dealership'
        return dealer

    def vehicle(self, vehicle_uuid, idx) -> Dict[str, Any]:
        vehicle = {'batch': vehicle_uuid, 'id': f'{vehicle_uuid}::{idx}'}
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

        desc = fake.sentence(nb_words=10)
        # add a key word for FTS operations to some of the docs
        if fake.random_int(min=0, max=1) == 1:
            tokens = desc.split(' ')
            pos = fake.random_int(min=0, max=7)
            tokens.insert(pos, 'great auto deal')
            desc = ' '.join(tokens)
        vehicle['description'] = desc
        vehicle['rating'] = fake.random_int(min=1, max=10)
        now = datetime.datetime.now()
        today = datetime.datetime(year=now.year, month=now.month, day=now.day)
        prev_year = (today - datetime.timedelta(days=365))
        vehicle['last_updated'] = fake.date_time_between(prev_year).strftime('%Y-%m-%dT%H:%M:%SZ')
        vehicle['type'] = 'vehicle'

        return vehicle

    def _get_geo(self):
        lat, ln, loc, country_code, tz = fake.local_latlng()
        return {
            'lat': lat,
            'ln': ln,
            'accuracy': random.choice(['ROOFTOP', 'APPROXIMATE', 'RANGE_INTERPOLATED']),  # nosec
            'location': {
                'loc': loc,
                'country_code': country_code,
                'tz': tz,
            }
        }


fake.add_provider(DealershipProvider)


class DataProvider:
    def __init__(self, num_docs=100):
        dealer_uuid = fake.uuid4()
        vehicle_uuid = fake.uuid4()

        self._dealer_list = []
        self._dealer_uuid = dealer_uuid[:8]
        self._new_dealer_list = []
        self._new_vehicle_list = []
        self._num_docs = num_docs
        self._vehicle_list = []
        self._vehicle_uuid = vehicle_uuid[:8]

    def build_docs(self):
        self._build_vehicles()
        # future?
        # self._build_dealerships()

    def generate_keys(self, num_keys):
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        return [f'{batch_uuid}::{i}' for i in range(num_keys)]

    def get_array_docs(self, num_docs):
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs = []
        for i in range(num_docs):
            docs.append({
                'batch': batch_uuid,
                'id': f'{batch_uuid}::{i}',
                'array': [1, 2, 3, 4, 5],
                'type': 'array'
            })

        return docs

    def get_array_only_docs(self, num_docs):
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs = {}
        for i in range(num_docs):
            key = f'{batch_uuid}::{i}'
            docs[key] = []

        return docs

    def get_bytes_docs(self, num_docs, start_value=None):
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs = {}
        for i in range(num_docs):
            if start_value:
                key = f'{batch_uuid}::bytes::{i}'
                docs[key] = start_value
            else:
                key = f'{batch_uuid}::bytes_empty::{i}'
                docs[key] = b''

        return docs

    def get_count_docs(self, num_docs):
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs = []
        for i in range(num_docs):
            docs.append({
                'batch': batch_uuid,
                'id': f'{batch_uuid}::{i}',
                'count': 100,
                'type': 'count'
            })

        return docs

    def get_counter_docs(self, num_docs, start_value=None):
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        if start_value is None:
            return f'{batch_uuid}::counter_empty'
        docs = {}
        for i in range(num_docs):
            key = f'{batch_uuid}::counter::{i}'
            docs[key] = start_value

        return docs

    def get_dealerships(self, full_doc=False):
        if full_doc is True:
            return self._dealer_list

        return [{k: v for k, v in d.items() if k != 'sales_reps'} for d in self._dealer_list]

    def get_new_dealership(self, full_doc=False):
        idx = len(self._dealer_list) + len(self._new_dealer_list)
        vehicle_ids = list(map(lambda v: v['id'], self._vehicle_list + self._new_vehicle_list))
        dealer = fake.dealership(self._dealer_uuid, vehicle_ids, idx)
        self._new_dealer_list.append(dealer)
        if full_doc is True:
            return dealer

        return {k: v for k, v in dealer.items() if k != 'sales_reps'}

    def get_new_vehicle(self):
        idx = len(self._vehicle_list) + len(self._new_vehicle_list)
        vehicle = fake.vehicle(self._vehicle_uuid, idx)
        self._new_vehicle_list.append(vehicle)
        return vehicle

    def get_simple_docs(self, num_docs):
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs = []
        for i in range(num_docs):
            docs.append({'batch': batch_uuid, 'id': f'{batch_uuid}::{i}', 'type': 'simple'})

        return docs

    def get_utf8_docs(self, num_docs, start_value=None):
        uuid = fake.uuid4()
        batch_uuid = uuid[:8]
        docs = {}
        for i in range(num_docs):
            if start_value:
                key = f'{batch_uuid}::utf8::{i}'
                docs[key] = start_value
            else:
                key = f'{batch_uuid}::utf8_empty::{i}'
                docs[key] = ''

        return docs

    def get_vehicles(self):
        return self._vehicle_list

    def _build_vehicles(self):
        for i in range(self._num_docs):
            self._vehicle_list.append(fake.vehicle(self._vehicle_uuid, i))

    def _build_dealerships(self):
        vehicle_ids = list(map(lambda v: v['id'], self._vehicle_list))
        for i in range(self._num_docs):
            self._dealer_list.append(fake.dealership(self._dealer_uuid, vehicle_ids, i))
