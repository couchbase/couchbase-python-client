# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from couchbase_core.items import Item, ItemSequence
from couchbase_v2.bucket import Bucket
from random import randint

class Player(Item):
    def __init__(self, name, create_structure=False):
        super(Player, self).__init__(name)
        if create_structure:
            self.value = {
                'email': None,
                'score': 0,
                'games': []
            }

    @classmethod
    def create(cls, name, email, cb):
        """
        Create the basic structure of a player
        """
        it = cls(name, create_structure=True)
        it.value['email'] = email

        # In an actual application you'd probably want to use 'add',
        # but since this app might be run multiple times, you don't
        # want to get DocumentExistsException
        cb.upsert_multi(ItemSequence([it]))
        return it

    @classmethod
    def load(cls, name, cb):
        it = Player(name)
        cb.get_multi(ItemSequence([it]))
        return it

    def save(self, cb):
        cb.replace_multi(ItemSequence([self]))

    @property
    def name(self):
        return self.key

    @property
    def score(self):
        return self.value['score']

    @score.setter
    def score(self, value):
        self.value['score'] = value

    @property
    def games(self):
        return self.value['games']

    @property
    def email(self):
        return self.value['email']
    @email.setter
    def email(self, value):
        self.value['email'] = value

cb = Bucket('couchbase://localhost/default')
single_player = Player.create("bob", "bob@bobnob.com", cb)
single_player.score += 100
single_player.save(cb)

# Let's try multiple players
players = ItemSequence([Player(x, create_structure=True)
           for x in ("joe", "jim", "bill", "larry")])

# Save them all
cb.upsert_multi(players)

# Give them all some points
for p, options in players:
    p.score += randint(20, 2000)
    # also set the email?
    if not p.email:
        p.email = "{0}@{0}.notspecified.com".format(p.name)

cb.replace_multi(players)
all_players = ItemSequence([x[0] for x in players] + [single_player])

INDENT = " " * 3
for player in all_players.sequence:
    print "Name:", player.name
    print INDENT , player

    lines = []
    lines.append("email: {0}".format(player.email))
    lines.append("score: {0}".format(player.score))

    for line in lines:
        print INDENT, line

cb.remove_multi(all_players)
cb.endure_multi(all_players, check_removed=True, replicate_to=0,
                persist_to=1)
