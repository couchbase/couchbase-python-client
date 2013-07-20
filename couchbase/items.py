#
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

# This module contains various collections to be used with items. These provide
# various means by which multiple operations can have their options customized

# Each of these collections yields an iterator consisting of a 3-tuple:
# (item, {options})
# The CAS, Format, and Value are all expected to be inside the Item itself;


from couchbase._libcouchbase import Item

class ItemCollection(object):
    """
    The base class for a collection of Items.
    """
    def __len__(self):
        raise NotImplementedError()

class ItemOptionDict(ItemCollection):
    def __init__(self, d=None):
        """
        A simple mapping of :class:`Item` objects to optional dictionaries
        of values.

        The keys and values for the options dictionary depends on the command
        being used. See the appropriate command for more options

        :param dict d: A dictionary of item -> option, or None
        """
        if d is None:
            d = {}
        self._d = d

    @property
    def dict(self):
        """
        Return the actual dict object
        """
        return self._d

    def add(self, itm, **options):
        """
        Convenience method to add an item together with a series of options.

        :param itm: The item to add
        :param options: keyword arguments which will be placed in the item's 
            option entry.
        
        If the item already exists, it (and its options) will be overidden. Use
        :attr:`dict` instead to update options

        """
        if not options:
            options = None
        self._d[itm] = options

    def __iter__(self):
        for p in self._d.items():
            yield p

    def __len__(self):
        return len(self._d)

class ItemSequence(ItemCollection):
    def __init__(self, seq):
        """
        Create a new :class:`ItemSequence` object

        :param iterable seq: A sequence containing the items
        """
        self._seq = seq

    @property
    def sequence(self):
        """
        The actual sequence object passed in
        """
        return self._seq

    def __len__(self):
        return len(self._seq)

    def __iter__(self):
        for e in self._seq:
            yield (e, None)
