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


from couchbase._libcouchbase import Item as _Item

class Item(_Item):
    def __init__(self, key=None, value=None):
        """
        Construct a new `Item` object.

        :param string key: The key to initialize this item with
        :param object value: The value to initialize this item with

        The `Item` class is a sublcass of a
        :class:`~couchbase.result.ValueResult`.
        Its members are all writeable and accessible from this object.

        .. warning::

            As the item build-in properties (such as ``key``, ``value``,
            ``cas``, etc.)
            are implemented directly in C and are not exposed in the item's
            ``__dict__`` field, you cannot override these fields in a subclass
            to be a ``property`` or some other custom data descriptor.

            To confuse matters even more, if you do implement these properties
            as descriptors, they will be visible from your own code, but *not*
            from the implementation code. You have been warned.

            In short, don't override these properties.

            Here's an example of what you should *not* do::

                class MyItem(Item):
                    # ...
                    @property
                    def key(self):
                        return self._key

                    @key.setter
                    def key(self, newkey):
                        self._key = key

        To use this class with the :class:`couchbase.bucket.Bucket`
        API methods, you must take care to:

        1. Use only the ``*_multi`` methods
        2. Pass one of the :class:`ItemCollection` objects to these methods.
           This will let the API know to enable special handling for
           the :class:`Item` API.

        """

        super(Item, self).__init__()
        self.key = key
        self.value = value

    def as_itcoll(self, **kwargs):
        """
        Convenience method to return an instance of a :class:`ItemCollection`
        containing only this item. This would then be used like so::

            cb.upsert_multi(itm.as_itcoll())

        Or use it with options::

            cb.upsert_multi(itm.as_itcoll(ignore_cas=True))

        :param kwargs: Extra operation-specific options.

        :return: An :class:`ItemCollection` instance
        """
        if not kwargs:
            return ItemSequence([self])
        else:
            return ItemOptionDict({self: kwargs})

class ItemCollection(object):
    """
    The base class for a collection of Items.
    """
    def __len__(self):
        raise NotImplementedError()

    def __iter__(self):
        """
        This iterator is mainly intended for the internal API use; it
        yields a tuple of (item, options).
        In the case of a :class:`ItemSequence` which does not store
        options, the second element is always `None`
        """

    def dict_items(self):
        """
        Iterator which returns a tuple of ``(item, options)``
        for each item in this collection.
        """
        return iter(self)

class ItemOptionDict(ItemCollection):
    def __init__(self, d=None):
        """
        A simple mapping of :class:`Item` objects to optional dictionaries
        of values.

        The keys and values for the options dictionary depends on the command
        being used. See the appropriate command for more options

        :param dict d: A dictionary of item -> option, or None.
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

    def create_and_add(self, key, value=None, cas=0, **options):
        """
        Creates and adds an item.
        :param key: The key to use for the item
        :param value: The value to use for the item
        :param options: Additional operation-specific options
        """
        itm = Item(key, value)
        itm.cas = cas
        return self.add(itm, **options)

    def __iter__(self):
        for p in self._d.items():
            yield p

    def __len__(self):
        return len(self._d)

class ItemSequence(ItemCollection):
    def __init__(self, obj):
        """
        Create a new :class:`ItemSequence` object

        :param seq: A sequence containing the items
        :type seq: An iterable or a single item
        """
        self._seq = [ obj ] if isinstance(obj, Item) else obj
        # Verify this is indeed a sequence
        len(self._seq)

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
