# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import, division, unicode_literals

from typing import Set, Tuple

from jx_base import Column

from jx_base.queries import get_property_name
from jx_mysql.utils import GUID, untyped_column, untype_field
from mo_dots import concat_field, relative_field, set_default, startswith_field
from mo_json import EXISTS, OBJECT, STRUCT
from mo_logs import Log


class Schema(object):
    """
    A Schema MAPS ALL COLUMNS IN SNOWFLAKE FROM THE PERSPECTIVE OF A SINGLE TABLE (a nested_path)
    """

    def __init__(self, nested_path, snowflake):
        if nested_path[-1] != ".":
            Log.error(
                "Expecting full nested path so we can track the tables, and deal with"
                " abiguity in the event the names are not typed"
            )
        self.path = concat_field(snowflake.fact_name, nested_path[0])
        self.nested_path = nested_path
        self.snowflake = snowflake

    def __getitem__(self, item):
        output = self.snowflake.namespace.columns.find(self.path, item)
        return output

    def get_table(self, relative_path):
        abs_path = concat_field(self.nested_path[0], relative_path)
        return self.snowflake.get_table(abs_path)

    def get_column_name(self, column):
        """
        RETURN THE COLUMN NAME, FROM THE PERSPECTIVE OF THIS SCHEMA
        :param column:
        :return: NAME OF column
        """
        relative_name = relative_field(column.name, self.nested_path[0])
        return get_property_name(relative_name)

    @property
    def namespace(self):
        return self.snowflake.namespace

    @property
    def container(self):
        return self.snowflake.container

    def keys(self):
        """
        :return: ALL DYNAMIC TYPED COLUMN NAMES
        """
        return set(c.name for c in self.columns)

    @property
    def columns(self):
        return self.snowflake.namespace.columns.find(self.snowflake.fact_name)

    def column(self, prefix):
        full_name = untyped_column(concat_field(self.nested_path, prefix))
        return set(
            c
            for c in self.snowflake.namespace.columns.find(self.snowflake.fact_name)
            for k, t in [untyped_column(c.name)]
            if k == full_name and k != GUID
            if c.jx_type not in [OBJECT, EXISTS]
        )

    def leaves(self, prefix) -> Set[Tuple[str, Column]]:
        for np in self.nested_path:
            full_name = concat_field(np, prefix)
            candidates = self.columns
            output = set(
                (untype_field(relative_field(k, full_name))[0], c)
                for c in candidates
                if c.jx_type not in [OBJECT, EXISTS]
                # if startswith_field(np, c.nested_path[0])
                for k in [c.name, c.es_column]
                if startswith_field(k, full_name) and k != GUID or k == full_name
            )
            if output:
                return output
        return set()

    def map_to_sql(self, var=""):
        """
        RETURN A MAP FROM THE RELATIVE AND ABSOLUTE NAME SPACE TO COLUMNS
        """
        origin = self.nested_path[0]
        if startswith_field(var, origin) and origin != var:
            var = relative_field(var, origin)
        fact_dict = {}
        origin_dict = {}
        for k, cs in self.namespace.items():
            for c in cs:
                if c.jx_type in STRUCT:
                    continue

                if startswith_field(get_property_name(k), var):
                    origin_dict.setdefault(c.names[origin], []).append(c)

                    if origin != c.nested_path[0]:
                        fact_dict.setdefault(c.name, []).append(c)
                elif origin == var:
                    origin_dict.setdefault(
                        concat_field(var, c.names[origin]), []
                    ).append(c)

                    if origin != c.nested_path[0]:
                        fact_dict.setdefault(concat_field(var, c.name), []).append(c)

        return set_default(origin_dict, fact_dict)
