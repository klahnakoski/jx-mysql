# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import, division, unicode_literals

from copy import copy

import jx_base
from jx_base import Facts
from jx_mysql.meta_columns import ColumnList
from jx_mysql.models.schema import Schema
from jx_mysql.models.snowflake import Snowflake


class Namespace(jx_base.Namespace):
    """
    MANAGE MYSQL DATABASE
    """

    def __init__(self, container):
        self.container = container
        self.columns = ColumnList(container.db)

    def __copy__(self):
        output = object.__new__(Namespace)
        output.db = None
        output.columns = copy(self.columns)
        return output

    def get_facts(self, fact_name):
        snowflake = Snowflake(fact_name, self)
        return Facts(self, snowflake)

    def get_schema(self, fact_name):
        return Schema("..", Snowflake(fact_name, self))

    def get_snowflake(self, fact_name):
        return Snowflake(fact_name, self)

    def add_column_to_schema(self, column):
        self.columns.add(column)
