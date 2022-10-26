# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import, division, unicode_literals

from jx_sqlite.expressions._utils import SQLang

from jx_base import Facts, Column
from jx_base.models.container import Container as _Container
from jx_mysql.models.namespace import Namespace
# from jx_mysql.query_table import QueryTable
from jx_mysql.models.snowflake import Snowflake
from jx_mysql.mysql import (
    SQL_SELECT,
    SQL_FROM,
    SQL_UPDATE,
    SQL_SET,
    ConcatSQL, sql_insert, sql_create,
)
from jx_mysql.mysql import (
    MySql,
    quote_column,
    sql_eq,
    # sql_create,
    # sql_insert,
    json_type_to_mysql_type,
)
from jx_mysql.utils import UID, GUID, DIGITS_TABLE, ABOUT_TABLE
from mo_dots import concat_field, set_default
from mo_future import first, NEXT
from mo_json import STRING
from mo_kwargs import override
from mo_logs import Log
from mo_threads.lock import locked
from mo_times import Date

_config = None


class Container(_Container):
    @override
    def __init__(
        self,
        db=None,  # EXISTING MySql DATBASE, OR CONFIGURATION FOR MySql DB
        filename=None,  # FILE FOR THE DATABASE (None FOR MEMORY DATABASE)
        kwargs=None,  # See MySql parameters
    ):
        global _config
        if isinstance(db, MySql):
            self.db = db
        else:
            # PASS CALL PARAMETERS TO MySql
            self.db = db = MySql(filename=filename, kwargs=set_default({}, db, kwargs))

        if not _config:
            # REGISTER mysql AS THE DEFAULT CONTAINER TYPE
            from jx_base.models.container import config as _config

            if not _config.default:
                _config.default = {"type": "mysql", "settings": {"db": db}}

        self.setup()
        self.namespace = Namespace(container=self)
        self.about = QueryTable("meta.about", self)
        self.next_uid = self._gen_ids()  # A DELIGHTFUL SOURCE OF UNIQUE INTEGERS

    def _gen_ids(self):
        def output():
            while True:
                with self.db.transaction() as t:
                    top_id = first(first(
                        t
                        .query(ConcatSQL(
                            SQL_SELECT,
                            quote_column("next_id"),
                            SQL_FROM,
                            quote_column(ABOUT_TABLE),
                        ))
                        .data
                    ))
                    max_id = top_id + 1000
                    t.execute(ConcatSQL(
                        SQL_UPDATE,
                        quote_column(ABOUT_TABLE),
                        SQL_SET,
                        sql_eq(next_id=max_id),
                    ))
                while top_id < max_id:
                    yield top_id
                    top_id += 1

        return locked(NEXT(output()))

    def setup(self):
        if not self.db.about(ABOUT_TABLE):
            with self.db.transaction() as t:
                t.execute(sql_create(
                    ABOUT_TABLE, {"version": "TEXT", "next_id": "INTEGER"}
                ))
                t.execute(sql_insert(ABOUT_TABLE, {"version": "1.0", "next_id": 1000}))
                t.execute(sql_create(DIGITS_TABLE, {"value": "INTEGER"}))
                t.execute(sql_insert(DIGITS_TABLE, [{"value": i} for i in range(10)]))

    def create_or_replace_facts(self, fact_name, uid=UID):
        """
        MAKE NEW TABLE, REPLACE OLD ONE IF EXISTS
        :param fact_name:  NAME FOR THE CENTRAL INDEX
        :param uid: name, or list of names, for the GUID
        :return: Facts
        """
        self.remove_facts(fact_name)
        self.namespace.columns._snowflakes[fact_name] = ["."]

        if uid != UID:
            Log.error("do not know how to handle yet")

        command = sql_create(
            fact_name, {UID: "INTEGER PRIMARY KEY", GUID: "TEXT"}, unique=UID
        )

        with self.db.transaction() as t:
            t.execute(command)

        snowflake = Snowflake(fact_name, self.ns)
        return Facts(self, snowflake)

    def remove_facts(self, fact_name):
        paths = self.namespace.columns._snowflakes[fact_name]
        if paths:
            with self.db.transaction() as t:
                for p in paths:
                    full_name = concat_field(fact_name, p[0])
                    t.execute("DROP TABLE " + quote_column(full_name))
            self.namespace.columns.remove_table(fact_name)

    def get_or_create_facts(self, fact_name, uid=UID):
        """
        FIND TABLE BY NAME, OR CREATE IT IF IT DOES NOT EXIST
        :param fact_name:  NAME FOR THE CENTRAL INDEX
        :param uid: name, or list of names, for the GUID
        :return: Facts
        """
        about = self.db.about(fact_name)
        if not about:
            if uid != UID:
                Log.error("do not know how to handle yet")

            self.namespace.columns._snowflakes[fact_name] = ["."]
            self.namespace.columns.add(Column(
                name="_id",
                es_column="_id",
                es_index=fact_name,
                es_type=json_type_to_mysql_type[STRING],
                json_type=STRING,
                nested_path=["."],
                multi=1,
                last_updated=Date.now(),
            ))
            command = sql_create(
                fact_name, {UID: "INTEGER PRIMARY KEY", GUID: "TEXT"}, unique=UID
            )

            with self.db.transaction() as t:
                t.execute(command)

        return QueryTable(fact_name, self)

    def get_table(self, table_name):
        return QueryTable(table_name, self)

    @property
    def language(self):
        return SQLang
