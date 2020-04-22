# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_dots import set_default, wrap
from mo_logs import Log, startup, constants
from mo_sql import SQL
from mo_times.timer import Timer

from jx_mysql.mysql import MySQL, execute_file
from jx_mysql.mysql_snowflake_extractor import MySqlSnowflakeExtractor
from mo_testing.fuzzytestcase import FuzzyTestCase

settings = startup.read_settings(filename="tests/resources/config/test.json")
constants.set(settings.constants)


class TestExtract(FuzzyTestCase):
    @classmethod
    def setUpClass(cls):
        Log.start(settings.debug)
        with Timer("setup database"):
            try:
                with MySQL(schema=None, kwargs=settings.database) as db:
                    db.query("drop database testing")
            except Exception as e:
                if "Can't drop database " in e:
                    pass
                else:
                    Log.warning("problem removing db", cause=e)
            execute_file(
                "tests/resources/database.sql", schema=None, kwargs=settings.database
            )

    def setUp(self):
        pass

    def run_compare(self, config, id_sql, expected):
        db = MySQL(**config.snowflake.database)
        extractor = MySqlSnowflakeExtractor(kwargs=config.snowflake)

        sql = extractor.get_sql(SQL(id_sql))

        result = []
        with db.transaction():
            cursor = db.query(sql, stream=True, row_tuples=True)
            cursor = list(cursor)
            extractor.construct_docs(cursor, result.append, False)

        self.assertEqual(result, expected, "expecting identical")
        self.assertEqual(expected, result, "expecting identical")

    def test_simple(self):
        self.run_compare(config_template, "SELECT 22 AS id", expected_results["simple"])

    def test_deep_and_slim(self):
        self.run_compare(
            set_default(
                {
                    "snowflake": {
                        "show_foreign_keys": False,
                        "reference_only": ["inner1.value", "inner2.value"],
                    }
                },
                config_template,
            ),
            "SELECT 30 AS id",
            expected_results["deep_and_slim"]
        )

    def test_complex(self):
        self.run_compare(
            config_template, "SELECT 10 AS id", expected_results["complex"]
        )

    def test_inline(self):
        self.run_compare(
            set_default(
                {"snowflake": {"reference_only": ["inner1.value", "inner2.value"]}},
                config_template,
            ),
            "SELECT 10 AS id",
            expected_results["inline"],
        )

    def test_lean(self):
        self.run_compare(
            set_default({"snowflake": {"show_foreign_keys": False}}, config_template),
            "SELECT 10 AS id",
            expected_results["lean"],
        )

    def test_lean_inline(self):
        self.run_compare(
            set_default(
                {
                    "snowflake": {
                        "show_foreign_keys": False,
                        "reference_only": ["inner1.value", "inner2.value"],
                    }
                },
                config_template,
            ),
            "SELECT 10 AS id",
            expected_results["lean_inline"],
        )

    def test_lean_inline_all(self):
        self.run_compare(
            set_default(
                {
                    "extract": {"ids": "select * from fact_table"},
                    "snowflake": {
                        "show_foreign_keys": False,
                        "reference_only": ["inner1.value", "inner2.value"],
                    },
                },
                config_template,
            ),
            "\nUNION ALL\n".join(
                "SELECT " + str(v) + " AS id"
                for v in [10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22]
            ),
            expected_results["lean_inline_all"],
        )


filename = "tests/output/test_output.json"

config_template = wrap(
    {
        "extract": {
            "last": "tests/output/test_run.json",
            "field": "id",
            "type": "number",
            "start": 0,
            "batch": 100,
        },
        "destination": filename,
        "snowflake": {
            "fact_table": "fact_table",
            "show_foreign_keys": True,
            "null_values": ["-", "unknown", ""],
            "add_relations": [],
            "include": [],
            "exclude": [],
            "reference_only": ["inner1", "inner2"],
            "database": settings.database,
        },
        "debug": {"trace": True},
    }
)

expected_results = {
    "simple": [{"id": 22, "name": "L"}],
    "deep_and_slim": [{"id":30, "nested1": [{"nested2": [{"about": "c", "minutia": 8.4},]}],}],
    "lean_inline": [
        {
            "about": "a",
            "id": 10,
            "name": "A",
            "nested1": [
                {
                    "about": 0,
                    "description": "aaa",
                    "nested2": [
                        {"about": "a", "minutia": 3.1415926539},
                        {"about": "b", "minutia": 4},
                        {"about": "c", "minutia": 5.1},
                    ],
                }
            ],
        }
    ],
    "lean": [
        {
            "about": {"value": "a", "time": {"value": 0}},
            "id": 10,
            "name": "A",
            "nested1": [
                {
                    "about": {"value": 0},
                    "description": "aaa",
                    "nested2": [
                        {
                            "about": {"value": "a", "time": {"value": 0}},
                            "minutia": 3.1415926539,
                        },
                        {"about": {"value": "b"}, "minutia": 4},
                        {"about": {"value": "c"}, "minutia": 5.1},
                    ],
                }
            ],
        }
    ],
    "complex": [
        {
            "about": {"id": 1, "time": {"id": -1, "value": 0}, "value": "a"},
            "id": 10,
            "name": "A",
            "nested1": [
                {
                    "about": {"id": -1, "value": 0},
                    "description": "aaa",
                    "id": 100,
                    "nested2": [
                        {
                            "about": {
                                "id": 1,
                                "time": {"id": -1, "value": 0},
                                "value": "a",
                            },
                            "id": 1000,
                            "minutia": 3.1415926539,
                            "ref": 100,
                        },
                        {
                            "about": {"id": 2, "time": {"id": -2}, "value": "b"},
                            "id": 1001,
                            "minutia": 4,
                            "ref": 100,
                        },
                        {
                            "about": {"id": 3, "value": "c"},
                            "id": 1002,
                            "minutia": 5.1,
                            "ref": 100,
                        },
                    ],
                    "ref": 10,
                }
            ],
        }
    ],
    "inline": [
        {
            "about": {"id": 1, "value": "a"},
            "id": 10,
            "name": "A",
            "nested1": [
                {
                    "about": {"id": -1, "value": 0},
                    "ref": 10,
                    "description": "aaa",
                    "nested2": [
                        {
                            "about": {"id": 1, "value": "a"},
                            "ref": 100,
                            "id": 1000,
                            "minutia": 3.1415926539,
                        },
                        {
                            "about": {"id": 2, "value": "b"},
                            "ref": 100,
                            "id": 1001,
                            "minutia": 4,
                        },
                        {
                            "about": {"id": 3, "value": "c"},
                            "ref": 100,
                            "id": 1002,
                            "minutia": 5.1,
                        },
                    ],
                    "id": 100,
                }
            ],
        }
    ],
    "lean_inline_all": [
        {
            "nested1": [
                {
                    "about": 0,
                    "description": "aaa",
                    "nested2": [
                        {"about": "a", "minutia": 3.1415926539},
                        {"about": "b", "minutia": 4},
                        {"about": "c", "minutia": 5.1},
                    ],
                }
            ],
            "about": "a",
            "id": 10,
            "name": "A",
        },
        {
            "nested1": [
                {"description": "bbb", "nested2": [{"about": "a", "minutia": 6.2}]}
            ],
            "about": "b",
            "id": 11,
            "name": "B",
        },
        {
            "nested1": [
                {"description": "ccc", "nested2": [{"about": "c", "minutia": 7.3}]}
            ],
            "about": "c",
            "id": 12,
            "name": "C",
        },
        {"nested1": [{"about": 0, "description": "ddd"}], "id": 13, "name": "D",},
        {
            "nested1": [
                {"about": 0, "description": "eee"},
                {"about": 0, "description": "fff"},
            ],
            "about": "a",
            "id": 15,
            "name": "E",
        },
        {
            "nested1": [{"description": "ggg"}, {"description": "hhh"}],
            "about": "b",
            "id": 16,
            "name": "F",
        },
        {
            "nested1": [{"description": "iii"}, {"description": "jjj"}],
            "about": "c",
            "id": 17,
            "name": "G",
        },
        {
            "nested1": [{"description": "kkk"}, {"description": "lll"}],
            "id": 18,
            "name": "H",
        },
        {"about": "a", "id": 19, "name": "I"},
        {"about": "b", "id": 20, "name": "J"},
        {"about": "c", "id": 21, "name": "K"},
        {"id": 22, "name": "L"},
    ],
}