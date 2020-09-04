from odbcli.completion.parseutils.tables import extract_table_identifiers, TableReference
from sqlparse import parse
import pytest

def test_get_table_identifiers():
    qry = "SELECT a.col1, b.col2 " \
           "FROM abc.def.ghi AS a " \
           "INNER JOIN jkl.mno.pqr AS b ON a.id_one = b.id_two"
    parsed = parse(qry)[0]
    res = list(extract_table_identifiers(parsed))
    expected = [
        TableReference(None, "a", "col1", None, False),
        TableReference(None, "b", "col2", None, False),
        TableReference("abc", "def", "ghi", "a", False),
        TableReference("jkl", "mno", "pqr", "b", False),
    ]

    assert res == expected
