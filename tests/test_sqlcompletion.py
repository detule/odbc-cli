from odbcli.completion.sqlcompletion import SqlStatement
import pytest


@pytest.mark.parametrize(
    "before_cursor, expected",
    [
        (" ", (None, None)),
        ("abc", (None, None)),
        ("abc.", (None, "abc")),
        ("abc.def", (None, "abc")),
        ("abc.def.", ("abc", "def")),
        ("abc.def.ghi", ("abc", "def"))
    ],
)
def test_get_identifier_parents(before_cursor, expected):
    stmt = SqlStatement(
        full_text = "SELECT * FROM abc.def.ghi",
        text_before_cursor = before_cursor)

    assert stmt.get_identifier_parents() == expected
