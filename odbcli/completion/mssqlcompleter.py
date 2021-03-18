# pylint: disable=too-many-arguments
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-locals
# pylint: disable=too-many-public-methods
# pylint: disable=too-many-statements

from __future__ import print_function, unicode_literals
import logging
import re
from typing import Callable
from itertools import count, chain
import operator
from collections import namedtuple, defaultdict, OrderedDict
from prompt_toolkit.completion import Completer, Completion, PathCompleter
from prompt_toolkit.document import Document
from ..conn import sqlConnection
from .sqlcompletion import (Blank, FromClauseItem, suggest_type, Special, NamedQuery,
                                             Database, Schema, Table, Function, Column, View,
                                             Keyword, Datatype, Alias, Path, JoinCondition, Join)
from .parseutils.meta import ColumnMetadata, ForeignKey
from .parseutils.utils import last_word
from .parseutils.tables import TableReference
from .mssqlliterals.main import get_literals
from .prioritization import PrevalenceCounter

Match = namedtuple('Match', ['completion', 'priority'])

_SchemaObject = namedtuple('SchemaObject', 'name catalog schema meta')


def SchemaObject(name, catalog=None, schema=None, meta=None):
    return _SchemaObject(name, catalog, schema, meta)


_Candidate = namedtuple(
    'Candidate', 'completion prio meta synonyms prio2 display'
)


def Candidate(
        completion, prio=None, meta=None, synonyms=None, prio2=None,
        display=None
):
    return _Candidate(
        completion, prio, meta, synonyms or [completion], prio2,
        display or completion
    )


# Used to strip trailing '::some_type' from default-value expressions
arg_default_type_strip_regex = re.compile(r'::[\w\.]+(\[\])?$')


def normalize_ref(ref):
    return ref if ref[0] == '"' else '"' + ref.lower() + '"'


def generate_alias(tbl):
    """ Generate a table alias, consisting of all upper-case letters in
    the table name, or, if there are no upper-case letters, the first letter +
    all letters preceded by _
    param tbl - unescaped name of the table to alias
    """
    return ''.join([l for l in tbl if l.isupper()] or
                   [l for l, prev in zip(tbl, '_' + tbl) if prev == '_' and l != '_'])


class MssqlCompleter(Completer):
    # keywords_tree: A dict mapping keywords to well known following keywords.
    # e.g. 'CREATE': ['TABLE', 'USER', ...],
    keywords_tree = get_literals('keywords', type_=dict)
    keywords = tuple(set(chain(keywords_tree.keys(), *keywords_tree.values())))
    functions = get_literals('functions')
    datatypes = get_literals('datatypes')
    reserved_words = set(get_literals('reserved'))

    def __init__(
            self,
            get_conn: Callable[[], sqlConnection],
            smart_completion=True,
            settings=None):
        super(MssqlCompleter, self).__init__()
        self.smart_completion = smart_completion
        self._get_conn = get_conn
        self.prioritizer = PrevalenceCounter()
        settings = settings or {}
        self.signature_arg_style = settings.get(
            'signature_arg_style', '{arg_name} {arg_type}'
        )
        self.call_arg_style = settings.get(
            'call_arg_style', '{arg_name: <{max_arg_len}} := {arg_default}'
        )
        self.call_arg_display_style = settings.get(
            'call_arg_display_style', '{arg_name}'
        )
        self.call_arg_oneliner_max = settings.get('call_arg_oneliner_max', 2)
        self.search_path_filter = settings.get('search_path_filter')
        self.generate_aliases = settings.get('generate_aliases')
        self.casing_file = settings.get('casing_file')
        self.insert_col_skip_patterns = [
            re.compile(pattern) for pattern in settings.get(
                'insert_col_skip_patterns',
                [r'^now\(\)$', r'^nextval\(']
            )
        ]
        self.generate_casing_file = settings.get('generate_casing_file')
        self.qualify_columns = settings.get(
            'qualify_columns', 'if_more_than_one_table')
        self.asterisk_column_order = settings.get(
            'asterisk_column_order', 'table_order')

        keyword_casing = settings.get('keyword_casing', 'upper').lower()
        if keyword_casing not in ('upper', 'lower', 'auto'):
            keyword_casing = 'upper'
        self.keyword_casing = keyword_casing
        self.name_pattern = re.compile(r"^[_a-z][_a-z0-9\$]*$")

        self.databases = []
        self.search_path = []
        self.casing = {}

        # OG: Unclear what the roll of all_completions is
        # self.all_completions = set(self.keywords + self.functions)

        # initialize attributes to be set later
        self._arg_list_cache = None
        self.special_commands = None
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Completer instantiated")

    @property
    def active_conn(self) -> sqlConnection:
        return self._get_conn()

    def escape_name(self, name):
        if self.active_conn is not None:
            name = self.active_conn.escape_name(name)

        return name

    def escape_schema(self, name):
        return u"'{}'".format(self.unescape_name(name))

    def unescape_name(self, name):
        """ Unquote a string."""
        if self.active_conn is not None:
            name = self.active_conn.unescape_name(name)

        return name

    def escape_names(self, names):
        if self.active_conn is not None:
            names = self.active_conn.escape_names(names)

        return names

    def extend_database_names(self, databases):
        databases = self.escape_names(databases)
        self.databases.extend(databases)

    def extend_keywords(self, additional_keywords):
        self.keywords = self.keywords + additional_keywords
        # OG: Unclear what the roll of all_completions is
        # self.all_completions.update(additional_keywords)

    def extend_casing(self, words):
        """ extend casing data

        :return:
        """
        # casing should be a dict {lowercasename:PreferredCasingName}
        self.casing = dict((word.lower(), word) for word in words)

    def extend_functions(self, func_data):
        """ OG: Currently not used
        """

        # func_data is a list of function metadata namedtuples

        # dbmetadata['schema_name']['functions']['function_name'] should return
        # the function metadata namedtuple for the corresponding function
        conn = self.active_conn
        metadata = conn.dbmetadata.data
        submeta = metadata['function']

        for f in func_data:
            schema, func = self.escape_names([f.schema_name, f.func_name])

            if func in submeta[schema]:
                submeta[schema][func].append(f)
            else:
                submeta[schema][func] = [f]

            # OG: Unclear what the roll of all_completions is
            # self.all_completions.add(func)

        self._refresh_arg_list_cache()

    def _refresh_arg_list_cache(self):
        """ OG: Currently not used
        """
        # We keep a cache of {function_usage:{function_metadata: function_arg_list_string}}
        # This is used when suggesting functions, to avoid the latency that would result
        # if we'd recalculate the arg lists each time we suggest functions (in
        # large DBs)
        conn = self.active_conn
        metadata = conn.dbmetadata.data
        self._arg_list_cache = {
            usage: {
                meta: self._arg_list(meta, usage)
                for sch, funcs in metadata['function'].items()
                for func, metas in funcs.items()
                for meta in metas
            }
            for usage in ('call', 'call_display', 'signature')
        }

    def extend_foreignkeys(self, fk_data):
        """ OG: Currently not used
        """

        # fk_data is a list of ForeignKey namedtuples, with fields
        # parentschema, childschema, parenttable, childtable,
        # parentcolumns, childcolumns

        # These are added as a list of ForeignKey namedtuples to the
        # ColumnMetadata namedtuple for both the child and parent
        # OG: This needs catalog facelift
        conn = self.active_conn
        metadata = conn.dbmetadata.data
        submeta = metadata['table']

        for fk in fk_data:
            e = self.escape_names
            parentschema, childschema = e([fk.parentschema, fk.childschema])
            parenttable, childtable = e([fk.parenttable, fk.childtable])
            childcol, parcol = e([fk.childcolumn, fk.parentcolumn])
            childcolmeta = submeta[childschema][childtable][childcol]
            parcolmeta = submeta[parentschema][parenttable][parcol]
            fk = ForeignKey(parentschema, parenttable, parcol,
                            childschema, childtable, childcol)
            childcolmeta.foreignkeys.append((fk))
            parcolmeta.foreignkeys.append((fk))

    def extend_datatypes(self, type_data):
        """ OG: Currently not used
        """

        # dbmetadata['datatypes'][schema_name][type_name] should store type
        # metadata, such as composite type field names. Currently, we're not
        # storing any metadata beyond typename, so just store None
        conn = self.active_conn
        metadata = conn.dbmetadata.data

        for t in type_data:
            schema, type_name = self.escape_names(t)
            metadata["datatype"][schema][type_name] = None
            # OG: Unclear what the roll of all_completions is
            # self.all_completions.add(type_name)

    def extend_query_history(self, text, is_init=False):
        if is_init:
            # During completer initialization, only load keyword preferences,
            # not names
            self.prioritizer.update_keywords(text)
        else:
            self.prioritizer.update(text)

    def set_search_path(self, search_path):
        self.search_path = self.escape_names(search_path)

    def reset_completions(self):
        # databases at this point is not used
        self.databases = []
        # special_commands at this point is not used
        self.special_commands = []
        # search_path at this point is not used
        self.search_path = []
        conn = self.active_conn
        conn.dbmetadata.reset_metadata()
        # OG: Unclear what the roll of all_completions is
        #self.all_completions = set(self.keywords + self.functions)

    def find_matches(self, text, collection, mode='fuzzy', meta=None):
        """Find completion matches for the given text.

        Given the user's input text and a collection of available
        completions, find completions matching the last word of the
        text.

        `collection` can be either a list of strings or a list of Candidate
        namedtuples.
        `mode` can be either 'fuzzy', or 'strict'
            'fuzzy': fuzzy matching, ties broken by name prevalance
            `keyword`: start only matching, ties broken by keyword prevalance

        yields prompt_toolkit Completion instances for any matches found
        in the collection of available completions.

        """
        if not collection:
            return []
        prio_order = [
            'keyword', 'function', 'view', 'table', 'datatype', 'database',
            'schema', 'column', 'table alias', 'join', 'name join', 'fk join'
        ]
        type_priority = prio_order.index(meta) if meta in prio_order else -1
        text = last_word(text, include='most_punctuations').lower()
        text_len = len(text)

        if text and text[0] == '"':
            # text starts with double quote; user is manually escaping a name
            # Match on everything that follows the double-quote. Note that
            # text_len is calculated before removing the quote, so the
            # Completion.position value is correct
            text = text[1:]

        if mode == 'fuzzy':
            fuzzy = True
            priority_func = self.prioritizer.name_count
        else:
            fuzzy = False
            priority_func = self.prioritizer.keyword_count

        # Construct a `_match` function for either fuzzy or non-fuzzy matching
        # The match function returns a 2-tuple used for sorting the matches,
        # or None if the item doesn't match
        # Note: higher priority values mean more important, so use negative
        # signs to flip the direction of the tuple
        if fuzzy:
            regex = '.*?'.join(map(re.escape, text))
            pat = re.compile('(%s)' % regex)

            def _match(item):
                match_item = None
                if item.lower()[:len(text) + 1] in (text, text + ' '):
                    # Exact match of first word in suggestion
                    # This is to get exact alias matches to the top
                    # E.g. for input `e`, 'Entries E' should be on top
                    # (before e.g. `EndUsers EU`)
                    match_item = float('Infinity'), -1
                r = pat.search(self.unescape_name(item.lower()))
                if r:
                    match_item = -len(r.group()), -r.start()
                return match_item
        else:
            match_end_limit = len(text)

            def _match(item):
                match_item = None
                match_point = item.lower().find(text, 0, match_end_limit)
                if match_point >= 0:
                    # Use negative infinity to force keywords to sort after all
                    # fuzzy matches
                    match_item = -float('Infinity'), -match_point
                return match_item

        matches = []
        for cand in collection:
            if isinstance(cand, _Candidate):
                item, prio, display_meta, synonyms, prio2, display = cand
                if display_meta is None:
                    display_meta = meta
                syn_matches = (_match(x) for x in synonyms)
                # Nones need to be removed to avoid max() crashing in Python 3
                syn_matches = [m for m in syn_matches if m]
                sort_key = max(syn_matches) if syn_matches else None
            else:
                if cand == "":
                    # We don't offer up empty strings as suggestions
                    continue
                item, display_meta, prio, prio2, display = cand, meta, 0, 0, cand
                sort_key = _match(cand)

            if sort_key:
                if display_meta and len(display_meta) > 50:
                    # Truncate meta-text to 50 characters, if necessary
                    display_meta = display_meta[:47] + u'...'

                # Lexical order of items in the collection, used for
                # tiebreaking items with the same match group length and start
                # position. Since we use *higher* priority to mean "more
                # important," we use -ord(c) to prioritize "aa" > "ab" and end
                # with 1 to prioritize shorter strings (ie "user" > "users").
                # We first do a case-insensitive sort and then a
                # case-sensitive one as a tie breaker.
                # We also use the unescape_name to make sure quoted names have
                # the same priority as unquoted names.
                lexical_priority = (tuple(0 if c in(' _') else -ord(c) \
                                    for c in self.unescape_name(item.lower())) +
                                    (1,) + tuple(c for c in item))

                item = self.case(item)
                display = self.case(display)
                priority = (
                    sort_key, type_priority, prio, priority_func(item),
                    prio2, lexical_priority
                )

#                item = decode(item)
#                display_meta = decode(display_meta)
#                display = decode(display)

                matches.append(
                    Match(
                        completion=Completion(
                            text=item,
                            start_position=-text_len,
                            display_meta=display_meta,
                            display=display
                        ),
                        priority=priority
                    )
                )
        return matches

    def case(self, word):
        return self.casing.get(word, word)

    def get_completions(self, document, complete_event, smart_completion=None):
        # pylint: disable=arguments-differ

        word_before_cursor = document.get_word_before_cursor(WORD=True)

        if smart_completion is None:
            smart_completion = self.smart_completion

        # If smart_completion is off then return nothing.
        # Our notion of smart completion is all or none unlike PGCLI and MyCLI.
        matches = []
        if not smart_completion:
            return matches

        suggestions = suggest_type(document.text, document.text_before_cursor)

        for suggestion in suggestions:
            suggestion_type = type(suggestion)
            self.logger.debug('Suggestion type: %r', suggestion_type)

            # Map suggestion type to method
            # e.g. 'table' -> self.get_table_matches
            matcher = self.suggestion_matchers[suggestion_type]
            matches.extend(matcher(self, suggestion, word_before_cursor))

        # Sort matches so highest priorities are first
        matches = sorted(matches, key=operator.attrgetter('priority'),
                         reverse=True)

        return [m.completion for m in matches]

    def get_column_matches(self, suggestion, word_before_cursor):
        tables = suggestion.table_refs
        do_qualify = suggestion.qualifiable and {'always': True, 'never': False,
                                                 'if_more_than_one_table': \
                                                     len(tables) > 1}[self.qualify_columns]

        def qualify(col, tbl):
            return (tbl + '.' + self.case(col)) if do_qualify else self.case(col)

        self.logger.debug("Completion column scope: %r", tables)
        scoped_cols = self.populate_scoped_cols2(
            tables, suggestion.local_tables)

        def make_cand(name, ref):
            synonyms = (name, generate_alias(self.case(name)))
            return Candidate(qualify(name, ref), 0, 'column', synonyms)

        def flat_cols():
            return [make_cand(c.name, t.ref)
                    for t, cols in scoped_cols.items() for c in cols]
        if suggestion.require_last_table:
            # require_last_table is used for 'tb11 JOIN tbl2 USING (...' which should
            # suggest only columns that appear in the last table and one more
            ltbl = tables[-1].ref
            other_tbl_cols = set(
                c.name for t, cs in scoped_cols.items() if t.ref != ltbl for c in cs)
            scoped_cols = {
                t: [col for col in cols if col.name in other_tbl_cols]
                for t, cols in scoped_cols.items()
                if t.ref == ltbl
            }
        lastword = last_word(word_before_cursor, include='most_punctuations')
        if lastword == '*':
            if suggestion.context == 'insert':
                def filter_col(col):
                    if not col.has_default:
                        return True
                    return not any(
                        p.match(col.default)
                        for p in self.insert_col_skip_patterns
                    )
                scoped_cols = {
                    t: [col for col in cols if filter_col(col)] for t, cols in scoped_cols.items()
                }
            if self.asterisk_column_order == 'alphabetic':
                for cols in scoped_cols.values():
                    cols.sort(key=operator.attrgetter('name'))
            if lastword != word_before_cursor \
                and len(tables) == 1 \
                and word_before_cursor[-len(lastword) - 1] == '.':
                # User typed x.*; replicate "x." for all columns except the
                # first, which gets the original (as we only replace the "*"")
                sep = ', ' + word_before_cursor[:-1]
                collist = sep.join(self.case(c.completion)
                                   for c in flat_cols())
            else:
                collist = ', '.join(qualify(c.name, t.ref)
                                    for t, cs in scoped_cols.items() for c in cs)

            return [Match(
                completion=Completion(
                    collist,
                    -1,
                    display_meta='columns',
                    display='*'
                ),
                priority=(1, 1, 1)
            )]

        return self.find_matches(word_before_cursor, flat_cols(),
                                 meta='column')

    def alias(self, tbl, tbls):
        """ Generate a unique table alias
        tbl - name of the table to alias, quoted if it needs to be
        tbls - TableReference iterable of tables already in query
        """
        tbl = self.case(tbl)
        tbls = set(normalize_ref(t.ref) for t in tbls)
        if self.generate_aliases:
            tbl = generate_alias(self.unescape_name(tbl))
        if normalize_ref(tbl) not in tbls:
            return tbl
        if tbl[0] == '"':
            aliases = ('"' + tbl[1:-1] + str(i) + '"' for i in count(2))
        else:
            aliases = (tbl + str(i) for i in count(2))
        return next(a for a in aliases if normalize_ref(a) not in tbls)

    # TODO: Need to account for suggestion.catalog, tbls.catalog etc
    def get_join_matches(self, suggestion, word_before_cursor):
        tbls = suggestion.table_refs
        cols = self.populate_scoped_cols2(tbls)
        # Set up some data structures for efficient access
        qualified = dict((normalize_ref(t.ref), t.schema) for t in tbls)
        ref_prio = dict((normalize_ref(t.ref), n) for n, t in enumerate(tbls))
        refs = set(normalize_ref(t.ref) for t in tbls)
        other_tbls = set((t.schema, t.name)
                         for t in list(cols)[:-1])
        joins = []
        # Iterate over FKs in existing tables to find potential joins
        fks = ((fk, rtbl, rcol) for rtbl, rcols in cols.items()
               for rcol in rcols for fk in rcol.foreignkeys)
        col = namedtuple('col', 'schema tbl col')
        for fk, rtbl, rcol in fks:
            right = col(rtbl.schema, rtbl.name, rcol.name)
            child = col(fk.childschema, fk.childtable, fk.childcolumn)
            parent = col(fk.parentschema, fk.parenttable, fk.parentcolumn)
            left = child if parent == right else parent
            if suggestion.schema and left.schema != suggestion.schema:
                continue
            c = self.case
            if self.generate_aliases or normalize_ref(left.tbl) in refs:
                lref = self.alias(left.tbl, suggestion.table_refs)
                join = '{0} {4} ON {4}.{1} = {2}.{3}'.format(
                    c(left.tbl), c(left.col), rtbl.ref, c(right.col), lref)
            else:
                join = '{0} ON {0}.{1} = {2}.{3}'.format(
                    c(left.tbl), c(left.col), rtbl.ref, c(right.col))
            alias = generate_alias(self.case(left.tbl))
            synonyms = [join, '{0} ON {0}.{1} = {2}.{3}'.format(
                alias, c(left.col), rtbl.ref, c(right.col))]
            # Schema-qualify if (1) new table in same schema as old, and old
            # is schema-qualified, or (2) new in other schema, except public
            if not suggestion.schema and (qualified[normalize_ref(rtbl.ref)] and
                                          left.schema == right.schema or
                                          left.schema not in(right.schema, 'public')):
                join = left.schema + '.' + join
            prio = ref_prio[normalize_ref(rtbl.ref)] * 2 + (
                0 if (left.schema, left.tbl) in other_tbls else 1)
            joins.append(Candidate(join, prio, 'join', synonyms=synonyms))

        return self.find_matches(word_before_cursor, joins, meta='join')

    def get_join_condition_matches(self, suggestion, word_before_cursor):
        col = namedtuple('col', 'schema tbl col')
        tbls = self.populate_scoped_cols2(suggestion.table_refs).items
        cols = [(t, c) for t, cs in tbls() for c in cs]
        try:
            lref = (suggestion.parent or suggestion.table_refs[-1]).ref
            ltbl, lcols = [(t, cs) for (t, cs) in tbls() if t.ref == lref][-1]
        except IndexError:  # The user typed an incorrect table qualifier
            return []
        conds, found_conds = [], set()

        def add_cond(lcol, rcol, rref, prio, meta):
            prefix = '' if suggestion.parent else ltbl.ref + '.'
            case = self.case
            cond = prefix + case(lcol) + ' = ' + rref + '.' + case(rcol)
            if cond not in found_conds:
                found_conds.add(cond)
                conds.append(Candidate(cond, prio + ref_prio[rref], meta))

        def list_dict(pairs):  # Turns [(a, b), (a, c)] into {a: [b, c]}
            d = defaultdict(list)
            for pair in pairs:
                d[pair[0]].append(pair[1])
            return d

        # Tables that are closer to the cursor get higher prio
        ref_prio = dict((tbl.ref, num) for num, tbl
                        in enumerate(suggestion.table_refs))
        # Map (schema, table, col) to tables
        coldict = list_dict(((t.schema, t.name, c.name), t)
                            for t, c in cols if t.ref != lref)
        # For each fk from the left table, generate a join condition if
        # the other table is also in the scope
        fks = ((fk, lcol.name) for lcol in lcols for fk in lcol.foreignkeys)
        for fk, lcol in fks:
            left = col(ltbl.schema, ltbl.name, lcol)
            child = col(fk.childschema, fk.childtable, fk.childcolumn)
            par = col(fk.parentschema, fk.parenttable, fk.parentcolumn)
            left, right = (child, par) if left == child else (par, child)
            for rtbl in coldict[right]:
                add_cond(left.col, right.col, rtbl.ref, 2000, 'fk join')
        # For name matching, use a {(colname, coltype): TableReference} dict
        coltyp = namedtuple('coltyp', 'name datatype')
        col_table = list_dict((coltyp(c.name, c.datatype), t) for t, c in cols)
        # Find all name-match join conditions
        for c in (coltyp(c.name, c.datatype) for c in lcols):
            for rtbl in (t for t in col_table[c] if t.ref != ltbl.ref):
                prio = 1000 if c.datatype in (
                    'integer', 'bigint', 'smallint') else 0
                add_cond(c.name, c.name, rtbl.ref, prio, 'name join')

        return self.find_matches(word_before_cursor, conds, meta='join')

    # TODO: Need to account for suggestion.catalog
    def get_function_matches(
            self, suggestion, word_before_cursor, alias=False):
        """ OG: currently not used / hacked to exit early
        """
        # OG: hack early exit
        return []
        # We'll have to do away with this right? How can we possibly know this
        if suggestion.usage == 'from':
            # Only suggest functions allowed in FROM clause
            def filt(f):
                return not f.is_aggregate and not f.is_window
        else:
            alias = False

            def filt(_):
                return True
        arg_mode = {
            'signature': 'signature',
            'special': None,
        }.get(suggestion.usage, 'call')
        # Function overloading means we way have multiple functions of the same
        # name at this point, so keep unique names only
        funcs = set(
            self._make_cand(f, alias, suggestion, arg_mode)
            for f in self.populate_functions(suggestion.schema, filt)
        )

        matches = self.find_matches(word_before_cursor, funcs, meta='function')

        # OG: TODO: catalog here
        if not suggestion.schema and not suggestion.usage:
            # also suggest hardcoded functions using startswith matching
            predefined_funcs = self.find_matches(
                word_before_cursor, self.functions, mode='strict',
                meta='function')
            matches.extend(predefined_funcs)

        return matches

    def get_schema_matches(self, suggestion, word_before_cursor):
        conn = self.active_conn
        if suggestion.parent:
            catalog_u = self.unescape_name(suggestion.parent)
        else:
            catalog_u = conn.current_catalog()

        catalog_e = self.escape_name(catalog_u)
        self.logger.debug("get_schema_matches: parent %s", suggestion.parent)
        # OG: Note here, if there is even a single schema in [catalog_e].keys()
        # we'll happily return a potentially incomplete result set.
        schema_names_e = conn.dbmetadata.get_schemas(catalog = catalog_e)

        if schema_names_e is None:
            # Asking for schema in a non-existant catalog
            return []

        if len(schema_names_e) == 0:
            # Catalog exists in dbmetadata but is empty
            if suggestion.parent:
                # Looking for schemas in a specified catalog
                schema_names = []
                # Attempt list_schemas
                schema_names = conn.list_schemas(
                        catalog = conn.sanitize_search_string(catalog_u))

                if len(schema_names) < 1:
                    res = conn.find_tables(
                            catalog = conn.sanitize_search_string(catalog_u),
                            schema = "",
                            table = "",
                            type = "")
                    schema_names = [r.schema for r in res]
            else:
                # Looking for schemas in current catalog
                schema_names = conn.list_schemas()

            schema_names = set(schema_names)
            
            schema_names_e = self.escape_names(schema_names)
            conn.dbmetadata.extend_schemas(catalog = catalog_e, names = schema_names_e)

        return self.find_matches(
            word_before_cursor, schema_names_e, meta='schema')

    def get_blank_item_matches(self, suggestion, word_before_cursor):
        return []

    def get_from_clause_item_matches(self, suggestion, word_before_cursor):
        alias = self.generate_aliases
        s = suggestion
        self.logger.debug("get_from_clause: grandparent.parent %s.%s", s.grandparent, s.parent)
        t_sug = Table(s.grandparent, s.parent, s.table_refs, s.local_tables)
        v_sug = View(s.grandparent, s.parent, s.table_refs)
        f_sug = Function(s.parent, s.table_refs, usage='from')
        return (
            self.get_table_matches(t_sug, word_before_cursor, alias) +
            self.get_view_matches(v_sug, word_before_cursor, alias) +
            self.get_function_matches(f_sug, word_before_cursor, alias)
        )

    def _arg_list(self, func, usage):
        """Returns a an arg list string, e.g. `(_foo:=23)` for a func.

        :param func is a FunctionMetadata object
        :param usage is 'call', 'call_display' or 'signature'

        """
        template = {
            'call': self.call_arg_style,
            'call_display': self.call_arg_display_style,
            'signature': self.signature_arg_style
        }[usage]
        args = func.args()
        if not template:
            return '()'
        if usage == 'call' and len(args) < 2:
            return '()'
        if usage == 'call' and func.has_variadic():
            return '()'
        multiline = usage == 'call' and len(args) > self.call_arg_oneliner_max
        max_arg_len = max(len(a.name) for a in args) if multiline else 0
        args = (
            self._format_arg(template, arg, arg_num + 1, max_arg_len)
            for arg_num, arg in enumerate(args)
        )
        if multiline:
            return '(' + ','.join('\n    ' + a for a in args if a) + '\n)'
        return '(' + ', '.join(a for a in args if a) + ')'

    def _format_arg(self, template, arg, arg_num, max_arg_len):
        if not template:
            return None
        if arg.has_default:
            arg_default = 'NULL' if arg.default is None else arg.default
            # Remove trailing ::(schema.)type
            arg_default = arg_default_type_strip_regex.sub('', arg_default)
        else:
            arg_default = ''
        return template.format(
            max_arg_len=max_arg_len,
            arg_name=self.case(arg.name),
            arg_num=arg_num,
            arg_type=arg.datatype,
            arg_default=arg_default
        )

    def _make_cand(self, tbl, do_alias, suggestion, arg_mode=None):
        """Returns a Candidate namedtuple.

        :param tbl is a SchemaObject
        :param arg_mode determines what type of arg list to suffix for functions.
        Possible values: call, signature

        """
        cased_tbl = self.case(tbl.name)
        if do_alias:
            alias = self.alias(cased_tbl, suggestion.table_refs)
        synonyms = (cased_tbl, generate_alias(cased_tbl))
        maybe_alias = (' ' + alias) if do_alias else ''
        maybe_schema = (self.case(tbl.schema) + '.') if tbl.schema else ''
        maybe_catalog = (self.case(tbl.catalog) + '.') if tbl.catalog else ''
        suffix = self._arg_list_cache[arg_mode][tbl.meta] if arg_mode else ''
        if arg_mode == 'call':
            display_suffix = self._arg_list_cache['call_display'][tbl.meta]
        elif arg_mode == 'signature':
            display_suffix = self._arg_list_cache['signature'][tbl.meta]
        else:
            display_suffix = ''
#        item = maybe_schema + cased_tbl + suffix + maybe_alias
#        display = maybe_schema + cased_tbl + display_suffix + maybe_alias
        item = cased_tbl + suffix + maybe_alias
        display = cased_tbl + display_suffix + maybe_alias
        prio2 = 0 if tbl.schema else 1
        return Candidate(item, synonyms=synonyms, prio2=prio2, display=display)

    # OG: optimization opportunity here, need something like
    # get_table_view_matches for example for FromClause matching
    # since populate_objects has the ability to simultaneously query
    # both object types.  It can then conveniently populate the metadata for
    # both types in one fell swoop.
    # to get most bang for your buck need TableView object in sqlcompletion
    # and use that at locations where Table() and View() are passed together
    def get_table_matches(self, suggestion, word_before_cursor, alias=False):
        tables = self.populate_objects(suggestion.catalog, suggestion.schema, 'table')
        tables.extend(SchemaObject(tbl.name)
                      for tbl in suggestion.local_tables)
        tables = [self._make_cand(t, alias, suggestion) for t in tables]
        return self.find_matches(word_before_cursor, tables, meta='table')

    def get_view_matches(self, suggestion, word_before_cursor, alias=False):
        views = self.populate_objects(suggestion.catalog, suggestion.schema, 'view')
        views = [self._make_cand(v, alias, suggestion) for v in views]
        return self.find_matches(word_before_cursor, views, meta='view')

    def get_alias_matches(self, suggestion, word_before_cursor):
        aliases = suggestion.aliases
        return self.find_matches(word_before_cursor, aliases,
                                 meta='table alias')

    def get_database_matches(self, _, word_before_cursor):
        conn = self.active_conn
        catalogs_e = conn.dbmetadata.get_catalogs()
        if catalogs_e is None and (conn.connected()):
            catalogs_e = self.escape_names(conn.list_catalogs())
            conn.dbmetadata.extend_catalogs(catalogs_e)

        return self.find_matches(word_before_cursor, catalogs_e,
                                 meta='catalog')

    def get_keyword_matches(self, suggestion, word_before_cursor):
        keywords = self.keywords_tree.keys()
        # Get well known following keywords for the last token. If any, narrow
        # candidates to this list.
        next_keywords = self.keywords_tree.get(suggestion.last_token, [])
        if next_keywords:
            keywords = next_keywords

        casing = self.keyword_casing
        if casing == 'auto':
            if word_before_cursor and word_before_cursor[-1].islower():
                casing = 'lower'
            else:
                casing = 'upper'

        if casing == 'upper':
            keywords = [k.upper() for k in keywords]
        else:
            keywords = [k.lower() for k in keywords]

        return self.find_matches(word_before_cursor, keywords,
                                 mode='strict', meta='keyword')

    def get_path_matches(self, _, word_before_cursor):
        # pylint: disable=no-self-use
        # function cannot be static since it has to be a callable for get_completions
        completer = PathCompleter(expanduser=True)
        document = Document(text=word_before_cursor,
                            cursor_position=len(word_before_cursor))
        for c in completer.get_completions(document, None):
            yield Match(completion=c, priority=(0,))

    def get_special_matches(self, _, word_before_cursor):
#        commands = special.main.COMMANDS
        commands = {}
        cmds = commands.keys()
        cmds = [Candidate(cmd, 0, commands[cmd].description) for cmd in cmds]
        return self.find_matches(word_before_cursor, cmds, mode='strict')

    def get_datatype_matches(self, suggestion, word_before_cursor):
        """ OG: Currently not used
        """
        return []

    def get_namedquery_matches(self, _, word_before_cursor):
        """ OG: Currently not used
        """
        return []

    suggestion_matchers = {
        Blank: get_blank_item_matches,
        FromClauseItem: get_from_clause_item_matches,
        JoinCondition: get_join_condition_matches,
        Join: get_join_matches,
        Column: get_column_matches,
        Function: get_function_matches,
        Schema: get_schema_matches,
        Table: get_table_matches,
        View: get_view_matches,
        Alias: get_alias_matches,
        Database: get_database_matches,
        Keyword: get_keyword_matches,
        Special: get_special_matches,
        Datatype: get_datatype_matches,
        NamedQuery: get_namedquery_matches,
        Path: get_path_matches,
    }

    def populate_scoped_cols(self, scoped_tbls, local_tbls=()):
        """Find all columns in a set of scoped_tables.

        :param scoped_tbls: list of TableReference namedtuples
        :param local_tbls: tuple(TableMetadata)
        :return: {TableReference:{colname:ColumnMetaData}}

        """
        conn = self.active_conn
        ctes = dict((normalize_ref(t.name), t.columns) for t in local_tbls)
        columns = OrderedDict()
        metadata = conn.dbmetadata.data

        def addcols(schema, rel, alias, reltype, cols):
            tbl = TableReference(schema, rel, alias, reltype == 'functions')
            if tbl not in columns:
                columns[tbl] = []
            columns[tbl].extend(cols)

        for tbl in scoped_tbls:
            # Local tables should shadow database tables
            if tbl.schema is None and normalize_ref(tbl.name) in ctes:
                cols = ctes[normalize_ref(tbl.name)]
                addcols(None, tbl.name, 'CTE', tbl.alias, cols)
                continue
            schemas = [tbl.schema] if tbl.schema else self.search_path
            for schema in schemas:
                relname = self.escape_name(tbl.name)
                schema = self.escape_name(schema)
                if tbl.is_function:
                    # Return column names from a set-returning function
                    # Get an array of FunctionMetadata objects
                    functions = metadata['function'].get(schema, {}).get(relname)
                    for func in (functions or []):
                        # func is a FunctionMetadata object
                        cols = func.fields()
                        addcols(schema, relname, tbl.alias, 'functions', cols)
                else:
                    for reltype in ('table', 'view'):
                        cols = metadata[reltype].get(schema, {}).get(relname)
                        if cols:
                            cols = cols.values()
                            addcols(schema, relname, tbl.alias, reltype, cols)
                            break

        return columns
    def populate_scoped_cols2(self, scoped_tbls, local_tbls=()):
        ctes = dict((normalize_ref(t.name), t.columns) for t in local_tbls)
        columns = OrderedDict()

        def addcols(catalog, schema, rel, alias, reltype, cols):
            tbl = TableReference(catalog, schema, rel, alias, reltype == 'functions')
            if tbl not in columns:
                columns[tbl] = []
            columns[tbl].extend(cols)

        for tbl in scoped_tbls:
            # Local tables should shadow database tables
            if tbl.catalog is None and tbl.schema is None and normalize_ref(tbl.name) in ctes:
                cols = ctes[normalize_ref(tbl.name)]
                addcols(None, None, tbl.name, 'CTE', tbl.alias, cols)
                continue
            if tbl.catalog:
                catalog_u = self.unescape_name(tbl.catalog)
            else:
                catalog_u = self.active_conn.current_catalog()

            # TODO: What if no schema? Possible in some DBMS
            if tbl.schema:
                schema_u = self.unescape_name(tbl.schema)
            else:
                schema_u = ""

            if catalog_u is None or catalog_u == "":
                # Don't allow "".[schema].[table]
                # Interpret this to mean [schema]."".[table]
                catalog_u = schema_u
                schema_u = ""

            relname_u = self.unescape_name(tbl.name)
            catalog = self.escape_name(catalog_u)
            schema = self.escape_name(schema_u)
            relname = self.escape_name(relname_u)
            if tbl.is_function:
                # Return column names from a set-returning function
                # Get an array of FunctionMetadata objects
                # OG: fixme
                continue
                #functions = meta['functions'].get(schema, {}).get(relname)
                #for func in (functions or []):
                #    # func is a FunctionMetadata object
                #    cols = func.fields()
                #    addcols(schema, relname, tbl.alias, 'functions', cols)
            else:
                conn = self.active_conn
                # Per SQLColumns spec: CatalogName cannot contain a string search pattern
                res = conn.find_columns(
                        catalog = catalog_u,
                        schema = conn.sanitize_search_string(schema_u),
                        table = conn.sanitize_search_string(relname_u),
                        column = "%")
                if len(res):
                    cols = [ColumnMetadata(
                        name = col.column,
                        datatype = col.data_type,
                        has_default = col.default,
                        default = col.default
                        ) for col in res]
                    addcols(catalog, schema, relname, tbl.alias, "table", cols)

        return columns

    def _get_schemas(self, obj_typ, schema):
        """ OG: Currently not used
            Returns a list of schemas from which to suggest objects.

        :param schema is the schema qualification input by the user (if any)

        """
        conn = self.active_conn
        metadata = conn.dbmetadata.data
        submeta = metadata[obj_typ]
        if schema:
            schema = self.escape_name(schema)
            return [schema] if schema in submeta else []
        return self.search_path if self.search_path_filter else submeta.keys()

    def _maybe_schema(self, schema, parent):
        return None if parent or schema in self.search_path else schema

    def populate_schema_objects(self, schema, obj_type):
        """ OG: Currently not used
            Returns a list of SchemaObjects representing tables or views.

        :param schema is the schema qualification input by the user (if any)

        """
        conn = self.active_conn
        metadata = conn.dbmetadata.data

        return [
            SchemaObject(
                name=obj,
                schema=(self._maybe_schema(schema=sch, parent=schema))
            )
            for sch in self._get_schemas(obj_type, schema)
            for obj in metadata[obj_type][sch].keys()
        ]

    def populate_objects(self, catalog, schema, obj_type):
        """Returns a list of SchemaObjects representing tables or views.

        :param schema is the schema qualification input by the user (if any)

        """
        ret = []
        obj_names = []
        conn = self.active_conn
        self.logger.debug("populate_objects(%s): Called for %s.%s",
                obj_type, catalog, schema)
        if catalog is None and schema is None:
            catalog = ""
            schema = ""
        if catalog is None:
            # Set to current catalog
            catalog = conn.current_catalog()
            if catalog is None or catalog == "":
                # Don't allow "".[schema]
                # Interpret this to mean [schema].""
                catalog = schema
                schema = ""
        # Note to self, as soon as a period is inputted a schema (parent) is
        # no longer none.  Technically we should never be in a situation where
        # catalog is not None but schema is None.
        # Here we have to start being careful about escaping.  Are the keys in
        # dbmetadata always escaped?
        catalog_e = self.escape_name(self.unescape_name(catalog))
        schema_e = self.escape_name(self.unescape_name(schema))
        obj_names = conn.dbmetadata.get_objects(catalog = catalog_e, schema = schema_e, obj_type = obj_type)
        if obj_names is None:
            self.logger.debug("populate_objects(%s): Called for %s.%s, catalog/schema not found",
                    obj_type, catalog, schema)
            return []

        if len(obj_names) == 0:
            # catalog.schema were found but dbmetadata had no information as to
            # content.  So let's attempt to query
            obj_names = []
            self.logger.debug("populate_objects(%s): Did not find %s.%s metadata.  Will query.", obj_type, catalog_e, schema_e)
            # Special case: Look for tables without catalog/schema
            if catalog == "" and schema == "":
                res = conn.find_tables(
                        catalog = "\x00",
                        schema = "\x00",
                        table = "",
                        type = obj_type)
            else:
                res = conn.find_tables(
                        catalog = conn.sanitize_search_string(
                            self.unescape_name(catalog)),
                        schema = conn.sanitize_search_string(
                            self.unescape_name(schema)),
                        table = "",
                        type = obj_type)
            for r in res:
                name_e = self.escape_name(r.name)
                ret.append(
                    SchemaObject(
                        name=name_e,
                        schema=schema_e,
                        catalog=catalog_e
                    )
                )
                obj_names.append(name_e)
            self.logger.debug("populate_objects(%s): Query complete %s.%s", obj_type, catalog_e, schema_e)
            conn.dbmetadata.extend_objects(
                    catalog = catalog_e, schema = schema_e,
                    names = obj_names, obj_type = obj_type)
        else:
            for name_e in obj_names:
                ret.append(
                    SchemaObject(
                        name=name_e,
                        schema=schema_e, #should this be r.schema
                        catalog=catalog_e #should this be r.catalog
                    )
                )
        return ret

    def populate_functions(self, schema, filter_func):
        """ OG: currently not used.
            Returns a list of function SchemaObjects.

        :param filter_func is a function that accepts a FunctionMetadata
        namedtuple and returns a boolean indicating whether that
        function should be kept or discarded

        """

        conn = self.active_conn
        metadata = conn.dbmetadata.data
        # Because of multiple dispatch, we can have multiple functions
        # with the same name, which is why `for meta in metas` is necessary
        # in the comprehensions below
        return [
            SchemaObject(
                name=func,
                schema=(self._maybe_schema(schema=sch, parent=schema)),
                meta=meta
            )
            for sch in self._get_schemas('function', schema)
            for (func, metas) in metadata['function'][sch].items()
            for meta in metas
            if filter_func(meta)
        ]
