import sys
import platform
from cyanodbc import Connection
from typing import List, Optional
from logging import getLogger
from prompt_toolkit.layout.containers import Window, ScrollOffsets, ConditionalContainer, Container
from prompt_toolkit.formatted_text.base import StyleAndTextTuples
from prompt_toolkit.formatted_text import fragment_list_width
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.dimension import Dimension
from prompt_toolkit.filters import is_done
from prompt_toolkit.layout.margins import ScrollbarMargin
from prompt_toolkit.mouse_events import MouseEvent
from .conn import sqlConnection
from .filters import ShowSidebar
from .utils import if_mousedown

class myDBObject:
    def __init__(
        self,
        conn: sqlConnection,
        name: str,
        otype: str,
        level: Optional[int] = 0,
        children: Optional[List["myDBObject"]] = None,
        parent: Optional["myDBObject"] = None,
        next_object: Optional["myDBObject"] = None
    ) -> None:

        self.conn = conn
        self.children = children
        self.parent = parent
        self.name = name
        self.otype = otype
        self.next_object = next_object
        self.level = level
        self.selected: bool = False
        self.logger = getLogger(__name__)

    def expand(self) -> None:
        """
        Populates children and sets parent for children nodes
        """
        raise NotImplementedError()

    def collapse(self) -> None:
        """
        Populates children and sets parent for children nodes
        Note, we don't have to blow up the children; just redirect
        next_object.  This way we re-query the database / force re-fresh
        which may be suboptimal.  TODO: Codify not/refresh path
        """
        if not self.selected:
            return
        if self.children is not None:
            self.next_object = self.children[len(self.children) - 1].next_object
            obj = self.children[len(self.children) - 1].next_object
            if obj is not None and obj.level <= self.level:
                self.next_object = obj
            else:
                self.next_object = None
            self.children = None
        elif self.parent is not None:
            self.parent.selected = True
            self.parent.collapse()

    def add_children(self, list_obj: List["myDBObject"]) -> None:
        self.children = list_obj
        for i in range(len(self.children) - 1):
            self.children[i].next_object = self.children[i + 1]
        self.children[len(self.children) - 1].next_object = self.next_object
        self.next_object = self.children[0]

    def select_next(self) -> None:
        if self.selected_object is None or self.next_object is None:
            self.selected = True
            return None

        if self.selected:
            self.next_object.selected = True
            self.selected = False
        else:
            self.next_object.select_next()

    def select_previous(self) -> None:
        if self.selected == True:
        # Can't proceed past this point so exit
        # should never happen unless the very first object
        # is seleced in which case it makes sense
            return None
        if self.selected_object is None or self.next_object is None:
            self.selected = True
            return None

        if self.next_object.selected == True:
            self.next_object.selected = False
            self.selected = True
        else:
            self.next_object.select_previous()

    @property
    def selected_object(self) -> "myDBObject":
        if self.selected:
            return self
        elif self.next_object:
            return self.next_object.selected_object
        return None

class myDBColumn(myDBObject):
    def expand(self) -> None:
        self.children = None

class myDBTable(myDBObject):
    def expand(self) -> None:
        if self.children is not None:
            return None
        cat = "%"
        schema = "%"

        if self.parent is not None:
            if type(self.parent).__name__ == "myDBSchema":
                schema = self.conn.sanitize_search_string(self.parent.name)
            elif type(self.parent).__name__ == "myDBCatalog":
                cat = self.conn.sanitize_search_string(self.parent.name)
            if self.parent.parent is not None:
                if type(self.parent.parent).__name__ == "myDBCatalog":
                    cat = self.conn.sanitize_search_string(
                            self.parent.parent.name)

        res = self.conn.find_columns(
                catalog = cat,
                schema = schema,
                table = self.name,
                column = "%")

        lst = [myDBColumn(
            conn = self.conn,
            name = col.column,
            otype = col.type_name,
            parent = self,
            level = self.level + 1) for col in res]
        if len(lst):
            self.add_children(list_obj = lst)

class myDBSchema(myDBObject):
    def expand(self) -> None:
        if self.children is not None:
            return None
        cat = self.conn.sanitize_search_string(self.parent.name) if self.parent is not None else "%"
        res = self.conn.find_tables(
                catalog = cat,
                schema = self.name,
                table = "",
                type = "")
        lst = [myDBTable(
            conn = self.conn,
            name = table.name,
            otype = table.type,
            parent = self,
            level = self.level + 1) for table in res]
        if len(lst):
            self.add_children(list_obj = lst)

class myDBCatalog(myDBObject):
    def expand(self) -> None:
        if self.children is not None:
            return None

        res = self.conn.find_tables(
                catalog = self.conn.sanitize_search_string(self.name),
                schema = "",
                table = "",
                type = "")
        schemas = []
        for r in res:
            if (r.schema not in schemas and r.schema != ""):
                schemas.append(r.schema)
        if len(schemas):
            lst = [myDBSchema(
                conn = self.conn,
                name = schema,
                otype = "Schema",
                parent = self,
                level = self.level + 1) for schema in schemas]
            self.add_children(list_obj = lst)
            return None

        # No schemas found; but if there are tables then these are direct
        # descendents
        lst = [myDBTable(
            conn = self.conn,
            name = table.name,
            otype = table.type,
            parent = self,
            level = self.level + 1) for table in res]
        if len(lst):
            self.add_children(list_obj = lst)

class myDBConn(myDBObject):
    def expand(self) -> None:
        if not self.conn.connected():
            return None
        if self.children is not None:
            return None

        cat_support = self.conn.catalog_support()
        if cat_support:
            rows = self.conn.list_catalogs()
            if len(rows):
                lst = [myDBCatalog(
                    conn = self.conn,
                    name = row,
                    otype = "Catalog",
                    parent = self,
                    level = self.level + 1) for row in rows]
                self.add_children(list_obj = lst)
            return None

        res = self.conn.find_tables(
                catalog = "%",
                schema = "",
                table = "",
                type = "")
        schemas = []
        for r in res:
            if (r.schema not in schemas and r.schema != ""):
                schemas.append(r.schema)
        if len(schemas):
            lst = [myDBSchema(
                conn = self.conn,
                name = schema,
                otype = "Schema",
                parent = self,
                level = self.level + 1) for schema in schemas]
            self.add_children(list_obj = lst)
            return None

        lst = [myDBTable(
            conn = self.conn,
            name = table.name,
            otype = table.type,
            parent = self,
            level = self.level + 1) for table in res]
        if len(lst):
            self.add_children(list_obj = lst)

def sql_sidebar(my_app: "sqlApp") -> Window:
    """
    Create the `Layout` for the sidebar with the configurable objects.
    """

    @if_mousedown
    def expand_item(obj: "myDBObject") -> None:
        obj.expand()
    def get_text_fragments() -> StyleAndTextTuples:
        tokens: StyleAndTextTuples = []
        def append(obj: "myDBObject") -> None:
            " Recursively build the token list "
            selected = obj.selected
            expanded = obj.children is not None
            active = my_app.active_conn is not None and my_app.active_conn is obj.conn and obj.level == 0

            act = ",active" if active else ""
            sel = ",selected" if selected else ""
            if len(obj.name) > 24 -  2 * obj.level:
                name_trim = obj.name[:24 - 2 * obj.level - 3] + "..."
            else:
                name_trim = ("%-" + str(24 - 2 * obj.level) + "s") % obj.name

            tokens.append(
                ("class:sidebar.status" + sel, " " * 2 * obj.level, expand_item)
            )
            tokens.append(("class:sidebar" + sel, " >" if selected else "  "))
            tokens.append(
                ("class:sidebar.label" + sel + act,
#                ("%-" + str(24 - 2 * obj.level) + "s") % obj.name,
                name_trim,
                expand_item)
            )
            tokens.append(("class:sidebar.status" + sel + act, " ", expand_item))
            tokens.append(("class:sidebar.status" + sel + act, "%+12s" % obj.otype, expand_item))

            if selected:
                tokens.append(("[SetCursorPosition]", ""))

            if expanded:
                tokens.append(("class:sidebar", "\/"))
            else:
                tokens.append(("class:sidebar", " <" if selected else "  "))
            tokens.append(("class:sidebar", "\n"))

        obj = my_app.obj_list[0]
        append(obj)
        while obj.next_object:
            append(obj.next_object)
            obj = obj.next_object

        tokens.pop()  # Remove last newline.

        return tokens

    class Control(FormattedTextControl):
        def move_cursor_down(self):
            my_app.obj_list[0].select_next()
        # Need to figure out what do do here
        # AFAICT thse are only called for the mouse handler
        # when events are otherwise not handled
        def move_cursor_up(self):
            my_app.obj_list[0].select_previous()

    return Window(
        Control(get_text_fragments),
        right_margins = [ScrollbarMargin(display_arrows = True)],
        style = "class:sidebar",
        width = Dimension.exact( 45 ),
        height = Dimension(min = 15, preferred = 33),
        scroll_offsets = ScrollOffsets(top = 1, bottom = 1),
    )

def sql_sidebar_help(my_app: "sqlApp"):
    """
    Create the `Layout` for the help text for the current item in the sidebar.
    """
    token = "class:sidebar.helptext"

    def get_current_description():
        """
        Return the description of the selected option.
        """
        obj = my_app.obj_list[0].selected_object
        if obj is not None:
            return obj.name
        return ""

    def get_help_text():
        return [(token, get_current_description())]

    return ConditionalContainer(
        content=Window(
            FormattedTextControl(get_help_text), style=token, height=Dimension(min=3)
            ),
        filter=ShowSidebar(my_app) & ~is_done,
        )

def sql_sidebar_navigation():
    """
    Create the `Layout` showing the navigation information for the sidebar.
    """

    def get_text_fragments():
        # Show navigation info.
        return [
            ("class:sidebar", "   "),
            ("class:sidebar.key", "[Up/Dn]"),
            ("class:sidebar", " "),
            ("class:sidebar.description", "Navigate"),
            ("class:sidebar", " "),
            ("class:sidebar.key", "[L/R]"),
            ("class:sidebar", " "),
            ("class:sidebar.description", "Expand/Collapse"),
            ("class:sidebar", "\n   "),
            ("class:sidebar.key", "[Enter]"),
            ("class:sidebar", " "),
            ("class:sidebar.description", "Connect/Preview"),
        ]

    return Window(
        FormattedTextControl(get_text_fragments),
        style = "class:sidebar",
        width=Dimension.exact( 45 ),
        height=Dimension(max = 2),
    )

def show_sidebar_button_info(my_app: "sqlApp") -> Container:
    """
    Create `Layout` for the information in the right-bottom corner.
    (The right part of the status bar.)
    """

    @if_mousedown
    def toggle_sidebar(mouse_event: MouseEvent) -> None:
        " Click handler for the menu. "
        my_app.show_sidebar = not my_app.show_sidebar

    # TO DO: app version rather than python
    version = sys.version_info
    tokens: StyleAndTextTuples = [
            ("class:status-toolbar.key", "[C-t]", toggle_sidebar),
            ("class:status-toolbar", " Object Browser", toggle_sidebar),
            ("class:status-toolbar", " - "),
            (
                "class:status-toolbar.python-version",
                "%s %i.%i.%i"
                % (platform.python_implementation(), version[0], version[1], version[2]),
                ),
            ("class:status-toolbar", " "),
            ]
    width = fragment_list_width(tokens)

    def get_text_fragments() -> StyleAndTextTuples:
        # Python version
        return tokens

    return ConditionalContainer(
            content=Window(
                FormattedTextControl(get_text_fragments),
                style="class:status-toolbar",
                height=Dimension.exact(1),
                width=Dimension.exact(width),
                ),
            filter=~is_done
#            & renderer_height_is_known
            )
