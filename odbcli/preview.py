from prompt_toolkit.layout.processors import ConditionalProcessor, HighlightIncrementalSearchProcessor, HighlightSelectionProcessor
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.layout.controls import BufferControl
from prompt_toolkit.document import Document
from prompt_toolkit.filters import has_focus, is_done
from prompt_toolkit.layout.dimension import Dimension as D
from prompt_toolkit.widgets import Button, TextArea, SearchToolbar, Box, Shadow, Frame
from prompt_toolkit.layout.containers import Window, VSplit, HSplit, ConditionalContainer
from prompt_toolkit.filters import is_done
from cyanodbc import ConnectError, DatabaseError
from cli_helpers.tabular_output import TabularOutputFormatter
from functools import partial
from .filters import ShowPreview
from .conn import connWrappers, connStatus, executionStatus
from logging import getLogger

def preview_element(my_app: "sqlApp"):
    help_text = """
    Press Enter in the input box to page through the table.
    Alternatively, enter a filtering SQL statement and then press Enter
    to page through the results.
    """
    formatter = TabularOutputFormatter()
    input_buffer = Buffer(
            name = "previewbuffer",
            tempfile_suffix = ".sql",
            multiline = False
            )

    input_control = BufferControl(
            buffer = input_buffer,
            include_default_input_processors = False,
            preview_search = False
    )
    input_window = Window(
            input_control,
        )

    search_buffer = Buffer(name = "previewsearchbuffer")
    search_field = SearchToolbar(search_buffer)
    output_field = TextArea(style = "class:preview-output-field",
            text = help_text,
            height = D(preferred = 50),
            search_field=search_field,
            wrap_lines = False,
            focusable = True,
            read_only = True,
            preview_search = True,
            input_processors = [
                ConditionalProcessor(
                    processor=HighlightIncrementalSearchProcessor(),
                    filter=has_focus("previewsearchbuffer")
                    | has_focus(search_field.control),
                    ),
                HighlightSelectionProcessor(),
            ]
            )

    def refresh_results(window_height) -> bool:
        sql_conn = my_app.selected_object.conn

        if sql_conn.execution_status == executionStatus.FAIL:
            # Let's display the error message to the user
            output = sql_conn.execution_err
        else:
            crsr = sql_conn.cursor
            if crsr.description:
                cols = [col.name for col in crsr.description]
            else:
                cols = []
            if len(cols):
                sql_conn.status = connStatus.FETCHING
                res = sql_conn.async_fetchmany(size = window_height - 4)
                output = formatter.format_output(res, cols, format_name = "psql")
                output = "\n".join(output)
            else:
                sql_conn.status = connStatus.IDLE
                output = "No rows returned\n"

        # Add text to output buffer.
        output_field.buffer.set_document(Document(
            text = output, cursor_position = 0), True)

        return True

    def accept(buff: Buffer) -> bool:
        obj = my_app.selected_object
        sql_conn = obj.conn
        catalog = None
        schema = None
        # TODO: Verify connected
        if obj.parent is not None:
            if type(obj.parent).__name__ == "myDBSchema":
                schema = obj.parent.name
            elif type(obj.parent).__name__ == "myDBCatalog":
                catalog = obj.parent.name
            if obj.parent.parent is not None:
                if type(obj.parent.parent).__name__ == "myDBCatalog":
                    catalog = obj.parent.parent.name

        if catalog:
            catalog =  (sql_conn.quotechar + "%s" + sql_conn.quotechar) % catalog
        if schema:
            schema =  (sql_conn.quotechar + "%s" + sql_conn.quotechar) % schema
        name = (sql_conn.quotechar + "%s" + sql_conn.quotechar) % obj.name
        identifier = ".".join(list(filter(None, [catalog, schema, obj.name])))
        query = sql_conn.preview_query(table = identifier, filter_query = buff.text,
                limit = my_app.preview_limit_rows)

        func = partial(refresh_results,
                window_height = output_field.window.render_info.window_height)
        # If status is IDLE, this is the first time we are executing.
        if sql_conn.query != query or sql_conn.status == connStatus.IDLE:
            # Exit the app to execute the query
            my_app.application.exit(result = ["preview", query])
            my_app.application.pre_run_callables.append(func)
        else:
            # No need to exit let's just go and fetch
            func()
        return True # Keep filter text

    input_buffer.accept_handler = accept

    def cancel_handler() -> None:
        sql_conn = my_app.selected_object.conn
        sql_conn.close_cursor()
        sql_conn.status = connStatus.IDLE
        input_buffer.text = ""
        output_field.buffer.set_document(Document(
            text = help_text, cursor_position = 0
        ), True)
        my_app.show_preview = False
        my_app.show_sidebar = True
        my_app.application.layout.focus(input_buffer)
        my_app.application.layout.focus("sidebarbuffer")
        return None

    cancel_button = Button(text = "Done", handler = cancel_handler)

    container = HSplit(
            [
                Box(
                    body = VSplit(
                        [input_window, cancel_button],
                        padding=1
                    ),
                    padding=1,
                    style="class:preview-input-field"
                ),
                Window(height=1, char="-", style="class:preview-divider-line"),
                output_field,
                search_field,
                ]
            )

    frame = Shadow(
            body = Frame(
                title = "Table Preview",
                body = container,
                style="class:dialog.body",
                width = D(preferred = 180, min = 30),
                modal = True
            )
    )


    return ConditionalContainer(
            content = frame,
            filter = ShowPreview(my_app) & ~is_done
    )

