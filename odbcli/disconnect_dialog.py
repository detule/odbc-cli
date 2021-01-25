from prompt_toolkit.widgets import Button, Dialog, Label
from prompt_toolkit.layout.containers import ConditionalContainer
from prompt_toolkit.layout.dimension import Dimension as D
from prompt_toolkit.filters import is_done
from .conn import connWrappers
from .filters import ShowDisconnectDialog

def disconnect_dialog(my_app: "sqlApp"):
    def yes_handler() -> None:
        # This is not preferred since completer may currently have
        # a different connection attached.
        # my_app.completer.reset_completions()
        obj = my_app.selected_object
        obj.conn.dbmetadata.reset_metadata()
        obj.collapse()
        obj.conn.close()
        if my_app.active_conn is obj.conn:
            my_app.active_conn = None
        my_app.show_disconnect_dialog = False
        my_app.show_sidebar = True
        my_app.application.layout.focus("sidebarbuffer")

    def rc_handler() -> None:
        obj = my_app.selected_object
        obj.conn.dbmetadata.reset_metadata()
        my_app.show_disconnect_dialog = False
        my_app.show_sidebar = True
        my_app.application.layout.focus("sidebarbuffer")

    def no_handler() -> None:
        my_app.show_disconnect_dialog = False
        my_app.show_sidebar = True
        my_app.application.layout.focus("sidebarbuffer")

    dialog = Dialog(
        title = lambda: my_app.selected_object.name,
        body = Label(text = "Disconnect or Reset Completions?",
            dont_extend_height = True),
        buttons = [
            Button(text = "Disconnect", handler = yes_handler),
            Button(text = "Reset Completions", handler = rc_handler, width = 20),
            Button(text = "Cancel", handler = no_handler),
        ],
        width = D(min = 10, preferred = 50),
        with_background = False,
    )

    return ConditionalContainer(
            content = dialog,
            filter = ShowDisconnectDialog(my_app) & ~is_done
    )
