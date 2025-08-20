from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, TabbedContent, Tab, Static


class TabTestApp(App):

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        with TabbedContent():
            with Tab("Red Button"):
                static = Static("This is the red tab.")
                static.styles.background = "red"
                yield static
            with Tab("Green Button"):
                static = Static("This is the green tab.")
                static.styles.background = "green"
                yield static
            with Tab("Blue Button"):
                static = Static("This is the blue tab.")
                static.styles.background = "blue"
                yield static
        yield Footer()


if __name__ == "__main__":
    app = TabTestApp()
    app.run()
