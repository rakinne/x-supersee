"""Supersee CLI.

Subcommands (planned):
    supersee serve              # uvicorn supersee.api:app
    supersee migrate            # apply pending migrations from migrations/
    supersee score <fixture>    # run the scorer over a fixture event, print JSON
    supersee replay-fixture     # pipe a fixture event through the full pipeline
    supersee refresh-ofac       # force-refresh ofac_sdn_crypto table
"""

from __future__ import annotations

import typer

app = typer.Typer(
    no_args_is_help=True,
    help="Supersee compliance investigation CLI.",
    add_completion=False,
)


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", help="Host to bind."),
    port: int = typer.Option(8000, help="Port to bind."),
    reload: bool = typer.Option(False, help="Enable uvicorn auto-reload (dev only)."),
) -> None:
    """Run the FastAPI app and the four background tasks via uvicorn."""
    import uvicorn

    uvicorn.run(
        "supersee.api:app",
        host=host,
        port=port,
        reload=reload,
        reload_dirs=["supersee"] if reload else None,
    )


if __name__ == "__main__":
    app()
