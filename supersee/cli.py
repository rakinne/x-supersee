"""Supersee CLI.

Subcommands:
    supersee serve              # uvicorn supersee.api:app
    supersee score <fixture>    # run the scorer over a fixture event, print JSON

Subcommands (planned):
    supersee migrate            # apply pending migrations from migrations/
    supersee replay-fixture     # pipe a fixture event through the full pipeline
    supersee refresh-ofac       # force-refresh ofac_sdn_crypto table
"""

from __future__ import annotations

import json
from pathlib import Path

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
    reload: bool = typer.Option(
        False, "--reload", help="Enable uvicorn auto-reload (dev only)."
    ),
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


@app.command()
def score(
    fixture: Path = typer.Argument(
        ...,
        help="Path to a fixture JSON file with `event` and `context` keys.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print the output."),
) -> None:
    """Run the deterministic scorer over a fixture event and print the result.

    The fixture is self-contained (no DB needed) so reviewers can iterate
    on rule weights and thresholds locally without spinning up the full
    pipeline. The output JSON matches the `cases` row shape: `risk_score`,
    `risk_band`, and `rule_hits` (only firing rules).
    """
    # Local imports so `supersee --help` and `supersee serve` don't pay the
    # scorer/config import cost.
    from supersee.config.scorer import ScorerSettings
    from supersee.scorer.score_event import EventData, ScoringContext, score_event

    data = json.loads(fixture.read_text())
    event_raw = data["event"]
    event = EventData(
        amount_xrp=event_raw.get("amount_xrp"),
        account_src=event_raw["account_src"],
        account_dst=event_raw.get("account_dst"),
        memo_decoded=event_raw.get("memo_decoded"),
    )
    ctx_raw = data.get("context", {})
    ctx = ScoringContext(
        rolling_p99_7d=ctx_raw.get("rolling_p99_7d"),
        known_counterparties=frozenset(ctx_raw.get("known_counterparties", [])),
        recent_outbound_count=ctx_raw.get("recent_outbound_count", 0),
        ofac_addresses=frozenset(ctx_raw.get("ofac_addresses", [])),
        watchlist_addresses=frozenset(ctx_raw.get("watchlist_addresses", [])),
        mixer_addresses=frozenset(ctx_raw.get("mixer_addresses", [])),
    )

    result = score_event(event, ctx, ScorerSettings())
    output: dict[str, object] = {
        "risk_score": result.risk_score,
        "risk_band": result.risk_band,
        "rule_hits": result.to_jsonb(),
    }
    typer.echo(json.dumps(output, indent=2 if pretty else None))


if __name__ == "__main__":
    app()
