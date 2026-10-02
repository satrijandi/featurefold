"""featuremart CLI: compile feature specs into dbt models and a feature registry."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from generator.expand import build_plan
from generator.project import render_project
from generator.registry import write_registry
from generator.render import Renderer
from generator.spec import SpecError, load_spec

DEFAULT_SPECS = Path("features")
DEFAULT_DBT = Path("transform")
DEFAULT_REGISTRY = Path("registry")

# Frozen so that a `generate --check` diff reflects a real change in the spec
# rather than the clock. The registry's real version marker is spec_version.
FROZEN_TIMESTAMP = "generated-on-demand"


def _spec_files(specs_dir: Path) -> list[Path]:
    return sorted(p for p in specs_dir.glob("*.yml") if not p.name.startswith("_"))


@click.group()
@click.version_option(package_name="feature-mart")
def cli() -> None:
    """Compile YAML feature specs into dbt models, tests and a feature registry."""


@cli.command()
@click.option("--specs-dir", type=click.Path(path_type=Path), default=DEFAULT_SPECS)
@click.option("--dbt-root", type=click.Path(path_type=Path), default=DEFAULT_DBT)
@click.option("--registry-dir", type=click.Path(path_type=Path), default=DEFAULT_REGISTRY)
@click.option(
    "--check",
    is_flag=True,
    help="Do not write. Exit non-zero if any generated file differs from what is on disk.",
)
def generate(specs_dir: Path, dbt_root: Path, registry_dir: Path, check: bool) -> None:
    """Generate dbt models, schema docs, invariant tests and the registry."""
    specs = _spec_files(specs_dir)
    if not specs:
        raise click.ClickException(f"no feature specs found in {specs_dir}/")

    planned: list[tuple[Path, str]] = []
    loaded: list = []
    total_features = 0

    for path in specs:
        try:
            spec = load_spec(path)
            plan = build_plan(spec)
        except SpecError as exc:
            raise click.ClickException(f"{path}:\n{exc}") from None

        for warning in getattr(plan, "warnings", []):
            click.secho(f"  warning [{spec.feature_name}]: {warning}", fg="yellow")

        loaded.append(spec)
        renderer = Renderer(plan)
        for rendered in renderer.render_all(dbt_root):
            planned.append((rendered.path, rendered.content))
        planned.append(
            (
                registry_dir / f"{spec.feature_name}.json",
                write_registry(plan, registry_dir, generated_at=FROZEN_TIMESTAMP),
            )
        )
        total_features += plan.feature_count
        click.echo(
            f"  {spec.feature_name}: {plan.feature_count} features "
            f"from {len(plan.partials)} partial columns"
        )

    # Sources, their staging models and the owner groups are project-wide: two
    # specs may read the same table or share an owner, and dbt allows exactly
    # one definition of each.
    try:
        planned += render_project(loaded, dbt_root)
    except SpecError as exc:
        raise click.ClickException(str(exc)) from None

    # A generated file with no spec behind it is drift too: it means a spec was
    # deleted or renamed, and stale models were left behind to keep running.
    expected = {p for p, _ in planned}
    orphans = [
        found
        for folder, pattern in (
            (dbt_root / "models", "**/*.sql"),
            (dbt_root / "models", "**/*.yml"),
            (dbt_root / "tests", "*.sql"),
            (registry_dir, "*.json"),
        )
        for found in folder.glob(pattern)
        if found not in expected and "GENERATED FILE" in found.read_text()
    ]

    if check:
        drifted = [p for p, content in planned if not p.exists() or p.read_text() != content]
        drifted += orphans
        if drifted:
            click.secho("\ngenerated output is out of date:", fg="red", bold=True)
            for p in sorted(set(drifted)):
                click.echo(f"  {p}")
            click.echo("\nrun `make generate` and commit the result.")
            sys.exit(1)
        click.secho(f"\nup to date: {len(planned)} files, {total_features} features", fg="green")
        return

    for path, content in planned:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    for path in orphans:
        path.unlink()
        click.echo(f"  removed {path} (no spec generates it any more)")
    click.secho(
        f"\nwrote {len(planned)} files across {len(specs)} spec(s), "
        f"{total_features} features total",
        fg="green",
    )


@cli.command()
@click.argument("spec_file", type=click.Path(exists=True, path_type=Path))
def explain(spec_file: Path) -> None:
    """Show how a spec expands, without writing anything."""
    try:
        spec = load_spec(spec_file)
        plan = build_plan(spec)
    except SpecError as exc:
        raise click.ClickException(str(exc)) from None

    click.secho(f"\n{spec.feature_name}", bold=True)
    click.echo(f"  owner        {spec.created_by}")
    click.echo(f"  spec version {spec.spec_hash}")
    click.echo(f"  entities     {', '.join(spec.entities)}")
    click.echo(f"  windows      {', '.join(w.name for w in spec.windows)}")
    click.echo(
        f"  convention   {spec.settings.window_convention}, "
        f"spine={spec.settings.entity_spine}, late_arrival={spec.settings.late_arrival_days}d"
    )

    click.secho("\n  expansion", bold=True)
    header = f"    {'field':<20} {'aggs':<22} {'combos':>7} {'windows':>8} {'features':>9}"
    click.echo(header)
    click.echo("    " + "-" * (len(header) - 4))
    for f in spec.fields:
        n_combo = 1
        for cat in f.apply_cond_cat:
            n_combo *= len(spec.categories[cat].members)
        n_feat = n_combo * len(f.aggs) * len(spec.windows)
        kind = " (derived)" if f.is_derived else ""
        click.echo(
            f"    {f.name:<20} {','.join(f.aggs) + kind:<22} {n_combo:>7} "
            f"{len(spec.windows):>8} {n_feat:>9}"
        )
    click.echo("    " + "-" * (len(header) - 4))
    click.echo(f"    {'TOTAL':<20} {'':<22} {'':>7} {'':>8} {plan.feature_count:>9}")

    click.secho("\n  storage", bold=True)
    click.echo(f"    partial columns per entity-day : {len(plan.partials)}")
    click.echo(f"    stored features                : {len(plan.public_stored)}")
    click.echo(f"    computed at publish time       : {len(plan.computed)}")

    click.secho("\n  sample feature names", bold=True)
    for name in plan.output_order[:4]:
        click.echo(f"    {name}")
    click.echo("    ...")
    for name in plan.output_order[-2:]:
        click.echo(f"    {name}")
    click.echo()


@cli.command()
@click.option("--specs-dir", type=click.Path(path_type=Path), default=DEFAULT_SPECS)
def validate(specs_dir: Path) -> None:
    """Validate every spec without generating."""
    failed = 0
    for path in _spec_files(specs_dir):
        try:
            plan = build_plan(load_spec(path))
            click.secho(f"  ok   {path}  ({plan.feature_count} features)", fg="green")
        except SpecError as exc:
            failed += 1
            click.secho(f"  FAIL {path}", fg="red", bold=True)
            click.echo("       " + str(exc).replace("\n", "\n       "))
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    cli()
