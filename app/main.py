import typer
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

from app.sync import DbtLookerSync

from rich.table import Table

app = typer.Typer()
console = Console()


def show_preview_and_confirm(syncer: DbtLookerSync, model_name: str) -> str:
    """
    Shows a preview of changes and asks for confirmation.
    Returns: 'both', 'docs', 'fields', or 'none'
    """
    candidates = syncer.get_sync_candidates()
    # Find the candidate matching the model_name
    match = next((c for c in candidates if c[0] == model_name), None)
    
    if not match:
        console.print(f"[bold red]Could not find dbt model '{model_name}' or matching LookML view.[/bold red]")
        return "none"

    _, view_path, _ = match
    dbt_model_data = syncer.dbt_parser.get_model(model_name)
    lookml_data = syncer.lookml_parser.load_lookml_file(view_path)
    
    if "views" not in lookml_data or not lookml_data["views"]:
        console.print(f"[bold red]No views found in {view_path.name}[/bold red]")
        return "none"
        
    view = lookml_data["views"][0]
    changes = syncer.get_structured_pending_changes(view, dbt_model_data)
    
    if not changes:
        console.print(f"[yellow]No changes pending for {model_name}.[/yellow]")
        return "none"

    table = Table(title=f"Pending changes for {view_path.name} (model: {model_name})")
    table.add_column("Field", style="cyan")
    table.add_column("Action", style="magenta")
    table.add_column("dbt Description", style="green")

    for c in changes:
        desc = c['description'][:50] + "..." if len(c['description']) > 50 else c['description']
        table.add_row(c['field'], c['action'], desc)

    console.print(table)
    
    from rich.prompt import Prompt
    choice = Prompt.ask(
        "\nWhat would you like to sync?",
        choices=["both", "docs", "fields", "none"],
        default="both"
    )
    return choice


@app.callback(invoke_without_command=True)
def main(
    dbt_project_dir: Path = typer.Option(
        ...,
        "--dbt-project-dir",
        "-d",
        help="Path to the dbt project directory (containing dbt_project.yml).",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
    lookml_project_dir: Path = typer.Option(
        ...,
        "--lookml-project-dir",
        "-l",
        help="Path to the LookML project directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
    dbt_model: str = typer.Option(
        None,
        "--model",
        "-m",
        help="Sync a single dbt model, skipping the interactive menu."
    )
):
    """
    Synchronizes dbt model documentation and columns to LookML views.
    """
    console.log(f"Starting sync for dbt project at [bold]{dbt_project_dir}[/bold]")
    console.log(f"LookML project at [bold]{lookml_project_dir}[/bold]")

    syncer = DbtLookerSync(dbt_project_dir, lookml_project_dir)
    
    if dbt_model:
        sync_mode = show_preview_and_confirm(syncer, dbt_model)
        if sync_mode != "none":
            syncer.sync_models([dbt_model], sync_mode=sync_mode)
            console.log("[bold green]Sync complete![/bold green]")
        else:
            console.log("[yellow]Sync cancelled.[/yellow]")
        return

    candidates = syncer.get_sync_candidates()
    if not candidates:
        console.log("[bold yellow]No dbt models with matching LookML views found.[/bold yellow]")
        return

    console.print(Panel("[bold]Select a view to sync[/bold]", expand=False))
    console.print("  [cyan]0[/cyan]: Sync All")
    for i, (model_name, view_path, stats) in enumerate(candidates, 1):
        new_f = stats['new_fields']
        miss_d = stats['missing_docs']
        
        parts = []
        if new_f > 0:
            parts.append(f"[green]{new_f} new fields[/green]")
        if miss_d > 0:
            parts.append(f"[blue]{miss_d} missing docs[/blue]")
            
        if not parts:
            count_str = "[dim](No changes)[/dim]"
        else:
            count_str = f"({', '.join(parts)})"
            
        console.print(f"  [cyan]{i}[/cyan]: {view_path.name} {count_str} (dbt model: '{model_name}')")

    try:
        choice = console.input("\nEnter the number of the view to sync (e.g., '1' or '0' for all): ")
        selected_index = int(choice)

        from rich.prompt import Confirm, Prompt
        if selected_index == 0:
            models_to_sync = [model_name for model_name, _, _ in candidates]
            if Confirm.ask(f"Are you sure you want to sync all {len(models_to_sync)} models?", default=False):
                sync_mode = Prompt.ask(
                    "What would you like to sync for ALL models?",
                    choices=["both", "docs", "fields"],
                    default="both"
                )
                syncer.sync_models(models_to_sync, sync_mode=sync_mode)
                console.log("[bold green]Sync complete![/bold green]")
            else:
                console.log("[yellow]Sync cancelled.[/yellow]")
        elif 1 <= selected_index <= len(candidates):
            model_to_sync = candidates[selected_index - 1][0]
            sync_mode = show_preview_and_confirm(syncer, model_to_sync)
            if sync_mode != "none":
                syncer.sync_models([model_to_sync], sync_mode=sync_mode)
                console.log("[bold green]Sync complete![/bold green]")
            else:
                console.log("[yellow]Sync cancelled.[/yellow]")
        else:
            console.print("[bold red]Invalid selection.[/bold red]")

    except (ValueError, IndexError):
        console.print("[bold red]Invalid input. Please enter a valid number.[/bold red]")

    console.log("[bold green]Sync complete![/bold green]")

if __name__ == "__main__":
    app()