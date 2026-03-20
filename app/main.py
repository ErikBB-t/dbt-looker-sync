import typer
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

from app.sync import DbtLookerSync

app = typer.Typer()
console = Console()


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
        syncer.sync_models([dbt_model])
        console.log("[bold green]Sync complete![/bold green]")
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

        if selected_index == 0:
            models_to_sync = [model_name for model_name, _, _ in candidates]
            syncer.sync_models(models_to_sync)
        elif 1 <= selected_index <= len(candidates):
            model_to_sync = candidates[selected_index - 1][0]
            syncer.sync_models([model_to_sync])
        else:
            console.print("[bold red]Invalid selection.[/bold red]")

    except (ValueError, IndexError):
        console.print("[bold red]Invalid input. Please enter a valid number.[/bold red]")

    console.log("[bold green]Sync complete![/bold green]")

if __name__ == "__main__":
    app()