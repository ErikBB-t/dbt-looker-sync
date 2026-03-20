from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple, Union
from rich.console import Console
from rich.prompt import Confirm, Prompt
import re
import subprocess

from app.dbt_parser import DbtParser
from app.lookml_parser import LookMLParser

console = Console()


class DbtLookerSync:
    def __init__(self, dbt_project_dir: Path, lookml_project_dir: Path):
        self.dbt_parser = DbtParser(dbt_project_dir)
        self.lookml_parser = LookMLParser(lookml_project_dir)
        self.lookml_files = self.lookml_parser.find_lookml_files()
        self.lookml_project_dir = lookml_project_dir
        self.branch_created = False
    
    def get_sync_candidates(self) -> List[Tuple[str, Path, Dict[str, int]]]:
        """
        Get a list of dbt models that have a matching LookML view file.
        Returns: List of (model_name, file_path, {'new_fields': x, 'missing_docs': y})
        """
        candidates = []
        dbt_models = self.dbt_parser.models
        if not self.lookml_files:
            return []

        for file_path in self.lookml_files:
            try:
                lookml_data = self.lookml_parser.load_lookml_file(file_path)
                source = self.lookml_parser.get_view_source(lookml_data)
                
                match_found = False
                model_to_check = None

                if not source:
                    for model_name in dbt_models.keys():
                        if file_path.name.split('.')[0] == model_name:
                            match_found = True
                            model_to_check = model_name
                            break
                else:
                    for model_name, model_data in dbt_models.items():
                        full_table = model_data["full_table_name"]
                        alias = model_data["alias"]
                        
                        if source == full_table or source == alias or source.endswith(f".{alias}"):
                            match_found = True
                            model_to_check = model_name
                            break
                        
                        if f"ref('{model_name}')" in source or f"ref('{alias}')" in source or model_name in source:
                            match_found = True
                            model_to_check = model_name
                            break

                if match_found and model_to_check:
                    view = lookml_data["views"][0]
                    stats = self._get_pending_stats(view, dbt_models[model_to_check])
                    candidates.append((model_to_check, file_path, stats))
                        
            except Exception as e:
                console.log(f"[yellow]Skipping {file_path.name} due to parsing error: {e}[/yellow]")

        return sorted(candidates, key=lambda x: x[2]['new_fields'] + x[2]['missing_docs'], reverse=True)

    def sync_models(self, models_to_sync: List[str]):
        """
        Syncs a list of specified dbt models to their corresponding LookML views.
        """
        dbt_models = self.dbt_parser.models
        all_candidates = self.get_sync_candidates()
        candidates_map = {name: path for name, path, _ in all_candidates}

        for model_name in models_to_sync:
            if model_name not in dbt_models:
                console.log(f"[bold red]Model '{model_name}' not found in dbt project.[/bold red]")
                continue
            
            dbt_model_data = dbt_models[model_name]
            lookml_view_file = candidates_map.get(model_name)

            if not lookml_view_file:
                console.log(f"No matching LookML view found for dbt model [bold]{model_name}[/bold]. Skipping.")
                continue

            console.log(f"Syncing dbt model [bold]{model_name}[/bold] with LookML view [bold]{lookml_view_file.name}[/bold]")
            self._sync_view(lookml_view_file, dbt_model_data, model_name)

    def _sync_view(self, lookml_view_file: Path, dbt_model_data: Dict[str, Any], model_name: str):
        try:
            lookml_data = self.lookml_parser.load_lookml_file(lookml_view_file)
            if "views" not in lookml_data or not lookml_data["views"]:
                console.log(f"[yellow]No views found in {lookml_view_file.name}. Skipping.[/yellow]")
                return

            view = lookml_data["views"][0]
            changes = self._get_pending_changes(view, dbt_model_data)
            
            if not changes:
                console.log(f"  [blue]No changes needed for {lookml_view_file.name}[/blue]")
                return

            console.print(f"\n[bold yellow]Pending changes for {lookml_view_file.name}:[/bold yellow]")
            for change in changes:
                console.print(f"  {change}")

            if Confirm.ask(f"\nApply these changes to [bold]{lookml_view_file.name}[/bold]?", default=False):
                if not self.branch_created:
                    self._ensure_git_safety(model_name)
                
                self._update_dimensions(view, dbt_model_data)
                self.lookml_parser.save_lookml_file(lookml_view_file, lookml_data)
                console.log(f"  [green]Successfully updated {lookml_view_file.name}[/green]")
            else:
                console.log(f"  [yellow]Skipped {lookml_view_file.name}[/yellow]")

        except Exception as e:
            console.log(f"[bold red]Error syncing {lookml_view_file.name}: {e}[/bold red]")

    def _ensure_git_safety(self, model_name: str):
        branch_result = subprocess.run(["git", "branch", "--show-current"], cwd=self.lookml_project_dir, capture_output=True, text=True)
        current_branch = branch_result.stdout.strip()
        
        if current_branch in ["main", "master"]:
            console.log(f"[blue]On production branch [bold]{current_branch}[/bold]. Running git pull...[/blue]")
            subprocess.run(["git", "pull"], cwd=self.lookml_project_dir, capture_output=True)
        else:
            console.print(f"[bold yellow]Warning: You are currently on branch '{current_branch}', not main/master.[/bold yellow]")
            if not Confirm.ask("Do you want to continue using this branch as base?", default=False):
                raise Exception("Sync aborted by user to switch branch.")

        status_result = subprocess.run(["git", "status", "--porcelain"], cwd=self.lookml_project_dir, capture_output=True, text=True)
        if status_result.stdout.strip():
            console.print("[bold yellow]Warning: You have uncommitted changes in your LookML repo.[/bold yellow]")
            if not Confirm.ask("Do you want to continue anyway?", default=False):
                raise Exception("Sync aborted by user due to uncommitted changes.")

        default_branch = f"dbt-sync-{model_name.replace('_', '-')}"
        branch_name = Prompt.ask("\nEnter name for new Git branch", default=default_branch)
        
        try:
            subprocess.run(["git", "checkout", "-b", branch_name], cwd=self.lookml_project_dir, check=True, capture_output=True)
            console.log(f"[green]Created branch [bold]{branch_name}[/bold].[/green]")
            self.branch_created = True
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode().strip()
            console.log(f"[bold red]Failed to create branch: {error_msg}[/bold red]")
            if not Confirm.ask("Continue on current branch?", default=False):
                raise Exception("Git branch creation failed.")

    def _get_column_to_field_map(self, view: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Maps dbt column names to LookML field objects by inspecting the 'sql' property."""
        col_map = {}
        # Scan all common field types in LookML
        field_types = ["dimensions", "measures", "dimension_groups", "filters", "parameters"]
        
        for ft in field_types:
            # Handle both list and single dict (lkml parser behavior)
            fields = view.get(ft, [])
            if isinstance(fields, dict):
                fields = [fields]
            elif not isinstance(fields, list):
                continue
                
            for field in fields:
                sql = str(field.get("sql", "")).lower()
                # Matches ${TABLE}.column_name, ${TABLE}.`column_name`, ${TABLE}."column_name", etc.
                # regex looks for ${table}. followed by optional quotes/backticks, then the column name
                match = re.search(r"\$\{table\}\.[\`\"\[]?([\w\d_]+)[\`\"\]]?", sql)
                if match:
                    col_name = match.group(1).lower()
                    if col_name not in col_map:
                        col_map[col_name] = field
        return col_map

    def _get_pending_stats(self, view: Dict[str, Any], dbt_model_data: Dict[str, Any]) -> Dict[str, int]:
        col_field_map = self._get_column_to_field_map(view)
        dbt_columns = dbt_model_data.get("columns", {})
        
        new_fields = 0
        missing_docs = 0
        
        for col_name, col_data in dbt_columns.items():
            if col_name in col_field_map:
                field = col_field_map[col_name]
                if col_data.get("description") and not field.get("description"):
                    missing_docs += 1
            else:
                new_fields += 1
                
        return {"new_fields": new_fields, "missing_docs": missing_docs}

    def _get_pending_changes(self, view: Dict[str, Any], dbt_model_data: Dict[str, Any]) -> List[str]:
        changes = []
        col_field_map = self._get_column_to_field_map(view)
        dbt_columns = dbt_model_data.get("columns", {})

        for col_name, col_data in dbt_columns.items():
            dbt_description = col_data.get("description")
            
            if col_name in col_field_map:
                field = col_field_map[col_name]
                if dbt_description and not field.get("description"):
                    changes.append(f"[blue]Add description[/blue] to field using [bold]{col_name}[/bold] (LookML name: {field.get('name')})")
            else:
                changes.append(f"[green]Add new dimension[/green] [bold]{col_name}[/bold]")

        return changes

    def _update_dimensions(self, view: Dict[str, Any], dbt_model_data: Dict[str, Any]) -> bool:
        updated = False
        col_field_map = self._get_column_to_field_map(view)
        dbt_columns = dbt_model_data.get("columns", {})

        for col_name, col_data in dbt_columns.items():
            dbt_description = col_data.get("description")
            
            if col_name in col_field_map:
                field = col_field_map[col_name]
                if dbt_description and not field.get("description"):
                    field["description"] = dbt_description
                    updated = True
            else:
                new_dim = self._create_new_dimension(col_data)
                if "dimensions" not in view:
                    view["dimensions"] = []
                view["dimensions"].append(new_dim)
                updated = True

        return updated

    def _create_new_dimension(self, dbt_column: Dict[str, Any]) -> Dict[str, Any]:
        col_name = dbt_column["name"]
        dim = {
            "name": col_name,
            "type": "string",
            "sql": f"${{TABLE}}.{col_name}",
        }
        if dbt_column.get("description"):
            dim["description"] = dbt_column["description"]
        return dim
