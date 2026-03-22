from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple, Union
from rich.console import Console
from rich.prompt import Confirm, Prompt
import re
import subprocess
import lkml

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

    def sync_models(self, model_names: List[str], sync_mode: str = "both"):
        """
        Syncs a list of specified dbt models to their corresponding LookML views.
        sync_mode can be 'both', 'docs' (only descriptions), or 'fields' (only new dimensions).
        """
        dbt_models = self.dbt_parser.models
        candidates = self.get_sync_candidates()
        for model_name in model_names:
            match = next((c for c in candidates if c[0] == model_name), None)
            if match:
                _, file_path, _ = match
                dbt_model_data = dbt_models.get(model_name)
                if dbt_model_data:
                    self._sync_view(file_path, dbt_model_data, model_name, sync_mode)

    def _sync_view(self, lookml_view_file: Path, dbt_model_data: Dict[str, Any], model_name: str, sync_mode: str = "both"):
        try:
            with open(lookml_view_file, "r") as f:
                original_content = f.read()

            lookml_data = lkml.load(original_content)
            if "views" not in lookml_data or not lookml_data["views"]:
                console.log(f"[yellow]No views found in {lookml_view_file.name}. Skipping.[/yellow]")
                return

            view = lookml_data["views"][0]
            all_changes = self.get_structured_pending_changes(view, dbt_model_data)
            
            # Filter changes based on sync_mode
            changes = []
            if sync_mode == "both":
                changes = all_changes
            elif sync_mode == "docs":
                changes = [c for c in all_changes if c["action"] == "Add description"]
            elif sync_mode == "fields":
                changes = [c for c in all_changes if c["action"] == "Add new dimension"]

            if not changes:
                console.log(f"  [blue]No matching changes needed for {lookml_view_file.name} with mode '{sync_mode}'[/blue]")
                return

            if not self.branch_created:
                self._ensure_git_safety(model_name)
            
            new_content = self._apply_changes_surgically(original_content, changes)
            
            with open(lookml_view_file, "w") as f:
                f.write(new_content)
            console.log(f"  [green]Successfully updated {lookml_view_file.name} (mode: {sync_mode})[/green]")

        except Exception as e:
            console.log(f"[bold red]Error syncing {lookml_view_file.name}: {e}[/bold red]")
            import traceback
            console.print(traceback.format_exc())

    def _apply_changes_surgically(self, content: str, changes: List[Dict[str, str]]) -> str:
        new_content = content
        
        for c in changes:
            field_name = c["lookml_name"]
            description = c["description"]
            
            if not description:
                continue

            if c["action"] == "Add description":
                new_content = self._inject_description(new_content, field_name, description)
            else:
                new_content = self._inject_new_dimension(new_content, c["field"], description)
        
        return new_content

    def _inject_description(self, content: str, field_name: str, description: str) -> str:
        # Regex finds the dimension/measure block start
        # Supports dimension: name { OR measure: name {
        pattern = rf"(^\s*(?:dimension|measure|dimension_group|filter|parameter):\s*{field_name}\s*\{{)"
        match = re.search(pattern, content, re.MULTILINE)
        
        if match:
            start_pos = match.end()
            # Find the end of this specific block to avoid leaking into other blocks
            end_pos = self._find_block_end(content, start_pos)
            block_content = content[start_pos:end_pos]
            
            if "description:" not in block_content:
                safe_desc = description.replace('"', '\\"')
                insertion = f"\n    description: \"{safe_desc}\""
                return content[:start_pos] + insertion + content[start_pos:]
        
        return content

    def _inject_new_dimension(self, content: str, col_name: str, description: str) -> str:
        # Find the last closing brace of the view block
        # Usually the last '}' in a well-formatted .view.lkml file
        last_brace_index = content.rfind("}")
        if last_brace_index == -1:
            return content

        safe_desc = description.replace('"', '\\"')
        new_dim = f"\n  dimension: {col_name} {{\n    type: string\n    sql: ${{TABLE}}.{col_name} ;;\n    description: \"{safe_desc}\"\n  }}\n"
        
        return content[:last_brace_index] + new_dim + content[last_brace_index:]

    def _find_block_end(self, content: str, start_pos: int) -> int:
        brace_count = 1
        for i in range(start_pos, len(content)):
            if content[i] == "{":
                brace_count += 1
            elif content[i] == "}":
                brace_count -= 1
                if brace_count == 0:
                    return i
        return len(content)

    def _ensure_git_safety(self, model_name: str):
        branch_result = subprocess.run(["git", "branch", "--show-current"], cwd=self.lookml_project_dir, capture_output=True, text=True)
        current_branch = branch_result.stdout.strip()
        
        status_result = subprocess.run(["git", "status", "--porcelain"], cwd=self.lookml_project_dir, capture_output=True, text=True)
        has_uncommitted = bool(status_result.stdout.strip())

        if current_branch in ["main", "master"]:
            console.log(f"[blue]You are on production branch [bold]{current_branch}[/bold].[/blue]")
        
        if has_uncommitted:
            console.print("[bold yellow]Warning: You have uncommitted changes in your LookML repo.[/bold yellow]")

        # Ask the user what they want to do
        choice = Prompt.ask(
            "\nGit strategy",
            choices=["current", "new", "abort"],
            default="new"
        )

        if choice == "abort":
            raise Exception("Sync aborted by user.")
        
        if choice == "new":
            default_branch = f"dbt-sync-{model_name.replace('_', '-')}"
            branch_name = Prompt.ask("Enter name for new Git branch", default=default_branch)
            try:
                subprocess.run(["git", "checkout", "-b", branch_name], cwd=self.lookml_project_dir, check=True, capture_output=True)
                console.log(f"[green]Created and switched to branch [bold]{branch_name}[/bold].[/green]")
            except subprocess.CalledProcessError as e:
                error_msg = e.stderr.decode().strip()
                console.log(f"[bold red]Failed to create branch: {error_msg}[/bold red]")
                if not Confirm.ask("Continue on current branch instead?", default=False):
                    raise Exception("Git branch creation failed.")
        else:
            console.log(f"[blue]Continuing on current branch [bold]{current_branch}[/bold].[/blue]")
        
        self.branch_created = True

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

    def get_structured_pending_changes(self, view: Dict[str, Any], dbt_model_data: Dict[str, Any]) -> List[Dict[str, str]]:
        changes = []
        col_field_map = self._get_column_to_field_map(view)
        dbt_columns = dbt_model_data.get("columns", {})

        for col_name, col_data in dbt_columns.items():
            dbt_description = col_data.get("description", "")
            
            if col_name in col_field_map:
                field = col_field_map[col_name]
                if dbt_description and not field.get("description"):
                    changes.append({
                        "field": col_name,
                        "action": "Add description",
                        "description": dbt_description,
                        "lookml_name": field.get("name")
                    })
            else:
                changes.append({
                    "field": col_name,
                    "action": "Add new dimension",
                    "description": dbt_description,
                    "lookml_name": col_name
                })

        return changes

    def _get_pending_changes(self, view: Dict[str, Any], dbt_model_data: Dict[str, Any]) -> List[str]:
        changes = []
        structured_changes = self.get_structured_pending_changes(view, dbt_model_data)
        
        for c in structured_changes:
            if c["action"] == "Add description":
                changes.append(f"[blue]Add description[/blue] to field using [bold]{c['field']}[/bold] (LookML name: {c['lookml_name']})")
            else:
                changes.append(f"[green]Add new dimension[/green] [bold]{c['field']}[/bold]")

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
