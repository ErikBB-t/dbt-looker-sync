import json
from pathlib import Path
from typing import Dict, Any, Optional


class DbtParser:
    """
    Parses dbt manifest.json file to extract model and column information.
    """

    def __init__(self, dbt_project_dir: Path):
        self.dbt_project_dir = dbt_project_dir
        self.manifest_path = self.dbt_project_dir / "target" / "manifest.json"
        if not self.manifest_path.exists():
            raise FileNotFoundError(
                f"manifest.json not found in {self.dbt_project_dir / 'target'}. "
                "Please run `dbt docs generate` in your dbt project."
            )
        self._dbt_models = None

    def _load_manifest(self) -> Dict[str, Any]:
        with open(self.manifest_path, "r") as f:
            return json.load(f)

    @property
    def models(self) -> Dict[str, Any]:
        if self._dbt_models is None:
            manifest = self._load_manifest()
            self._dbt_models = self._parse_models(manifest)
        return self._dbt_models

    def _parse_models(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        models = {}
        for node_name, node_info in manifest.get("nodes", {}).items():
            if node_info.get("resource_type") == "model":
                model_name = node_info.get("name")
                
                # Only include mrt models as requested
                if not model_name.startswith("mrt_"):
                    continue

                database = node_info.get("database")
                schema = node_info.get("schema")
                alias = node_info.get("alias", model_name)
                
                # BigQuery style full table name
                full_table_name = f"{database}.{schema}.{alias}".lower()
                
                columns = {}
                for col_name, col_info in node_info.get("columns", {}).items():
                    columns[col_name.lower()] = {
                        "name": col_info.get("name").lower(),
                        "description": col_info.get("description"),
                    }
                models[model_name] = {
                    "columns": columns,
                    "full_table_name": full_table_name,
                    "alias": alias.lower()
                }
        return models

    def get_model(self, model_name: str) -> Optional[Dict[str, Any]]:
        return self.models.get(model_name)
