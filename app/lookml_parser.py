from pathlib import Path
from typing import List, Dict, Any, Optional
import lkml


class LookMLParser:
    """
    Handles parsing and writing of LookML view files.
    """

    def __init__(self, lookml_project_dir: Path):
        self.lookml_project_dir = lookml_project_dir

    def find_lookml_files(self) -> List[Path]:
        """Finds all .view.lkml files in the project."""
        return list(self.lookml_project_dir.glob("**/*.view.lkml"))

    def load_lookml_file(self, file_path: Path) -> Dict[str, Any]:
        """Loads and parses a single LookML file."""
        with open(file_path, "r") as f:
            return lkml.load(f)

    def save_lookml_file(self, file_path: Path, lookml_data: Dict[str, Any]):
        """Saves LookML data back to a file."""
        with open(file_path, "w") as f:
            lkml.dump(lookml_data, f)

    def get_view_source(self, lookml_data: Dict[str, Any]) -> Optional[str]:
        """Extracts the sql_table_name or derived_table sql from a view."""
        if "views" not in lookml_data or not lookml_data["views"]:
            return None
        
        view = lookml_data["views"][0]
        
        # Check sql_table_name
        if "sql_table_name" in view:
            return str(view["sql_table_name"]).lower().strip('`').strip('"')
        
        # Check derived_table
        if "derived_table" in view and "sql" in view["derived_table"]:
            return str(view["derived_table"]["sql"]).lower()
            
        return None
