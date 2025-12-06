from typing import Any, Dict


class DataCleaner:
    @staticmethod
    def clean(item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean up known data issues in the item. Add more rules as needed.
        """
        item = DataCleaner._fix_data_types(item)
        return item

    # Example remediation function, add more as required

    @staticmethod
    def _fix_data_types(item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fixes data types that don't match the schema or logic.
        """
        # The API returns boolean false for 'outcome' when nothing was found,
        # but our model expects a string.
        if item.get("outcome") is False:
            item["outcome"] = "Nothing found"

        # Ensure involved_person is consistent with type as per spec
        if item.get("type") == "Vehicle search":
            item["involved_person"] = False
        else:
            item["involved_person"] = True
            
        return item

