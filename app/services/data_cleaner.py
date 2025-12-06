from typing import Any, Dict


# Currently aligned to only stop searches but could be extended to be more generic
# and clean other data types as needed.
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
        if item.get("outcome") is False:
            item["outcome"] = "Nothing found"

        if item.get("type") == "Vehicle search":
            item["involved_person"] = False
        else:
            item["involved_person"] = True

        return item
