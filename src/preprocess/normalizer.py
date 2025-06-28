from datetime import datetime

class Normalizer:
    def __init__(self,):
        pass

    def normalize(self, data):
        # Implement normalization logic here

        # For example, you might want to convert date formats, handle missing values, etc.
        # This is a placeholder implementation
        normalized_data = {
            "title": data.get("title", [""])[0],
            "authors": [author.get("family", "") for author in data.get("author", [])],
            "published_date": data.get("issued", {}).get("date-parts", [[None]])[0][0],
            "doi": data.get("DOI", ""),
            "journal": data.get("container-title", [""])[0],
            "publisher": data.get("publisher", ""),
            "is_referenced_by_count": data.get("is-referenced-by-count", 0),
            "reference_count": data.get("reference-count", 0),
        }

        # Polyfill date
        date_parts = normalized_data["published_date"]
        if isinstance(date_parts, list):
            date_parts = date_parts[0] if date_parts else []
        elif isinstance(date_parts, str):
            date_parts = [int(part) for part in date_parts.split("-")]
        normalized_data["published_date"] = self.__polyfill_date(date_parts)

        # Merge authors into a single string
        normalized_data["authors"] = self.__merge_list(normalized_data["authors"], separator=", ")

        return normalized_data
    
    def __polyfill_date(self, date_parts) -> str:
        if not date_parts:
            return "1970-01-01"
        
        # dateparts is an array of year, month, day - fill missing parts with 01

        if not isinstance(date_parts, list):
            date_parts = [date_parts]

        year = date_parts[0]
        month = date_parts[1] if len(date_parts) > 1 else 1
        day = date_parts[2] if len(date_parts) > 2 else 1

        # sanitize year - if more than 12 months into the future, set to current year
        current_year = datetime.now().year
        if year > current_year + 1:
            year = current_year

        # sanitize month: 01 - 12 range
        if month < 1 or month > 12:
            month = 1

        # sanitize day: 01 - 31 range
        if day < 1 or day > 31:
            day = 1

        # format date as YYYY-MM-DD
        date = f"{year:04d}-{month:02d}-{day:02d}"

        return date
    

    def __merge_list(self, list, separator = ", ") -> str:
        """
        Merge a list into a string with a separator.
        """
        if not list:
            return ""
        if isinstance(list, str):
            return list
        
        return separator.join(list)
