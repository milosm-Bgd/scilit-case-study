class Deduplicator:
    def __init__(self):
        self.seen = set()

    def deduplicate(self, data):
        """
        Deduplicate the data by doi field.
        """
        unique_data = []
        for item in data:
            doi = item.get("doi")
            if doi and doi not in self.seen:
                unique_data.append(item)
                self.seen.add(doi)
        
        return unique_data