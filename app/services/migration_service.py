import io
import csv
import zipfile
from datetime import datetime
from uuid import UUID
from ..repositories.social_repo import SocialRepository

class MigrationService:
    def __init__(self, social_repo: SocialRepository):
        self.social_repo = social_repo

    async def export_mambo_data(self, user_id: UUID) -> io.BytesIO:
        """Generate a ZIP file with movie history CSVs."""
        data = await self.social_repo.get_export_data(user_id)
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            # 1. Watched CSV
            watched_csv = self._dict_to_csv(data['watched'], ['date', 'title', 'release_date'])
            zip_file.writestr('watched.csv', watched_csv)

            # 2. Ratings CSV
            ratings_csv = self._dict_to_csv(data['ratings'], ['date', 'title', 'release_date', 'rating'])
            zip_file.writestr('ratings.csv', ratings_csv)

            # 3. Reviews CSV
            reviews_csv = self._dict_to_csv(data['reviews'], ['date', 'title', 'release_date', 'rating', 'body', 'is_spoiler'])
            zip_file.writestr('reviews.csv', reviews_csv)

        zip_buffer.seek(0)
        return zip_buffer

    def _dict_to_csv(self, data_list: list[dict], headers: list[str]) -> str:
        """Convert a list of dictionaries to a CSV string."""
        if not data_list:
            # Return just headers if no data
            return ",".join([h.capitalize() for h in headers]) + "\n"

        output = io.StringIO()
        writer = csv.writer(output)
        
        # Letterboxd-style headers (Capitalized)
        writer.writerow([h.replace('_', ' ').capitalize() for h in headers])
        
        for row in data_list:
            line = []
            for h in headers:
                val = row.get(h)
                # Format dates
                if isinstance(val, (datetime)):
                    val = val.strftime('%Y-%m-%d')
                # Format release year from date
                if h == 'release_date' and val:
                    if isinstance(val, str):
                        val = val.split('-')[0]
                    elif hasattr(val, 'year'):
                        val = val.year
                line.append(val)
            writer.writerow(line)
            
        return output.getvalue()
