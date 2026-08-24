import os
import io
import re
import csv
import uuid
import zipfile
import asyncio
import logging
import httpx
import cloudscraper
from bs4 import BeautifulSoup
from datetime import datetime, date
from typing import Dict, Any, List, Optional, Callable
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.config import settings

logger = logging.getLogger('mambo.letterboxd_service')

# Global progress tracker: user_id -> {"processed": int, "total": int, "status": str}
sync_progress: Dict[str, Dict[str, Any]] = {}

class LetterboxdService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.tmdb_key = settings.tmdb_api_key

    @staticmethod
    def parse_stars(text: str) -> Optional[float]:
        if not text:
            return None
        count = text.count("★")
        if "½" in text:
            count += 0.5
        return count if count > 0 else None

    def _fetch_url(self, url: str, timeout: int = 25) -> Optional[str]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        scraper_key = getattr(settings, 'scraperapi_key', None) or os.getenv("SCRAPERAPI_KEY")
        if scraper_key:
            import requests
            try:
                params = {"api_key": scraper_key, "url": url, "keep_headers": "true", "render": "false"}
                resp = requests.get("https://api.scraperapi.com", params=params, headers=headers, timeout=timeout)
                if resp.status_code == 200 and resp.text:
                    return resp.text
            except Exception as e:
                logger.warning(f"ScraperAPI fetch failed for {url}: {e}")

        try:
            scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
            resp = scraper.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200 and resp.text:
                return resp.text
        except Exception:
            pass

        import requests
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200 and resp.text:
                return resp.text
        except Exception:
            pass

        return None

    async def fetch_profile(self, username: str) -> dict:
        """Fetches basic profile info using ScraperAPI / cloudscraper."""
        loop = asyncio.get_event_loop()
        try:
            def scrape():
                url = f"https://letterboxd.com/{username}/"
                res_text = self._fetch_url(url)
                if res_text:
                    return res_text

                # Fallback: Open RSS Feed
                try:
                    import requests
                    headers = {"User-Agent": "Mozilla/5.0"}
                    rss_url = f"https://letterboxd.com/{username}/rss/"
                    resp = requests.get(rss_url, headers=headers, timeout=12)
                    if resp.status_code == 200 and "<rss" in resp.text:
                        return {"is_rss": True, "xml": resp.text}
                except Exception:
                    pass

                return None

            html = await loop.run_in_executor(None, scrape)
            if not html:
                return {"success": False, "message": "Profile not found on Letterboxd."}

            if isinstance(html, dict) and html.get("is_rss"):
                xml_text = html.get("xml", "")
                display_name = username
                if "<title>Letterboxd - " in xml_text:
                    try:
                        display_name = xml_text.split("<title>Letterboxd - ")[1].split("</title>")[0].strip()
                    except Exception:
                        pass
                return {
                    "success": True,
                    "username": username,
                    "display_name": display_name,
                    "avatar_url": None,
                    "bio": None,
                    "stats": {},
                }

            soup = BeautifulSoup(html, "html.parser")
            name_el = soup.find("h1", class_="person-display-name")
            display_name = name_el.get_text(strip=True) if name_el else username

            avatar_el = soup.select_one(".profile-avatar img")
            avatar_url = avatar_el.get("src") if avatar_el else None

            bio_el = soup.select_one(".profile-bio")
            bio = bio_el.get_text(strip=True) if bio_el else None

            stats = {}
            for h4 in soup.select("h4.profile-statistic"):
                text_stat = h4.get_text(strip=True)
                m = re.match(r"^([\d,]+)(.+)$", text_stat)
                if m:
                    val = int(m.group(1).replace(",", ""))
                    label = m.group(2).strip().lower()
                    stats[label] = val

            return {
                "success": True,
                "username": username,
                "display_name": display_name,
                "avatar_url": avatar_url,
                "bio": bio,
                "stats": stats
            }
        except Exception as e:
            logger.error(f"Error scraping profile for {username}: {e}")
            return {"success": False, "message": f"Error contacting Letterboxd: {str(e)}"}

    async def get_tmdb_id_from_letterboxd(self, client: httpx.AsyncClient, slug: str) -> Optional[int]:
        """Scrapes the TMDB ID from a Letterboxd film details page."""
        url = f"https://letterboxd.com/film/{slug}/"
        try:
            resp = await client.get(url, timeout=10)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                body_tag = soup.find("body")
                if body_tag and body_tag.has_attr("data-tmdb-id"):
                    tmdb_id = body_tag.get("data-tmdb-id")
                    if tmdb_id and tmdb_id.isdigit():
                        return int(tmdb_id)
        except Exception as e:
            logger.warning(f"Letterboxd fallback scrape failed for slug {slug}: {e}")
        return None

    async def search_tmdb_for_movie(self, client: httpx.AsyncClient, title: str, year: Optional[str]) -> Optional[int]:
        """Searches TMDB API for a movie by title and year, with fallback to multi search."""
        if not self.tmdb_key:
            return None

        # 1. Search Movie specifically (Letterboxd is movie-centric)
        url_movie = "https://api.themoviedb.org/3/search/movie"
        params_movie = {
            "api_key": self.tmdb_key,
            "query": title,
            "language": "en-US",
            "page": 1,
        }
        if year:
            params_movie["primary_release_year"] = year

        try:
            resp = await client.get(url_movie, params=params_movie, timeout=10)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    for res in results:
                        res_title = res.get("title") or ""
                        if res_title.lower() == title.lower():
                            return res.get("id")
                    # Return first search result
                    return results[0].get("id")
        except Exception as e:
            logger.warning(f"TMDB movie search failed for {title}: {e}")

        # 2. Fallback to Multi Search (to support mini-series logged on Letterboxd)
        url_multi = "https://api.themoviedb.org/3/search/multi"
        params_multi = {
            "api_key": self.tmdb_key,
            "query": title,
            "language": "en-US",
            "page": 1,
        }
        try:
            resp = await client.get(url_multi, params=params_multi, timeout=10)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    for res in results:
                        media_type = res.get("media_type")
                        if media_type not in ("movie", "tv"):
                            continue
                        res_title = res.get("title") or res.get("name") or ""
                        if res_title.lower() == title.lower():
                            return res.get("id")
                    valid = [r for r in results if r.get("media_type") in ("movie", "tv")]
                    if valid:
                        return valid[0].get("id")
        except Exception as e:
            logger.warning(f"TMDB multi search failed for {title}: {e}")
        return None

    async def fetch_tmdb_details(self, client: httpx.AsyncClient, tmdb_id: int) -> Optional[dict]:
        """Fetches full movie or show details from TMDB to determine type & basic metadata."""
        if not self.tmdb_key:
            return None
        
        # Try Movie details first
        try:
            resp = await client.get(f"https://api.themoviedb.org/3/movie/{tmdb_id}", params={"api_key": self.tmdb_key}, timeout=8)
            if resp.status_code == 200:
                d = resp.json()
                d["content_type"] = "movie"
                return d
        except Exception:
            pass

        # Try TV details next
        try:
            resp = await client.get(f"https://api.themoviedb.org/3/tv/{tmdb_id}", params={"api_key": self.tmdb_key}, timeout=8)
            if resp.status_code == 200:
                d = resp.json()
                d["content_type"] = "series"  # default to series inside MAMBO
                # Check genres for Anime keyword
                genres = [g.get("name", "").lower() for g in d.get("genres", [])]
                if "animation" in genres and d.get("original_language") == "ja":
                    d["content_type"] = "anime"
                return d
        except Exception:
            pass

        return None

    async def import_scraped_data(self, user_id: str, data: dict):
        """Processes the scraped/ZIP data, updates database records and sync progress."""
        logger.info(f"Starting import process for user {user_id}")
        user_uuid = uuid.UUID(user_id)
        
        # Check cancellation before starting
        if sync_progress.get(user_id, {}).get("cancelled"):
            logger.info(f"Import cancelled by user {user_id} before import_scraped_data start")
            return

        # 1. Gather all slugs/items
        films = data.get("films", [])
        reviews = data.get("reviews", [])
        watchlist = data.get("watchlist", [])
        
        liked_films = data.get("liked_films", [])
        liked_set = set(liked_films)

        # Ensure all reviewed films exist in films list so they are processed
        films_slugs = {f["slug"] for f in films if f.get("slug")}
        for r in reviews:
            r_slug = r.get("slug")
            if r_slug and r_slug not in films_slugs:
                films.append({
                    "slug": r_slug,
                    "rating": r.get("rating"),
                    "watch_date": r.get("watch_date")
                })
                films_slugs.add(r_slug)

        # Ensure all custom list items exist in films list so they are processed
        custom_lists = data.get("custom_lists", [])
        for cl in custom_lists:
            for item in cl.get("items", []):
                cl_slug = item.get("slug") if isinstance(item, dict) else item
                if cl_slug and cl_slug not in films_slugs:
                    films.append({
                        "slug": cl_slug,
                        "rating": None,
                        "watch_date": None
                    })
                    films_slugs.add(cl_slug)

        total_items = len(films) + len(reviews) + len(watchlist) + len(custom_lists)
        
        # Double check cancellation
        if sync_progress.get(user_id, {}).get("cancelled"):
            logger.info(f"Import cancelled by user {user_id} before initializing progress")
            return

        sync_progress[user_id] = {
            "processed": 0,
            "total": total_items,
            "status": "running",
            "current_item": None,
            "recent_items": [],
            "imported_films": 0,
            "imported_reviews": 0,
            "unresolved_count": 0,
            "skipped_count": 0,
            "cancelled": False
        }

        # 2. Get Watched & Watchlist Collection IDs
        res = await self.db.execute(text(
            "SELECT id FROM public.collections WHERE user_id = :uid AND collection_type = 'watched' LIMIT 1"
        ), {"uid": user_uuid})
        watched_row = res.fetchone()
        watched_coll_id = watched_row[0] if watched_row else uuid.uuid4()
        if not watched_row:
            await self.db.execute(text("""
                INSERT INTO public.collections (id, user_id, name, description, is_public, collection_type, is_default, is_deletable, is_pinned, pin_order)
                VALUES (:id, :uid, 'Watched', 'Content I have watched', false, 'watched', true, false, true, 3)
            """), {"id": watched_coll_id, "uid": user_uuid})
            await self.db.commit()

        res = await self.db.execute(text(
            "SELECT id FROM public.collections WHERE user_id = :uid AND collection_type = 'watchlist' LIMIT 1"
        ), {"uid": user_uuid})
        watchlist_row = res.fetchone()
        watchlist_coll_id = watchlist_row[0] if watchlist_row else uuid.uuid4()
        if not watchlist_row:
            await self.db.execute(text("""
                INSERT INTO public.collections (id, user_id, name, description, is_public, collection_type, is_default, is_deletable, is_pinned, pin_order)
                VALUES (:id, :uid, 'Watchlist', 'Content I want to watch', false, 'watchlist', true, false, true, 1)
            """), {"id": watchlist_coll_id, "uid": user_uuid})
            await self.db.commit()

        # Map reviews by slug — also index without year suffix for fuzzy matching
        reviews_map = {}
        for r in reviews:
            r_slug = r.get("slug")
            if not r_slug:
                continue
            reviews_map[r_slug] = r
            # Also index slug without trailing year (e.g. "the-bride-2025" → "the-bride")
            parts = r_slug.split("-")
            if len(parts) > 1 and parts[-1].isdigit() and len(parts[-1]) == 4:
                base_slug = "-".join(parts[:-1])
                if base_slug not in reviews_map:
                    reviews_map[base_slug] = r

        # 3. Process Films (Watched & Rated)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        async with httpx.AsyncClient(headers=headers) as client:
            # A. Batch pre-resolve titles and slugs from DB to avoid N+1 queries
            slug_to_meta = {}
            for film in films:
                slug = film.get("slug")
                if not slug:
                    continue
                parts = slug.split("-")
                year = None
                if len(parts) > 1 and parts[-1].isdigit() and len(parts[-1]) == 4:
                    year = parts[-1]
                    parts = parts[:-1]
                clean_title = " ".join(parts)
                slug_to_meta[slug] = {"clean_title": clean_title, "year": year}

            for item in watchlist:
                slug = item if isinstance(item, str) else item.get("slug")
                if not slug or slug in slug_to_meta:
                    continue
                parts = slug.split("-")
                year = None
                if len(parts) > 1 and parts[-1].isdigit() and len(parts[-1]) == 4:
                    year = parts[-1]
                    parts = parts[:-1]
                clean_title = " ".join(parts)
                slug_to_meta[slug] = {"clean_title": clean_title, "year": year}

            # Batch query existing content
            existing_titles = list({m["clean_title"].lower() for m in slug_to_meta.values()})
            db_content_map = {}
            for i in range(0, len(existing_titles), 100):
                chunk = existing_titles[i:i+100]
                # Use = ANY(:titles) — asyncpg cannot bind a tuple to IN :param
                res = await self.db.execute(text("""
                    SELECT id, LOWER(title) as l_title, content_type, total_episodes 
                    FROM public.content 
                    WHERE LOWER(title) = ANY(:titles)
                """), {"titles": list(chunk)})
                for row in res.fetchall():
                    db_content_map[row[1]] = {
                        "id": row[0],
                        "content_type": row[2],
                        "total_episodes": row[3]
                    }

            # Identify missing slugs
            missing_slugs = [slug for slug in slug_to_meta if slug_to_meta[slug]["clean_title"].lower() not in db_content_map]

            # Resolve missing TMDB details concurrently (SQLAlchemy AsyncSession safe!)
            semaphore = asyncio.Semaphore(5)
            async def resolve_metadata(s, m):
                if sync_progress.get(user_id, {}).get("cancelled"):
                    return None
                async with semaphore:
                    try:
                        tmdb_id = await self.search_tmdb_for_movie(client, m["clean_title"], m["year"])
                        if not tmdb_id:
                            tmdb_id = await self.get_tmdb_id_from_letterboxd(client, s)
                        if not tmdb_id:
                            return None
                        details = await self.fetch_tmdb_details(client, tmdb_id)
                        if not details:
                            return None
                        return {"slug": s, "tmdb_id": tmdb_id, "details": details}
                    except Exception:
                        return None

            resolved_map = {}
            if missing_slugs:
                # Check cancellation before starting tasks
                if sync_progress.get(user_id, {}).get("cancelled"):
                    return
                sync_progress[user_id]["current_item"] = "Pre-resolving TMDB titles..."
                tasks = [resolve_metadata(s, slug_to_meta[s]) for s in missing_slugs]
                http_results = await asyncio.gather(*tasks)
                
                # Write newly resolved content rows sequentially (Safe for SQLAlchemy session)
                for r in http_results:
                    if sync_progress.get(user_id, {}).get("cancelled"):
                        return
                    if not r:
                        continue
                    s = r["slug"]
                    tmdb_id = r["tmdb_id"]
                    details = r["details"]
                    
                    res_tmdb = await self.db.execute(text(
                        "SELECT id, content_type, total_episodes FROM public.content WHERE tmdb_id = :tmdb_id LIMIT 1"
                    ), {"tmdb_id": tmdb_id})
                    tmdb_row = res_tmdb.fetchone()
                    if tmdb_row:
                        resolved_map[s] = {
                            "id": tmdb_row[0],
                            "content_type": tmdb_row[1],
                            "total_episodes": tmdb_row[2]
                        }
                    else:
                        content_id = uuid.uuid4()
                        content_type = details["content_type"]
                        meta = slug_to_meta[s]
                        title = details.get("title") or details.get("name") or meta["clean_title"]
                        synopsis = details.get("overview")
                        poster_path = details.get("poster_path")
                        poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
                        backdrop_path = details.get("backdrop_path")
                        backdrop_url = f"https://image.tmdb.org/t/p/original{backdrop_path}" if backdrop_path else None
                        release_date_str = details.get("release_date") or details.get("first_air_date")
                        release_date = None
                        if release_date_str:
                            try:
                                release_date = datetime.strptime(release_date_str, "%Y-%m-%d").date()
                            except Exception:
                                pass
                        genres = [g.get("name") for g in details.get("genres", []) if g.get("name")]
                        vote_count = details.get("vote_count", 0)
                        ext_rating = details.get("vote_average", 0.0)
                        total_episodes = details.get("number_of_episodes", 0)
                        total_seasons = details.get("number_of_seasons", 1)
                        
                        await self.db.execute(text("""
                            INSERT INTO public.content (
                                id, title, tmdb_id, content_type, poster_url, backdrop_url, 
                                synopsis, release_date, genres, original_title, original_language,
                                vote_count, external_rating, is_permanent, total_episodes, total_seasons, last_synced_at, created_at
                            ) VALUES (
                                :id, :title, :tmdb_id, :type, :poster_url, :backdrop_url,
                                :synopsis, :release_date, :genres, :title, :lang,
                                :vote_count, :ext_rating, true, :ep, :seasons, now(), now()
                            )
                        """), {
                            "id": content_id, "title": title, "tmdb_id": tmdb_id, "type": content_type,
                            "poster_url": poster_url, "backdrop_url": backdrop_url, "synopsis": synopsis,
                            "release_date": release_date, "genres": genres, "lang": details.get("original_language", "en"),
                            "vote_count": vote_count, "ext_rating": ext_rating, "ep": total_episodes, "seasons": total_seasons
                        })
                        
                        await self.db.execute(text("""
                            INSERT INTO public.letterboxd_slug_map (slug, content_id)
                            VALUES (:slug, :cid) ON CONFLICT DO NOTHING
                        """), {"slug": s, "cid": content_id})
                        
                        resolved_map[s] = {
                            "id": content_id,
                            "content_type": content_type,
                            "total_episodes": total_episodes
                        }
                await self.db.commit()

            # B. Loop and import watch history (instantly using pre-resolved caches!)
            processed_reviews_cids = set()
            for idx, film in enumerate(films):
                # Stop early if user cancelled
                if sync_progress.get(user_id, {}).get("cancelled"):
                    logger.info(f"Import cancelled by user {user_id} during film loop")
                    return

                slug = film.get("slug")
                if not slug:
                    sync_progress[user_id]["processed"] += 1
                    continue
                
                sync_progress[user_id]["current_item"] = slug.replace("-", " ").title()
                
                content_id = None
                content_type = "movie"
                total_episodes = 0
                
                if slug in resolved_map:
                    content_id = resolved_map[slug]["id"]
                    content_type = resolved_map[slug]["content_type"]
                    total_episodes = resolved_map[slug]["total_episodes"]
                else:
                    meta = slug_to_meta.get(slug, {})
                    db_item = db_content_map.get(meta.get("clean_title", "").lower())
                    if db_item:
                        content_id = db_item["id"]
                        content_type = db_item["content_type"]
                        total_episodes = db_item["total_episodes"]
                
                if not content_id:
                    sync_progress[user_id]["unresolved_count"] += 1
                    sync_progress[user_id]["processed"] += 1
                    continue

                try:
                    display_title = slug.replace("-", " ").title()

                    # B. Check existing watch logs
                    res = await self.db.execute(text(
                        "SELECT id FROM public.watch_history WHERE user_id = :uid AND content_id = :cid LIMIT 1"
                    ), {"uid": user_uuid, "cid": content_id})
                    wh_row = res.fetchone()
                    
                    # Convert Letterboxd rating (0.5 - 5.0) to MAMBO (1 - 10)
                    rating_val = film.get("rating")
                    star_rating = None
                    rating = None
                    if rating_val and rating_val > 0:
                        star_rating = int(rating_val * 2)
                        rating = float(rating_val * 2)

                    wh_id = None
                    if wh_row:
                        wh_id = wh_row[0]
                        sync_progress[user_id]["skipped_count"] += 1
                    else:
                        wh_id = uuid.uuid4()
                        watched_at = film.get("watch_date") or date.today()
                        if isinstance(watched_at, str):
                            try:
                                watched_at = datetime.strptime(watched_at, "%Y-%m-%d").date()
                            except Exception:
                                watched_at = date.today()
                        await self.db.execute(text("""
                            INSERT INTO public.watch_history (id, user_id, content_id, watched_at, watch_type, rating, imported_from)
                            VALUES (:id, :uid, :cid, :wat, 'first_watch', :rating, 'letterboxd')
                        """), {"id": wh_id, "uid": user_uuid, "cid": content_id, "wat": watched_at, "rating": rating})
                        sync_progress[user_id]["imported_films"] += 1

                    # C. Check existing review
                    review_item = reviews_map.get(slug)
                    if not review_item:
                        parts = slug.split("-")
                        if len(parts) > 1 and parts[-1].isdigit() and len(parts[-1]) == 4:
                            review_item = reviews_map.get("-".join(parts[:-1]))
                    if review_item and content_id not in processed_reviews_cids:
                        res = await self.db.execute(text(
                            "SELECT id FROM public.reviews WHERE user_id = :uid AND content_id = :cid LIMIT 1"
                        ), {"uid": user_uuid, "cid": content_id})
                        if not res.fetchone():
                            processed_reviews_cids.add(content_id)
                            review_id = uuid.uuid4()
                            text_review = review_item.get("review_text")
                            
                            # Parse watch date or fallback to film watch date/today
                            review_date = review_item.get("watch_date") or film.get("watch_date") or date.today()
                            if isinstance(review_date, str):
                                try:
                                    review_date = datetime.strptime(review_date, "%Y-%m-%d").date()
                                except Exception:
                                    review_date = date.today()
                            review_dt = datetime.combine(review_date, datetime.min.time()) if isinstance(review_date, date) else datetime.now()
                            
                            await self.db.execute(text("""
                                INSERT INTO public.reviews (
                                    id, user_id, content_id, rating, star_rating, text_review, contains_spoiler, is_spoiler, watch_history_id, created_at, updated_at, imported_from
                                ) VALUES (
                                    :id, :uid, :cid, :rating, :star_rating, :text_review, false, false, :wid, :cat, :uat, 'letterboxd'
                                )
                            """), {
                                "id": review_id, "uid": user_uuid, "cid": content_id, "rating": rating,
                                "star_rating": star_rating, "text_review": text_review, "wid": wh_id,
                                "cat": review_dt, "uat": review_dt
                            })
                            # Link review to watch history
                            await self.db.execute(text(
                                "UPDATE public.watch_history SET review_id = :rid WHERE id = :wid"
                            ), {"rid": review_id, "wid": wh_id})

                    # D. Update user_content_status
                    res = await self.db.execute(text(
                        "SELECT id FROM public.user_content_status WHERE user_id = :uid AND content_id = :cid LIMIT 1"
                    ), {"uid": user_uuid, "cid": content_id})
                    ucs_row = res.fetchone()
                    
                    is_liked = slug in liked_set
                    if ucs_row:
                        # Do not overwrite MAMBO rating if already rated
                        await self.db.execute(text("""
                            UPDATE public.user_content_status 
                            SET is_watched = true, is_dropped = false, is_interested = false, status = 'completed', 
                                rating = COALESCE(rating, :rating),
                                is_liked = :is_liked,
                                progress_episodes = CASE WHEN :type IN ('series', 'anime') THEN :ep ELSE progress_episodes END,
                                last_watched_at = COALESCE(last_watched_at, now()), 
                                last_activity_at = now()
                            WHERE id = :id
                        """), {"id": ucs_row[0], "rating": rating, "is_liked": is_liked, "type": content_type, "ep": total_episodes})
                    else:
                        await self.db.execute(text("""
                            INSERT INTO public.user_content_status (
                                id, user_id, content_id, is_watched, is_liked, status, watch_count, rating, 
                                progress_episodes, first_watched_at, last_watched_at, created_at, updated_at, last_activity_at, imported_from
                            ) VALUES (
                                gen_random_uuid(), :uid, :cid, true, :is_liked, 'completed', 1, :rating, 
                                :ep, now(), now(), now(), now(), now(), 'letterboxd'
                            )
                        """), {"uid": user_uuid, "cid": content_id, "is_liked": is_liked, "rating": rating, "ep": total_episodes if content_type in ("series", "anime") else 0})

                    # E. Add to Watched collection
                    await self.db.execute(text("""
                        INSERT INTO public.collection_items (collection_id, content_id, added_by, imported_from)
                        VALUES (:coll_id, :cid, :uid, 'letterboxd') ON CONFLICT DO NOTHING
                    """), {"coll_id": watched_coll_id, "cid": content_id, "uid": user_uuid})

                    # Commit chunk
                    await self.db.commit()

                    # Update live feed tracking
                    has_review = review_item is not None
                    recent_entry = {
                        "title": display_title,
                        "rating": rating / 2 if rating else None,  # convert back to 0-5 stars
                        "has_review": has_review,
                    }
                    sync_progress[user_id]["recent_items"].append(recent_entry)
                    if len(sync_progress[user_id]["recent_items"]) > 15:
                        sync_progress[user_id]["recent_items"].pop(0)
                    sync_progress[user_id]["imported_films"] += 1
                    if has_review:
                        sync_progress[user_id]["imported_reviews"] += 1

                except Exception as e:
                    logger.error(f"Error importing film {slug} for user {user_id}: {e}")
                    await self.db.rollback()

                sync_progress[user_id]["processed"] += 1

            # 4. Process Watchlist
            for item in watchlist:
                # Stop early if user cancelled
                if sync_progress.get(user_id, {}).get("cancelled"):
                    logger.info(f"Import cancelled by user {user_id} during watchlist loop")
                    return

                slug = item if isinstance(item, str) else item.get("slug")
                if not slug:
                    continue

                try:
                    content_id = None
                    if slug in resolved_map:
                        content_id = resolved_map[slug]["id"]
                    else:
                        meta = slug_to_meta.get(slug, {})
                        db_item = db_content_map.get(meta.get("clean_title", "").lower())
                        if db_item:
                            content_id = db_item["id"]
                    
                    if not content_id:
                        parts = slug.split("-")
                        year = None
                        if len(parts) > 1 and parts[-1].isdigit() and len(parts[-1]) == 4:
                            year = parts[-1]
                            parts = parts[:-1]
                        clean_title = " ".join(parts)

                        # Prefer movies first when matching local content from Letterboxd
                        res = await self.db.execute(text("""
                            SELECT id 
                            FROM public.content 
                            WHERE LOWER(title) = :t 
                            ORDER BY CASE WHEN content_type = 'movie' THEN 0 ELSE 1 END ASC 
                            LIMIT 1
                        """), {"t": clean_title.lower()})
                        content_row = res.fetchone()
                        
                        if content_row:
                            content_id = content_row[0]
                        else:
                            tmdb_id = await self.search_tmdb_for_movie(client, clean_title, year)
                            if tmdb_id:
                                # Double check if content with this tmdb_id already exists
                                res_tmdb = await self.db.execute(text(
                                    "SELECT id FROM public.content WHERE tmdb_id = :tmdb_id LIMIT 1"
                                ), {"tmdb_id": tmdb_id})
                                tmdb_row = res_tmdb.fetchone()
                                if tmdb_row:
                                    content_id = tmdb_row[0]
                                else:
                                    details = await self.fetch_tmdb_details(client, tmdb_id)
                                    if details:
                                        content_id = uuid.uuid4()
                                        content_type = details["content_type"]
                                        title = details.get("title") or details.get("name") or clean_title
                                        synopsis = details.get("overview")
                                        poster_path = details.get("poster_path")
                                        poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
                                        backdrop_path = details.get("backdrop_path")
                                        backdrop_url = f"https://image.tmdb.org/t/p/original{backdrop_path}" if backdrop_path else None
                                        release_date_str = details.get("release_date") or details.get("first_air_date")
                                        release_date = None
                                        if release_date_str:
                                            try:
                                                release_date = datetime.strptime(release_date_str, "%Y-%m-%d").date()
                                            except Exception:
                                                pass
                                        genres = [g.get("name") for g in details.get("genres", []) if g.get("name")]
                                        vote_count = details.get("vote_count", 0)
                                        ext_rating = details.get("vote_average", 0.0)

                                        await self.db.execute(text("""
                                            INSERT INTO public.content (
                                                id, title, tmdb_id, content_type, poster_url, backdrop_url, 
                                                synopsis, release_date, genres, original_title, original_language,
                                                vote_count, external_rating, is_permanent, last_synced_at, created_at
                                            ) VALUES (
                                                :id, :title, :tmdb_id, :type, :poster_url, :backdrop_url,
                                                :synopsis, :release_date, :genres, :title, :lang,
                                                :vote_count, :ext_rating, true, now(), now()
                                            )
                                        """), {
                                            "id": content_id, "title": title, "tmdb_id": tmdb_id, "type": content_type,
                                            "poster_url": poster_url, "backdrop_url": backdrop_url, "synopsis": synopsis,
                                            "release_date": release_date, "genres": genres, "lang": details.get("original_language", "en"),
                                            "vote_count": vote_count, "ext_rating": ext_rating
                                        })
                                        await self.db.commit()

                    if content_id:
                        # Check status
                        res = await self.db.execute(text(
                            "SELECT id, status FROM public.user_content_status WHERE user_id = :uid AND content_id = :cid LIMIT 1"
                        ), {"uid": user_uuid, "cid": content_id})
                        status_row = res.fetchone()
                        
                        if not status_row:
                            await self.db.execute(text("""
                                INSERT INTO public.user_content_status (
                                    id, user_id, content_id, is_watched, status, created_at, updated_at, last_activity_at, imported_from
                                ) VALUES (
                                    gen_random_uuid(), :uid, :cid, false, 'plan_to_watch', now(), now(), now(), 'letterboxd'
                                )
                            """), {"uid": user_uuid, "cid": content_id})
                            
                        # Add to Watchlist collection items
                        await self.db.execute(text("""
                            INSERT INTO public.collection_items (collection_id, content_id, added_by, imported_from)
                            VALUES (:coll_id, :cid, :uid, 'letterboxd') ON CONFLICT DO NOTHING
                        """), {"coll_id": watchlist_coll_id, "cid": content_id, "uid": user_uuid})
                        await self.db.commit()

                except Exception as e:
                    logger.error(f"Error importing watchlist item {slug} for user {user_id}: {e}")
                    await self.db.rollback()

                sync_progress[user_id]["processed"] += 1

        # 4.5 Process Custom Lists (Letterboxd Lists -> MAMBO Collections)
        if custom_lists:
            sync_progress[user_id]["current_item"] = "Importing custom lists..."
            for cl in custom_lists:
                if sync_progress.get(user_id, {}).get("cancelled"):
                    logger.info(f"Import cancelled by user {user_id} during custom lists import")
                    return
                
                list_name = cl.get("name", "Letterboxd List").strip()
                list_desc = cl.get("description") or f"Imported from Letterboxd list: {list_name}"
                cl_items = cl.get("items", [])
                if not cl_items or not list_name:
                    continue

                try:
                    res = await self.db.execute(text("""
                        SELECT id FROM public.collections 
                        WHERE user_id = :uid AND LOWER(name) = LOWER(:name) LIMIT 1
                    """), {"uid": user_uuid, "name": list_name})
                    c_row = res.fetchone()
                    
                    if c_row:
                        coll_id = c_row[0]
                    else:
                        coll_id = uuid.uuid4()
                        await self.db.execute(text("""
                            INSERT INTO public.collections (
                                id, user_id, name, description, is_public, visibility, collection_type, 
                                is_default, is_deletable, is_pinned, pin_order, item_count, created_at, updated_at
                            ) VALUES (
                                :id, :uid, :name, :desc, true, 'public', 'custom', 
                                false, true, false, 0, 0, now(), now()
                            )
                        """), {"id": coll_id, "uid": user_uuid, "name": list_name, "desc": list_desc})
                        await self.db.commit()

                    for item in cl_items:
                        s = item.get("slug") if isinstance(item, dict) else item
                        note_text = f"Rank #{item.get('position')}" if isinstance(item, dict) and item.get("position") else "Imported from Letterboxd"
                        if not s:
                            continue
                        
                        cid = None
                        if s in resolved_map:
                            cid = resolved_map[s]["id"]
                        else:
                            meta = slug_to_meta.get(s, {})
                            db_item = db_content_map.get(meta.get("clean_title", "").lower())
                            if db_item:
                                cid = db_item["id"]
                        
                        if not cid:
                            parts = s.split("-")
                            clean_title = " ".join(parts[:-1]) if (len(parts) > 1 and parts[-1].isdigit() and len(parts[-1]) == 4) else " ".join(parts)
                            res_c = await self.db.execute(text(
                                "SELECT id FROM public.content WHERE LOWER(title) = :t LIMIT 1"
                            ), {"t": clean_title.lower()})
                            c_found = res_c.fetchone()
                            if c_found:
                                cid = c_found[0]

                        if cid:
                            await self.db.execute(text("""
                                INSERT INTO public.collection_items (collection_id, content_id, added_by, note, imported_from)
                                VALUES (:coll_id, :cid, :uid, :note, 'letterboxd') ON CONFLICT DO NOTHING
                            """), {"coll_id": coll_id, "cid": cid, "uid": user_uuid, "note": note_text})
                    
                    await self.db.commit()
                except Exception as cl_err:
                    logger.error(f"Error creating collection for Letterboxd list {list_name}: {cl_err}")
                    await self.db.rollback()

                sync_progress[user_id]["processed"] += 1

        # 5. Final Stats recalculation
        try:
            await self.db.execute(text("""
                UPDATE public.user_stats 
                SET total_watched = (SELECT COUNT(DISTINCT content_id) FROM public.watch_history WHERE user_id = :uid),
                    total_reviews = (SELECT COUNT(*) FROM public.reviews WHERE user_id = :uid AND is_deleted = false),
                    total_posts = (SELECT COUNT(*) FROM public.reviews WHERE user_id = :uid AND is_deleted = false) + COALESCE((SELECT COUNT(*) FROM public.posts WHERE user_id = :uid), 0),
                    updated_at = now()
                WHERE user_id = :uid
            """), {"uid": user_uuid})
            
            # Recalculate item_count for all collections of this user
            await self.db.execute(text("""
                UPDATE public.collections c
                SET item_count = (SELECT COUNT(*) FROM public.collection_items WHERE collection_id = c.id),
                    updated_at = now()
                WHERE c.user_id = :uid
            """), {"uid": user_uuid})

            # Set status to completed in profiles table
            await self.db.execute(text("""
                UPDATE public.profiles 
                SET letterboxd_import_status = 'completed', updated_at = now()
                WHERE id = :uid
            """), {"uid": user_uuid})
            
            await self.db.commit()
            sync_progress[user_id]["status"] = "completed"
            logger.info(f"Import process successfully completed for user {user_id}")

        except Exception as e:
            logger.error(f"Error finishing import stats for user {user_id}: {e}")
            await self.db.rollback()
            sync_progress[user_id]["status"] = "failed"

    async def run_sync_in_background(self, user_id: str, username: str):
        """Task runner for scraping Letterboxd page by page and importing."""
        from app.core.database import AsyncSessionLocal
        session = AsyncSessionLocal()
        self.db = session
        user_uuid = uuid.UUID(user_id)

        # Update profile to 'running'
        await self.db.execute(text(
            "UPDATE public.profiles SET letterboxd_import_status = 'running', letterboxd_username = :u, updated_at = now() WHERE id = :uid"
        ), {"u": username, "uid": user_uuid})
        await self.db.commit()

        # Initialize progress for the client immediately
        sync_progress[user_id] = {
            "processed": 0,
            "total": 0,
            "status": "running",
            "current_item": "Scraping your Letterboxd account...",
            "recent_items": [],
            "imported_films": 0,
            "imported_reviews": 0,
        }

        # Scrape and gather
        loop = asyncio.get_event_loop()
        try:
            def scrape_all():
                def check_cancelled():
                    if sync_progress.get(user_id, {}).get("cancelled"):
                        raise Exception("Import cancelled by user")

                headers_chrome = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": '"Windows"',
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1"
                }

                scraper = cloudscraper.create_scraper(
                    browser={"browser": "chrome", "platform": "windows", "mobile": False}
                )
                
                # Fetch Watched films list
                check_cancelled()
                sync_progress[user_id]["current_item"] = "Fetching watched films list..."
                films = []
                for p in range(1, 25): # fetch up to 25 pages (~1800 items)
                    check_cancelled()
                    try:
                        html = self._fetch_url(f"https://letterboxd.com/{username}/films/page/{p}/")
                        if not html:
                            break
                        soup = BeautifulSoup(html, "html.parser")
                        items = []
                        for div in soup.find_all(attrs={"data-item-slug": True}):
                            slug = div.get("data-item-slug", "").strip()
                            li = div.find_parent("li")
                            rating_span = li.select_one("span.rating") if li else None
                            rating = self.parse_stars(rating_span.get_text()) if rating_span else None
                            items.append({"slug": slug, "rating": rating})
                        if not items:
                            break
                        films.extend(items)
                        if not soup.select_one("a.next"):
                            break
                    except Exception:
                        break

                # RSS Fallback if HTML scraping returned 0 items
                if not films:
                    try:
                        import requests
                        import xml.etree.ElementTree as ET
                        headers_basic = {"User-Agent": "Mozilla/5.0"}
                        rss_url = f"https://letterboxd.com/{username}/rss/"
                        rss_resp = requests.get(rss_url, headers=headers_basic, timeout=12)
                        if rss_resp.status_code == 200 and "<rss" in rss_resp.text:
                            root = ET.fromstring(rss_resp.text)
                            channel = root.find("channel")
                            if channel:
                                for item in channel.findall("item"):
                                    film_title = item.findtext("{https://letterboxd.com}filmTitle")
                                    tmdb_id = item.findtext("{https://themoviedb.org}movieId")
                                    rating_str = item.findtext("{https://letterboxd.com}memberRating")
                                    link = item.findtext("link") or ""
                                    slug = link.rstrip("/").split("/")[-1] if "film/" in link else None
                                    if not slug and film_title:
                                        slug = film_title.lower().replace(" ", "-")
                                    if slug:
                                        rating_val = float(rating_str) if rating_str else None
                                        films.append({"slug": slug, "rating": rating_val, "tmdb_id": tmdb_id, "title": film_title})
                    except Exception as rss_err:
                        logger.warning(f"RSS fallback error: {rss_err}")

                # Fetch Watchlist (all pages)
                check_cancelled()
                sync_progress[user_id]["current_item"] = "Fetching watchlist..."
                watchlist = []
                p = 1
                while True:
                    check_cancelled()
                    try:
                        html = self._fetch_url(f"https://letterboxd.com/{username}/watchlist/page/{p}/")
                        if not html:
                            break
                        soup = BeautifulSoup(html, "html.parser")
                        slugs = list(dict.fromkeys(re.findall(r'data-item-slug="([^"]+)"', str(soup))))
                        if not slugs:
                            break
                        watchlist.extend(slugs)
                        if not soup.select_one("a.next"):
                            break
                        p += 1
                    except Exception:
                        break

                # Fetch Liked films (all pages)
                check_cancelled()
                sync_progress[user_id]["current_item"] = "Fetching liked films..."
                liked_films = []
                p = 1
                while True:
                    check_cancelled()
                    html = self._fetch_url(f"https://letterboxd.com/{username}/likes/films/page/{p}/")
                    if not html:
                        break
                    soup = BeautifulSoup(html, "html.parser")
                    slugs = list(dict.fromkeys(re.findall(r'data-item-slug="([^"]+)"', str(soup))))
                    if not slugs:
                        break
                    liked_films.extend(slugs)
                    if not soup.select_one("a.next"):
                        break
                    p += 1

                # Fetch Reviews (all pages)
                check_cancelled()
                sync_progress[user_id]["current_item"] = "Fetching reviews..."
                reviews = []
                p = 1
                while True:
                    check_cancelled()
                    html = self._fetch_url(f"https://letterboxd.com/{username}/reviews/page/{p}/")
                    if not html:
                        break
                    soup = BeautifulSoup(html, "html.parser")
                    listitems = soup.select(".listitem")
                    items = []
                    for item in listitems:
                        a_tag = item.select_one("h2.primaryname a")
                        if not a_tag:
                            continue
                        href = a_tag.get("href", "")
                        slug = href.split("/film/")[-1].strip("/")
                        if "/" in slug:
                            slug = slug.split("/")[0]
                        if not slug:
                            continue
                        
                        body_text_div = item.select_one("div.body-text")
                        review_text = body_text_div.get_text(separator="\n").strip() if body_text_div else None
                        if not review_text:
                            continue
                        
                        rating_title = item.select_one("span.inline-rating svg title")
                        rating = self.parse_stars(rating_title.get_text()) if rating_title else None
                        
                        time_tag = item.select_one("time.timestamp")
                        watch_date = None
                        if time_tag:
                            dt_str = time_tag.get("datetime")
                            if dt_str:
                                try:
                                    watch_date = datetime.strptime(dt_str, "%Y-%m-%d").date()
                                except Exception:
                                    pass
                                    
                        items.append({
                            "slug": slug,
                            "review_text": review_text,
                            "rating": rating,
                            "watch_date": watch_date
                        })
                    if not items:
                        break
                    reviews.extend(items)
                    if not soup.select_one("a.next"):
                        break
                    p += 1

                # Fetch Custom Lists
                check_cancelled()
                sync_progress[user_id]["current_item"] = "Fetching custom lists..."
                custom_lists = []
                try:
                    lists_html = self._fetch_url(f"https://letterboxd.com/{username}/lists/")
                    if lists_html:
                        lists_soup = BeautifulSoup(lists_html, "html.parser")
                        list_links = lists_soup.select("h2.title-2 a, h2.title a, .headline-2 a, a.list-link, .film-list-card a, section.list-summary a")
                        seen_list_hrefs = set()
                        for link in list_links:
                            check_cancelled()
                            list_href = link.get("href")
                            list_title = link.get_text(strip=True)
                            if list_href and "/list/" in list_href and list_href not in seen_list_hrefs and list_title:
                                seen_list_hrefs.add(list_href)
                                full_url = list_href if list_href.startswith("http") else f"https://letterboxd.com{list_href}"
                                l_html = self._fetch_url(full_url)
                                if l_html:
                                    l_soup = BeautifulSoup(l_html, "html.parser")
                                    desc_el = l_soup.select_one(".body-text.-short p, .body-text p, .description p")
                                    l_desc = desc_el.get_text(strip=True) if desc_el else f"Imported from Letterboxd: {list_title}"
                                    l_items = []
                                    posters = l_soup.select("div.film-poster, li.poster-container div, ul.poster-list li div")
                                    for idx, poster in enumerate(posters):
                                        l_slug = poster.get("data-film-slug") or poster.get("data-target-link", "").split("/film/")[-1].strip("/")
                                        if not l_slug:
                                            a_elem = poster.select_one("a")
                                            if a_elem and a_elem.get("href"):
                                                h = a_elem.get("href", "")
                                                if "/film/" in h:
                                                    l_slug = h.split("/film/")[-1].strip("/")
                                        if l_slug and "/" in l_slug:
                                            l_slug = l_slug.split("/")[0]
                                        if l_slug:
                                            l_items.append({"slug": l_slug, "position": idx + 1})
                                    if l_items:
                                        custom_lists.append({
                                            "name": list_title,
                                            "description": l_desc,
                                            "items": l_items
                                        })
                except Exception as cl_err:
                    logger.warning(f"Error scraping custom lists for {username}: {cl_err}")

                check_cancelled()
                sync_progress[user_id]["current_item"] = "Resolving TMDB metadata..."
                return {
                    "films": films, 
                    "watchlist": watchlist, 
                    "reviews": reviews, 
                    "liked_films": liked_films,
                    "custom_lists": custom_lists
                }

            data = await loop.run_in_executor(None, scrape_all)
            await self.import_scraped_data(user_id, data)

        except Exception as e:
            logger.error(f"Background scrape failed for user {user_id}: {e}")
            # Rollback any aborted transaction before attempting recovery writes
            try:
                await self.db.rollback()
                await self.db.execute(text(
                    "UPDATE public.profiles SET letterboxd_import_status = 'failed', updated_at = now() WHERE id = :uid"
                ), {"uid": user_uuid})
                await self.db.commit()
            except Exception as recovery_err:
                logger.error(f"Recovery update also failed for user {user_id}: {recovery_err}")
            sync_progress[user_id] = {"processed": 0, "total": 0, "status": "failed"}
        finally:
            await session.close()

    async def run_zip_in_background(self, user_id: str, zip_bytes: bytes):
        """Task runner for parsing uploaded ZIP contents and importing."""
        from app.core.database import AsyncSessionLocal
        session = AsyncSessionLocal()
        self.db = session
        user_uuid = uuid.UUID(user_id)

        await self.db.execute(text(
            "UPDATE public.profiles SET letterboxd_import_status = 'running', updated_at = now() WHERE id = :uid"
        ), {"uid": user_uuid})
        await self.db.commit()

        try:
            # Parse ZIP
            data = {
                "films": [],
                "reviews": [],
                "watchlist": [],
                "liked_films": []
            }
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                namelist = z.namelist()
                
                def _parse_date(dt_str: Optional[str]) -> Optional[date]:
                    if not dt_str:
                        return None
                    s = str(dt_str).strip()
                    if not s:
                        return None
                    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d"):
                        try:
                            return datetime.strptime(s.split('.')[0], fmt).date()
                        except Exception:
                            pass
                    return None

                # 1. Watched & Diary list
                diary_file = next((f for f in namelist if f.endswith("diary.csv")), None)
                if diary_file:
                    with z.open(diary_file) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                        for row in reader:
                            uri = row.get("Letterboxd URI", "")
                            slug = uri.split("/film/")[-1].strip("/") if "/film/" in uri else ""
                            if "/" in slug:
                                slug = slug.split("/")[0]
                            watch_date = _parse_date(row.get("Watched Date") or row.get("Date"))
                            rating_str = row.get("Rating")
                            rating_val = float(rating_str) if rating_str else None
                            if slug:
                                data["films"].append({
                                    "slug": slug,
                                    "watch_date": watch_date,
                                    "rating": rating_val
                                })

                watched_file = next((f for f in namelist if f.endswith("watched.csv")), None)
                if watched_file:
                    watched_slugs = {f["slug"] for f in data["films"] if f.get("slug")}
                    with z.open(watched_file) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                        for row in reader:
                            uri = row.get("Letterboxd URI", "")
                            slug = uri.split("/film/")[-1].strip("/") if "/film/" in uri else ""
                            if "/" in slug:
                                slug = slug.split("/")[0]
                            if slug and slug not in watched_slugs:
                                watch_date = _parse_date(row.get("Date"))
                                data["films"].append({
                                    "slug": slug,
                                    "watch_date": watch_date
                                })
                                watched_slugs.add(slug)

                # 2. Ratings backfill (including rated-only films)
                ratings_file = next((f for f in namelist if f.endswith("ratings.csv")), None)
                if ratings_file:
                    with z.open(ratings_file) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                        ratings_map = {}
                        watched_slugs = {f["slug"] for f in data["films"] if f.get("slug")}
                        for row in reader:
                            uri = row.get("Letterboxd URI", "")
                            slug = uri.split("/film/")[-1].strip("/") if "/film/" in uri else ""
                            if "/" in slug:
                                slug = slug.split("/")[0]
                            if slug:
                                rating_val = float(row.get("Rating", 0.0))
                                ratings_map[slug] = rating_val
                                if slug not in watched_slugs:
                                    watch_date = _parse_date(row.get("Date"))
                                    data["films"].append({
                                        "slug": slug,
                                        "watch_date": watch_date,
                                        "rating": rating_val
                                    })
                                    watched_slugs.add(slug)
                        for film in data["films"]:
                            if film.get("rating") is None:
                                film["rating"] = ratings_map.get(film["slug"])

                # 3. Reviews list
                reviews_file = next((f for f in namelist if f.endswith("reviews.csv")), None)
                if reviews_file:
                    with z.open(reviews_file) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                        for row in reader:
                            uri = row.get("Letterboxd URI", "")
                            slug = uri.split("/film/")[-1].strip("/") if "/film/" in uri else ""
                            if "/" in slug:
                                slug = slug.split("/")[0]
                            watch_date = _parse_date(row.get("Date"))
                            data["reviews"].append({
                                "slug": slug,
                                "review_text": row.get("Review"),
                                "rating": float(row.get("Rating")) if row.get("Rating") else None,
                                "watch_date": watch_date
                            })

                # 4. Watchlist
                watchlist_file = next((f for f in namelist if f.endswith("watchlist.csv")), None)
                if watchlist_file:
                    with z.open(watchlist_file) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                        for row in reader:
                            uri = row.get("Letterboxd URI", "")
                            slug = uri.split("/film/")[-1].strip("/") if "/film/" in uri else ""
                            if "/" in slug:
                                slug = slug.split("/")[0]
                            data["watchlist"].append({
                                "slug": slug
                            })

                # 5. Liked films
                likes_file = next((f for f in namelist if f.endswith("likes.csv") or "likes/films.csv" in f or "likes.csv" in f), None)
                if likes_file:
                    with z.open(likes_file) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                        for row in reader:
                            uri = row.get("Letterboxd URI", "")
                            if "/film/" in uri:
                                slug = uri.split("/film/")[-1].strip("/")
                                if "/" in slug:
                                    slug = slug.split("/")[0]
                                if slug:
                                    data["liked_films"].append(slug)

                # 6. Custom Lists (lists/ folder)
                data["custom_lists"] = []
                list_files = [f for f in namelist if (f.startswith("lists/") or "/lists/" in f) and f.endswith(".csv")]
                for list_file in list_files:
                    try:
                        filename = list_file.split("/")[-1].replace(".csv", "")
                        raw_name = filename.replace("-", " ").title()
                        list_items = []
                        list_description = None
                        
                        with z.open(list_file) as f:
                            text_content = io.TextIOWrapper(f, encoding='utf-8-sig', errors='ignore').read()
                            lines = text_content.splitlines()
                            if lines:
                                reader = csv.DictReader(lines)
                                for row in reader:
                                    uri = row.get("Letterboxd URI", "") or row.get("URL", "")
                                    slug = uri.split("/film/")[-1].strip("/") if "/film/" in uri else ""
                                    if "/" in slug:
                                        slug = slug.split("/")[0]
                                    
                                    if not list_description and row.get("Description"):
                                        list_description = row.get("Description")

                                    pos_val = row.get("Position")
                                    pos = int(pos_val) if pos_val and str(pos_val).isdigit() else len(list_items) + 1

                                    if slug:
                                        list_items.append({"slug": slug, "position": pos})
                                        
                        if list_items:
                            data["custom_lists"].append({
                                "name": raw_name,
                                "description": list_description or f"Imported from Letterboxd list: {raw_name}",
                                "items": list_items
                            })
                    except Exception as list_err:
                        logger.warning(f"Failed to parse custom list {list_file}: {list_err}")

            await self.import_scraped_data(user_id, data)

        except Exception as e:
            logger.error(f"Background ZIP import failed for user {user_id}: {e}")
            await self.db.execute(text(
                "UPDATE public.profiles SET letterboxd_import_status = 'failed', updated_at = now() WHERE id = :uid"
            ), {"uid": user_uuid})
            await self.db.commit()
            sync_progress[user_id] = {"processed": 0, "total": 0, "status": "failed"}
        finally:
            await session.close()
