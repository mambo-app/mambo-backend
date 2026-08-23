from pydantic import BaseModel
from typing import Optional
from enum import Enum
from uuid import UUID

class ActionType(str, Enum):
    watch = 'watch'
    rewatch = 'rewatch'
    drop = 'drop'
    like = 'like'
    unlike = 'unlike'
    save = 'save'
    unsave = 'unsave'
    skip = 'skip'
    recommend = 'recommend'
    rate = 'rate'
    unrate = 'unrate'
    review = 'review'
    notify = 'notify'
    unnotify = 'unnotify'
    set_status = 'set_status'
    watch_episode = 'watch_episode'
    increment_progress = 'increment_progress'
    complete_season = 'complete_season'
    untrack = 'untrack'
    unskip = 'unskip'
    delete_watch = 'delete_watch'

class ContentActionRequest(BaseModel):
    action: ActionType
    # Optional metadata if needed for specific actions
    rating: Optional[float] = None
    status: Optional[str] = None
    season_number: Optional[int] = None
    episode_number: Optional[int] = None
    watch_history_id: Optional[UUID] = None

class ContentActionResponse(BaseModel):
    status: str
    action: ActionType
    content_id: UUID
    is_permanent: bool = True
