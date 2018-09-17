# Stats.bundle

Get stats...and stuff. IDK.

# Endpoints:

server/stats/ENDPOINT?X-Plex-Token=XXXXXX

Where ENDPOINT is one of 
stats/user
stats/director
stats/writer
stats/genre
stats/actor
stats/all

Every endpoint accepts an X-Plex-Limit parameter in the query

And stats/user accepts any one of these (as X-Plex-...)
"Type", "Userid", "Username", "Limit", "Device", "Title"

"Type" can be any of movie/show/album/track/episode

This is under development, so refer to Contents\Code\__init__.py for
the most up-to-date list of endpoints and accepted headers for each