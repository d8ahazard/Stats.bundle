############################################################################
# This plugin will allow external calls, that the plugin can then handle
# See TODO doc for more details
#
# Made by
# dane22 & digitalhigh...Plex Community members
#
############################################################################

from __future__ import print_function
import StringIO
import glob
from plugin.core.environment import Environment, translate as _
import os

Environment.setup(Core, Dict, Platform, Prefs)
from zipfile import ZipFile, ZIP_DEFLATED
from plex_database.core import db
import sys

from subzero.lib.io import FileIO

import log_helper
from CustomContainer import MediaContainer, ZipObject, MetaContainer, StatContainer, UserContainer, \
    ViewContainer, AnyContainer
from lib import Plex

if sys.platform == "win32":
    if 'PLEXLOCALAPPDATA' in os.environ:
        key = 'PLEXLOCALAPPDATA'
    else:
        key = 'LOCALAPPDATA'
    pmsPath = os.path.join(os.environ[key], 'Plex Media Server')
else:
    pmsPath = os.path.join(os.environ["HOME"], "Library", "Application Support", "Plex Media Server")

dbPath = os.path.join(pmsPath, "Plug-in Support", "Databases", "com.plexapp.plugins.library.db")
os.environ['LIBRARY_DB'] = dbPath

# ------------------------------------------------
# Libraries
# ------------------------------------------------
from plugin.core.libraries.manager import LibrariesManager

LibrariesManager.setup(cache=True)
LibrariesManager.test()
# Dummy Imports for PyCharm

# import Framework.context
# from Framework.api.objectkit import ObjectContainer, DirectoryObject
# from Framework.docutils import Plugin, HTTP, Log, Request
# from Framework.docutils import Data

NAME = 'PMSStats'
VERSION = '1.1.105'
PREFIX = '/applications/stats'
PREFIX2 = '/stats'
APP = '/stats'
ICON = 'icon-cast.png'
ICON_CAST = 'icon-cast.png'
ICON_CAST_REFRESH = 'icon-cast_refresh.png'
ICON_PLEX_CLIENT = 'icon-plex_client.png'
PLUGIN_IDENTIFIER = "com.plexapp.plugins.Stats"


# Start function
def Start():
    Plugin.AddViewGroup("Details", viewMode="InfoList", mediaType="items")
    ObjectContainer.title1 = NAME
    DirectoryObject.thumb = R(ICON)
    HTTP.CacheTime = 5
    ValidatePrefs()


@handler(PREFIX, NAME)
@handler(PREFIX2, NAME)
@route(PREFIX + '/MainMenu')
@route(PREFIX2)
def MainMenu():
    """
    Main menu
    and stuff
    """
    Log.Debug("**********  Starting MainMenus  **********")
    title = NAME + " - " + VERSION
    if Data.Exists('last_scan'):
        title = NAME + " - " + Data.Load('last_scan')

    oc = ObjectContainer(
        title1=title,
        no_cache=True,
        no_history=True,
        title_bar="Stats",
        view_group="Details")

    do = DirectoryObject(
        title="Stats",
        thumb=R(ICON_CAST_REFRESH),
        key=Callback(Stats))

    oc.add(do)

    do = DirectoryObject(
        title="Advanced",
        thumb=R(ICON_CAST_REFRESH),
        key=Callback(AdvancedMenu))

    oc.add(do)

    return oc


@route(PREFIX + '/ValidatePrefs')
def ValidatePrefs():
    """
    Called by the framework every time a user changes the prefs
    We add this dummy function, to avoid errors in the log
    and stuff.
    """

    dependencies = ['pychromecast', 'zeroconf']
    log_helper.register_logging_handler(dependencies, level="DEBUG")
    return


@route(APP + '/all')
@route(PREFIX2 + '/stats/all')
def All():
    mc = MediaContainer()
    headers = sort_headers(["Limit", "Type"])
    if "Limit" in headers:
        limit = headers["Limit"]
    else:
        limit = 100

    Log.Debug("Here's where we fetch some stats.")
    records = query_tag_stats("all", limit)
    for record in records:
        sc = StatContainer(record)
        mc.add(sc)

    return mc


@route(APP + '/actor')
@route(PREFIX2 + '/stats/actor')
def Actor():
    mc = MediaContainer()
    headers = sort_headers(["Limit", "Type"])
    if "Limit" in headers:
        limit = headers["Limit"]
    else:
        limit = 100

    Log.Debug("Here's where we fetch some stats.")
    records = query_tag_stats("actor", limit)
    for record in records:
        sc = StatContainer(record)
        mc.add(sc)

    return mc


@route(APP + '/director')
@route(PREFIX2 + '/stats/director')
def Director():
    mc = MediaContainer()
    headers = sort_headers(["Limit", "Type"])
    if "Limit" in headers:
        limit = headers["Limit"]
    else:
        limit = 100

    Log.Debug("Here's where we fetch some stats.")
    records = query_tag_stats("director", limit)
    for record in records:
        sc = StatContainer(record)
        mc.add(sc)

    return mc


@route(APP + '/writer')
@route(PREFIX2 + '/stats/writer')
def Writer():
    mc = MediaContainer()
    headers = sort_headers(["Limit", "Type"])
    if "Limit" in headers:
        limit = headers["Limit"]
    else:
        limit = 100

    Log.Debug("Here's where we fetch some stats.")
    records = query_tag_stats("writer", limit)
    for record in records:
        sc = StatContainer(record)
        mc.add(sc)

    return mc


@route(APP + '/user')
@route(PREFIX2 + '/stats/user')
def User():
    mc = MediaContainer()
    headers = sort_headers(["Type", "Userid", "Username", "Limit", "Device", "Title"])

    records = query_media_stats(headers)

    users1 = {}
    users2 = {}
    for record in records[0]:
        user_name = record["userName"]
        if user_name not in users1:
            users1[user_name] = []
        temp_dict = users1[user_name]
        Log.Debug("Appending record '%s'" % JSON.StringFromObject(record))
        del(record["userName"])
        temp_dict.append(dict(record))
        users1[user_name] = temp_dict

    for record in records[1]:
        user_name = record["userName"]
        if user_name not in users2:
            users2[user_name] = []
        temp_dict = users2[user_name]
        del(record["userName"])
        temp_dict.append(dict(record))
        users2[user_name] = temp_dict

    for name in users1:
        id = users1[name][0]["user_id"]
        uc = UserContainer({"name": name, "id": id})
        ac = AnyContainer(None, "Media")
        Log.Debug("Creating container1 for %s" % name)
        for record in users1[name]:
            del(record["user_id"])
            Log.Debug("Adding record")
            vc = ViewContainer(record)
            ac.add(vc)
        uc.add(ac)
        ac = AnyContainer(None, "Stats")
        for record in users2[name]:
            del(record["user_id"])
            vc = ViewContainer(record)
            ac.add(vc)
        uc.add(ac)
        mc.add(uc)

    Log.Debug("Still alive")

    return mc


@route(APP + '/genre')
@route(PREFIX2 + '/stats/genre')
def Genre():
    mc = MediaContainer()
    headers = sort_headers(["Limit", "Type"])
    if "Limit" in headers:
        limit = headers["Limit"]
    else:
        limit = 100

    Log.Debug("Here's where we fetch some statsssss.")
    records = query_tag_stats("genre", limit)
    for record in records:
        sc = StatContainer(record)
        mc.add(sc)

    return mc


# This guy lets us fetch logs remotely if needed
@route(PREFIX + '/logs')
@route(PREFIX2 + '/logs')
def DownloadLogs():
    buff = StringIO.StringIO()
    zip_archive = ZipFile(buff, mode='w', compression=ZIP_DEFLATED)
    paths = get_log_paths()
    if (paths[0] is not False) & (paths[1] is not False):
        logs = sorted(glob.glob(paths[0] + '*')) + [paths[1]]
        for path in logs:
            Log.Debug("Trying to read path: " + path)
            data = StringIO.StringIO()
            data.write(FileIO.read(path))
            zip_archive.writestr(os.path.basename(path), data.getvalue())

        zip_archive.close()

        return ZipObject(buff.getvalue())

    Log.Debug("No log path found, foo.")
    return ObjectContainer(
        no_cache=True,
        title1="No logs found",
        no_history=True,
        view_group="Details")


def get_log_paths():
    # find log handler
    server_log_path = False
    plugin_log_path = False
    for handler in Core.log.handlers:
        if getattr(getattr(handler, "__class__"), "__name__") in (
                'FileHandler', 'RotatingFileHandler', 'TimedRotatingFileHandler'):
            plugin_log_file = handler.baseFilename
            if os.path.isfile(os.path.realpath(plugin_log_file)):
                plugin_log_path = plugin_log_file
                Log.Debug("Found a plugin path: " + plugin_log_path)

            if plugin_log_file:
                server_log_file = os.path.realpath(os.path.join(plugin_log_file, "../../Plex Media Server.log"))
                if os.path.isfile(server_log_file):
                    server_log_path = server_log_file
                    Log.Debug("Found a server log path: " + server_log_path)

    return [plugin_log_path, server_log_path]


# These routes build a menu and let us snag the logs, as well as trigger restarts
@route(PREFIX2 + '/advanced')
def AdvancedMenu(header=None, message=None):
    oc = ObjectContainer(header=header or "Internal stuff, pay attention!", message=message, no_cache=True,
                         no_history=True,
                         replace_parent=False, title2="Advanced")

    oc.add(DirectoryObject(
        key=Callback(TriggerRestart),
        title="Restart the plugin",
    ))

    return oc


def DispatchRestart():
    Thread.CreateTimer(1.0, Restart)


@route(PREFIX2 + '/advanced/restart/trigger')
def TriggerRestart():
    DispatchRestart()
    oc = ObjectContainer(
        title1="restarting",
        no_cache=True,
        no_history=True,
        title_bar="Chromecast",
        view_group="Details")

    return oc


@route(PREFIX2 + '/advanced/restart/execute')
def Restart():
    Plex[":/plugins"].restart(PLUGIN_IDENTIFIER)


def sort_headers(header_list, strict=False):
    returns = {}
    for key, value in Request.Headers.items():
        Log.Debug("Header key %s is %s", key, value)
        for item in header_list:
            if key in ("X-Plex-" + item, item):
                Log.Debug("We have a " + item)
                returns[item] = unicode(value)
                header_list.remove(item)

    if strict:
        len2 = len(header_list)
        if len2 == 0:
            Log.Debug("We have all of our values: " + JSON.StringFromObject(returns))
            return returns

        else:
            Log.Error("Sorry, parameters are missing.")
            for item in header_list:
                Log.Error("Missing " + item)
            return False
    else:
        return returns


def query_tag_stats(selection, limit=100):
    Log.Debug("Limit is set to %s" % limit)

    try:
        import apsw
    except ImportError as ex:
        Log.Debug('Unable to import "apsw": %s', ex, exc_info=True)
        apsw = None

    if apsw is not None:
        Log.Debug("Shit, we got the library!")
        connection = apsw.Connection(os.environ['LIBRARY_DB'])
        cursor = connection.cursor()

        results = []

        options = ["all", "actor", "director", "writer", "genre"]

        if selection not in options:
            return False

        if selection == "all":
            fetch_values = "tags.tag, tags.tag_type, mt.metadata_type, mt.user_thumb_url, mt.user_art_url, mt.id, " \
                           "COUNT(tags.id)"
            tag_selection = "tags.tag_type = 6 OR tags.tag_type = 5 OR tags.tag_type = 4 OR tags.tag_type = 1"

        if selection == "actor":
            fetch_values = "tags.tag, mt.user_thumb_url, mt.user_art_url, mt.id, COUNT(tags.id)"
            tag_selection = "tags.tag_type = 6"

        if selection == "director":
            fetch_values = "tags.tag, mt.user_thumb_url, mt.user_art_url, mt.id, COUNT(tags.id)"
            tag_selection = "tags.tag_type = 4"

        if selection == "writer":
            fetch_values = "tags.tag, mt.user_thumb_url, mt.user_art_url, mt.id, COUNT(tags.id)"
            tag_selection = "tags.tag_type = 5"

        if selection == "genre":
            fetch_values = "tags.tag, mt.user_thumb_url, mt.user_art_url, mt.id, COUNT(tags.id)"
            tag_selection = "tags.tag_type = 5"

        query = """SELECT %s
                        AS Total FROM tags
                        LEFT JOIN taggings ON tags.id = taggings.tag_id
                        INNER JOIN metadata_items AS mt
                        ON taggings.metadata_item_id = mt.id
                        WHERE %s
                        GROUP BY tags.tag,tags.tag_type
                        ORDER BY Total
                        desc LIMIT %s;""" % (fetch_values, tag_selection, limit)

        if selection == "all":
            for title, tag_type,  meta_type, meta_thumb, meta_art, ratingkey, tag_count in cursor.execute(query):
                tag_types = {
                    1: "genre",
                    4: "director",
                    5: "writer",
                    6: "actor"
                }

                if tag_type in tag_types:
                    tag_title = tag_types[tag_type]
                else:
                    tag_title = "unknown"

                meta_types = {
                    1: "movie",
                    2: "show",
                    4: "episode",
                    9: "album",
                    10: "track"
                }
                if meta_type in meta_types:
                    meta_type = meta_types[meta_type]

                dicts = {
                    "title": title,
                    "type": tag_title,
                    "count": tag_count,
                    "metaType": meta_type,
                    "thumb": meta_thumb,
                    "art": meta_art,
                    "ratingKey": ratingkey
                }

                results.append(dicts)
        else:
            for tag, thumb, art, ratingkey, count in cursor.execute(query):
                dicts = {
                    "title": tag,
                    "thumb": thumb,
                    "art": art,
                    "ratingKey": ratingkey,
                    "count": count
                }

                results.append(dicts)

        return results
    else:
        Log.Debug("No DB HERE, FUCKER.")
        return False


def query_media_stats(headers):

    if "Limit" in headers:
        limit = headers["Limit"]
        del headers["Limit"]
    else:
        limit = None

    meta_types = {
        "movie": 1,
        "show": 2,
        "episode": 4,
        "album": 9,
        "track": 10
    }

    if "Type" in headers:
        if headers["Type"] in meta_types:
            headers['Type'] = meta_types[headers['Type']]

    Log.Debug("Limit is set to %s" % limit)

    try:
        import apsw
    except ImportError as ex:
        Log.Debug('Unable to import "apsw": %s', ex, exc_info=True)
        apsw = None

    if apsw is not None:
        Log.Debug("Shit, we got the librarys!")
        connection = apsw.Connection(os.environ['LIBRARY_DB'])
        cursor = connection.cursor()

        if limit is not None:
            limit = "LIMIT %s" % limit
        else:
            limit = ""

        lines = []
        query_selector = ""
        if len(headers.keys()) != 0:
            Log.Debug("We have headers...")
            selectors = {
                "Userid": "acc.id",
                "Username": "acc.name",
                "Type": "mi.metadata_type",
                "Title": "mi.title"
            }

            for header_key, value in headers.items():
                if header_key in selectors:
                    Log.Debug("Header key %s is present" % header_key)
                    header_key = selectors[header_key]
                    lines.append("%s = '%s'" % (header_key, value))

        if bool(lines):
            Log.Debug("We have lines too...")
            query_selector = "WHERE " + "AND".join(lines)

        query = """SELECT mi.id AS media_id, 
                    metadata_item_views.title, metadata_item_views.grandparent_title, metadata_item_views.viewed_at,
                    mi.metadata_type, mi.user_thumb_url, mi.user_art_url,
                    acc.id, acc.name from metadata_item_views
                    INNER JOIN metadata_items AS mi 
                       ON metadata_item_views.title = mi.title
                    INNER JOIN accounts as acc
                       ON acc.id = metadata_item_views.account_id
                %s
                ORDER BY metadata_item_views.viewed_at desc
                %s;""" % (query_selector, limit)

        Log.Debug("Querys is '%s'" % query)
        results = []
        for rating_key, title, grandparent_title, viewed_at, meta_type, thumb, art, user_id, user_name in cursor.execute(query):
            dictz = {
                "user_id": user_id,
                "userName": user_name,
                "ratingKey": rating_key,
                "title": title,
                "grandparentTitle": grandparent_title,
                "lastViewed": viewed_at,
                "type": meta_type,
                "thumb": thumb,
                "art": art
            }
            results.append(dictz)

        lines = []
        results2 = []
        query_selector = ""
        if len(headers.keys()) != 0:
            Log.Debug("We have headers...")
            selectors = {
                "Userid": "sm.account_id",
                "Username": "accounts.name",
                "Type": "sm.metadata_type"
            }

            for header_key, value in headers.items():
                if header_key in selectors:
                    Log.Debug("Header key %s is present" % header_key)
                    header_key = selectors[header_key]
                    lines.append("%s = '%s'" % (header_key, value))

        if bool(lines):
            Log.Debug("We have lines too...")
            query_selector = "WHERE " + "AND".join(lines)

        query2 = """select sm.account_id, sm.timespan, sm.at, sm.metadata_type, sm.count, sm.duration,
                         accounts.name,
                         devices.name AS device_name, devices.identifier AS device_id,
                         sb.bytes from statistics_media AS sm
                         INNER JOIN statistics_bandwidth as sb
                             ON sb.at = sm.at AND sb.account_id = sm.account_id AND sb.device_id = sm.device_id
                         INNER JOIN accounts
                             ON accounts.id = sm.account_id
                         INNER JOIN devices
                             ON devices.id = sm.device_id
                             %s
                         ORDER BY sm.at DESC 
                         %s;""" % (query_selector, limit)

        for user_id, timespan, viewed_at, meta_type, count, duration, user_name, device_name, device_id, bytes in cursor.execute(query2):
            dictz = {
                "user_id": user_id,
                "userName": user_name,
                "timespan": timespan,
                "viewedAt": viewed_at,
                "metaType": meta_type,
                "count": count,
                "duration": duration,
                "deviceName": device_name,
                "deviceId": device_id,
                "bytes": bytes
            }
            results2.append(dictz)

        return [results, results2]
    else:
        Log.Debug("No DB HERE, FUCKER.")
        return False
