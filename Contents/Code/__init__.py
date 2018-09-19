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
import os
import sys
from zipfile import ZipFile, ZIP_DEFLATED
import log_helper
from CustomContainer import MediaContainer, ZipObject, MetaContainer, StatContainer, UserContainer, \
    ViewContainer, AnyContainer
from helpers import PathHelper
from lib import Plex
from helpers.system import SystemHelper
from helpers.variable import pms_path

from subzero.lib.io import FileIO

UNICODE_MAP = {
            65535: 'ucs2',
            1114111: 'ucs4'
        }

pms_path = pms_path()
Log.Debug("New PMS Path is '%s'" % pms_path)
dbPath = os.path.join(pms_path, "Plug-in Support", "Databases", "com.plexapp.plugins.library.db")
Log.Debug("Setting DB path to '%s'" % dbPath)
os.environ['LIBRARY_DB'] = dbPath
os.environ["PMS_PATH"] = pms_path

os_platform = False
path = None

# ------------------------------------------------
# Libraries
# ------------------------------------------------
# from plugin.core.libraries.manager import LibrariesManager
#
# LibrariesManager.setup(cache=False)
# LibrariesManager.test()
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
    distribution = None
    libraries_path = os.path.join(pms_path, "Plug-ins", "Stats.bundle", "Contents", "Libraries")
    loaded = insert_paths(distribution, libraries_path)
    if loaded:
        os.environ["Loaded"] = "True"
    else:
        os.environ["Loaded"] = "False"


@handler(PREFIX, NAME)
@handler(PREFIX2, NAME)
@route(PREFIX + '/MainMenu')
@route(PREFIX2)
def MainMenu():
    """
    Main menu
    and stuff
    """
    Log.Debug("**********  Starting MainMenus  ***********")
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

    dependencies = ["helpers"]
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
    if records is not None:
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
    if records is not None:
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
    if records is not None:
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
    if records is not None:
        for record in records:
            sc = StatContainer(record)
            mc.add(sc)

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


@route(APP + '/library')
@route(PREFIX2 + '/stats/library')
def Library():
    mc = MediaContainer()
    headers = sort_headers(["Limit", "Type"])
    Log.Debug("Here's where we fetch some library stats.")
    sections = {}
    records = query_library_stats(headers)
    for record in records:
        section = record["sectionTitle"]
        if section not in sections:
            sections[section] = []
        del (record["sectionTitle"])
        sections[section].append(dict(record))

    for name in sections:
        Log.Debug("Looping through section '%s'" % name)
        sec_id = sections[name][0]["section"]
        sec_type = sections[name][0]["sectionType"]
        section_types = {
            1: "movie",
            2: "show",
            3: "music",
            4: "photo",
            8: "music",
            13: "photo"
        }
        if sec_type in section_types:
            sec_type = section_types[sec_type]

        item_count = 0
        play_count = 0
        playable_count = 0
        last_item = None
        section_children = []
        for record in sections[name]:
            item_count += record["totalItems"]
            if record['playCount'] is not None:
                play_count += record['playCount']
            if record["type"] in ["episode", "track", "movie"]:
                playable_count = record["totalItems"]

            item_type = str(record["type"]).capitalize()
            record_data = {
                "totalItems": record["totalItems"]
            }
            vc = AnyContainer(record_data, item_type, False)

            if record["lastViewed"] is not None:
                last_item = {
                    "title": record['title'],
                    "grandparentTitle": record['grandparentTitle'],
                    "art": record['art'],
                    "thumb": record['thumb'],
                    "ratingKey": record['ratingKey'],
                    "viewedAt": record['lastViewed'],
                    "userTitle": record['userTitle'],
                    "userId": record['userId']
                }
                li = AnyContainer(last_item, "lastViewed", False)
                vc.add(li)

            section_children.append(vc)

        section_data = {
            "title": name,
            "id": sec_id,
            "totalItems": item_count,
            "playableItems": playable_count,
            "playCount": play_count,
            "type": sec_type
        }
        ac = AnyContainer(section_data, "Section", "False")
        for child in section_children:
            ac.add(child)

        mc.add(ac)

    return mc


@route(APP + '/user')
@route(PREFIX2 + '/stats/user')
def User():
    mc = MediaContainer()
    headers = sort_headers(["Type", "Userid", "Username", "Limit", "Device", "Title"])

    records = query_user_stats(headers)
    if records is not None:
        users1 = {}
        users2 = {}
        temp1 = records[0]
        temp2 = records[1]
        del records
        for record in temp1:
            user_name = record["userName"]
            if user_name not in users1:
                users1[user_name] = []
            del (record["userName"])
            users1[user_name].append(dict(record))
        temp1 = temp2
        for record in temp1:
            user_name = record["userName"]
            if user_name not in users2:
                users2[user_name] = []
            del (record["userName"])
            users2[user_name].append(dict(record))
        del temp2
        for name in users1:
            user_id = users1[name][0]["user_id"]
            uc = UserContainer({"name": name, "id": user_id})
            ac = AnyContainer({"totalPlays": len(users1[name])}, "Media")
            Log.Debug("Creating container1 for %s" % name)
            i = 0
            for record in users1[name]:
                if i >= 25:
                    break
                del (record["user_id"])
                ac.add(ViewContainer(record))
                i += 1

            uc.add(ac)
            ac = AnyContainer(None, "Stats")
            if name in users2:
                ac = AnyContainer({"totalItems": len(users2[name])}, "Stats")
                i = 0
                for record in users2[name]:
                    if i >= 25:
                        break
                    del (record["user_id"])
                    ac.add(ViewContainer(record))
                    i += 1
            uc.add(ac)
            mc.add(uc)

    Log.Debug("Still alive, returning data")

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


def query_tag_stats(selection, limit=1000):
    Log.Debug("Limit is set to %s" % limit)

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    if cursor is not None:
        results = []

        options = ["all", "actor", "director", "writer", "genre"]

        if selection not in options:
            return False

        if selection == "all":
            fetch_values = "tags.tag, tags.tag_type, mt.metadata_type, mt.id, " \
                           "COUNT(tags.id)"
            tag_selection = "tags.tag_type = 6 OR tags.tag_type = 5 OR tags.tag_type = 4 OR tags.tag_type = 1"

        if selection == "actor":
            fetch_values = "tags.tag, mt.id, COUNT(tags.id)"
            tag_selection = "tags.tag_type = 6"

        if selection == "director":
            fetch_values = "tags.tag, mt.id, COUNT(tags.id)"
            tag_selection = "tags.tag_type = 4"

        if selection == "writer":
            fetch_values = "tags.tag, mt.id, COUNT(tags.id)"
            tag_selection = "tags.tag_type = 5"

        if selection == "genre":
            fetch_values = "tags.tag, mt.id, COUNT(tags.id)"
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
            for title, tag_type, meta_type, ratingkey, tag_count in cursor.execute(query):
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
                    "ratingKey": ratingkey,
                    "thumb": "/library/metadata/" + str(ratingkey) + "/thumb",
                    "art": "/library/metadata/" + str(ratingkey) + "/art"
                }

                if meta_type == "episode":
                    dicts["banner"] = "/library/metadata/" + str(ratingkey) + "/banner/"

                results.append(dicts)
        else:
            for tag, ratingkey, count in cursor.execute(query):
                dicts = {
                    "title": tag,
                    "count": count,
                    "ratingKey": ratingkey,
                    "thumb": "/library/metadata/" + str(ratingkey) + "/thumb",
                    "art": "/library/metadata/" + str(ratingkey) + "/art"
                }

                results.append(dicts)
        close_connection(connection)
        return results
    else:
        Log.Error("DB Connection error!")
        return None


def query_user_stats(headers):
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

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    query_types = "(1, 4, 10)"
    if cursor is not None:
        if limit is not None:
            limit = "LIMIT %s" % limit
        else:
            limit = ""
        Log.Debug("Limit is set to %s" % limit)

        lines = []
        results2 = []
        selector = "sm.metadata_type"
        if "Type" in headers:
            temp_selector = "WHERE "
        else:
            temp_selector = "WHERE %s IN %s " % (selector, query_types)
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
            query_selector = temp_selector + "AND".join(lines)
        else:
            query_selector = temp_selector

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

        Log.Debug("Query1) is '%s'" % query2)
        for user_id, timespan, viewed_at, meta_type, count, duration, user_name, device_name, device_id, bytes in cursor.execute(
                query2):
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
        Log.Debug("Query1 completed.")
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

        selector = "mi.metadata_type"
        if "Type" in headers:
            temp_selector = "WHERE "
        else:
            temp_selector = "WHERE %s IN %s " % (selector, query_types)

        if bool(lines):
            Log.Debug("We have lines too...")
            query_selector = temp_selector + "AND".join(lines)
        else:
            query_selector = temp_selector


        query = """SELECT mi.id AS media_id, 
                    metadata_item_views.title, metadata_item_views.grandparent_title, metadata_item_views.viewed_at,
                    mi.metadata_type,
                    acc.id, acc.name from metadata_item_views
                    INNER JOIN metadata_items AS mi 
                       ON metadata_item_views.title = mi.title
                    INNER JOIN accounts as acc
                       ON acc.id = metadata_item_views.account_id
                %s
                ORDER BY metadata_item_views.viewed_at desc
                %s;""" % (query_selector, limit)

        Log.Debug("Query2 is '%s'" % query)
        results = []
        for ratingkey, title, grandparent_title, viewed_at, meta_type, user_id, user_name in cursor.execute(
                query):
            dicts = {
                "user_id": user_id,
                "userName": user_name,
                "title": title,
                "grandparentTitle": grandparent_title,
                "lastViewed": viewed_at,
                "type": meta_type,
                "ratingKey": ratingkey,
                "thumb": "/library/metadata/" + str(ratingkey) + "/thumb",
                "art": "/library/metadata/" + str(ratingkey) + "/art"
            }

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(ratingkey) + "/banner/"

            results.append(dicts)
        Log.Debug("Query2 completed")
        close_connection(connection)
        return [results, results2]
    else:
        Log.Error("DB Connection error!")
        return None


def query_library_stats(headers):
    if "Limit" in headers:
        limit = headers["Limit"]
        del headers["Limit"]
    else:
        limit = None

    meta_types = {
        1: "movie",
        2: "show",
        3: "season",
        4: "episode",
        8: "artist",
        9: "album",
        10: "track",
        12: "extra",
        13: "photo",
        15: "playlist",
        18: "collection"
    }

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    if cursor is not None:
        query = """select
            FirstSet.library_section_id,
            FirstSet.metadata_type,    
            FirstSet.item_count,
            SecondSet.play_count,
            SecondSet.rating_key,
            SecondSet.title,
            SecondSet.grandparent_title,
            SecondSet.last_viewed,
            SecondSet.user_name,
            SecondSet.user_id,
            FirstSet.section_name,
            FirstSet.section_type
        from 
            (
                SELECT
                    mi.library_section_id,
                    mi.metadata_type,
                    ls.name as section_name, ls.section_type,
                    count(mi.metadata_type) as item_count
                FROM metadata_items as mi
                INNER JOIN library_sections as ls
                    ON mi.library_section_id = ls.id
                WHERE library_section_id is NOT NULL
                GROUP BY library_section_id, metadata_type
            ) as FirstSet
        LEFT JOIN
            (
                SELECT 
                    mi.id as rating_key,
                    miv.title as title,
                    miv.library_section_id,
                    miv.viewed_at as last_viewed,
                    miv.metadata_type,
                    miv.grandparent_title as grandparent_title,
                    count(miv.metadata_type) as play_count,
                    accounts.name as user_name, accounts.id as user_id,
                    ls.name as section_name, ls.section_type as section_type,
                    max(viewed_at) as last_viewed 
                FROM metadata_item_views as miv
                INNER JOIN library_sections as ls
                    ON miv.library_section_id = ls.id
                INNER JOIN metadata_items as mi
                    ON mi.title = miv.title
                INNER JOIN accounts
                    ON miv.account_id = accounts.id
                AND
                    mi.metadata_type = miv.metadata_type             
                WHERE mi.library_section_id is NOT NULL 
                GROUP BY miv.metadata_type
            ) as SecondSet
        on FirstSet.library_section_id = SecondSet.library_section_id AND FirstSet.metadata_type = SecondSet.metadata_type
        GROUP BY FirstSet.library_section_id, FirstSet.metadata_type
        ORDER BY FirstSet.library_section_id;"""

        Log.Debug("Querys is '%s'" % query)
        results = []
        for section, meta_type, item_count, play_count, ratingkey, title, \
            grandparent_title, last_viewed, user_name, user_id, sec_name, sec_type in cursor.execute(
                query):

            if meta_type in meta_types:
                meta_type = meta_types[meta_type]
            else:
                Log.Debug("Unkown meta type for %s of %s" % (title, meta_type))
                meta_type = "unknown"

            dicts = {
                "section": section,
                "totalItems": item_count,
                "playCount": play_count,
                "title": title,
                "grandparentTitle": grandparent_title,
                "lastViewed": last_viewed,
                "type": meta_type,
                "userTitle": user_name,
                "userId": user_id,
                "sectionType": sec_type,
                "sectionTitle": sec_name,
                "ratingKey": ratingkey,
                "thumb": "/library/metadata/" + str(ratingkey) + "/thumb",
                "art": "/library/metadata/" + str(ratingkey) + "/art"
            }

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(ratingkey) + "/banner/"

            results.append(dicts)
        close_connection(connection)
        return results
    else:
        Log.Error("Error connecting to DB!")


def fetch_cursor():
    cursor = None
    connection = None
    if os.environ["Loaded"]:
        import apsw
        Log.Debug("Shit, we got the librarys!")
        connection = apsw.Connection(os.environ['LIBRARY_DB'])
        cursor = connection.cursor()
    return [cursor, connection]


def close_connection(connection):
    if connection is not None:
        Log.Debug("Closing connection..")
        connection.close()
    else:
        Log.Debug("No connection to close!")


def vcr_ver():
    msvcr_map = {
        'msvcr120.dll': 'vc12',
        'msvcr130.dll': 'vc14'
    }
    try:
        import ctypes.util

        # Retrieve linked msvcr dll
        name = ctypes.util.find_msvcrt()

        # Return VC++ version from map
        if name not in msvcr_map:
            Log.Error('Unknown VC++ runtime: %r', name)
            return None

        return msvcr_map[name]
    except Exception as ex:
        Log.Error('Unable to retrieve VC++ runtime version: %s' % ex, exc_info=True)
        return None


def init_apsw():
    try:
        import apsw
    except ImportError:
        Log.Error("Shit, module not imported")
    pass


def insert_paths(distribution, libraries_path):

    # Retrieve system details
    system = SystemHelper.name()
    architecture = SystemHelper.architecture()

    if not architecture:
        Log.Debug('Unable to retrieve system architecture')
        return False

    Log.Debug('System: %r, Architecture: %r', system, architecture)

    # Build architecture list
    architectures = [architecture]

    if architecture == 'i686':
        # Fallback to i386
        architectures.append('i386')

    # Insert library paths
    found = False

    for arch in architectures + ['universal']:
        if insert_architecture_paths(libraries_path, system, arch):
            Log.Debug('Inserted libraries path for system: %r, arch: %r', system, arch)
            found = True

    # Display interface message if no libraries were found
    if not found:
        if distribution and distribution.get('name'):
            message = 'Unable to find compatible native libraries in the %s distribution' % distribution['name']
        else:
            message = 'Unable to find compatible native libraries'
        Log.Debug(message)

        # InterfaceMessages.add(60, '%s (system: %r, architecture: %r)', message, system, architecture)

    return found


def insert_architecture_paths(libraries_path, system, architecture):
    architecture_path = os.path.join(libraries_path, system, architecture)

    if not os.path.exists(architecture_path):
        Log.Debug("Arch path doesn't exist!!")
        return False

    # Architecture libraries
    Log.Debug("inserting libs path")
    PathHelper.insert(libraries_path, system, architecture)

    # System libraries
    if system == 'Windows':
        # Windows libraries (VC++ specific)
        insert_paths_windows(libraries_path, system, architecture)
    else:
        # Darwin/FreeBSD/Linux libraries
        insert_paths_unix(libraries_path, system, architecture)

    return True


def insert_paths_unix(libraries_path, system, architecture):
    # UCS specific libraries
    ucs = UNICODE_MAP.get(sys.maxunicode)
    Log.Debug('UCS: %r', ucs)

    if ucs:
        Log.Debug("inserting UCS path")
        PathHelper.insert(libraries_path, system, architecture, ucs)

    # CPU specific libraries
    cpu_type = SystemHelper.cpu_type()
    page_size = SystemHelper.page_size()

    Log.Debug('CPU Type: %r', cpu_type)
    Log.Debug('Page Size: %r', page_size)

    if cpu_type:
        Log.Debug("Inserting CPU Type path")
        PathHelper.insert(libraries_path, system, architecture, cpu_type)

        if page_size:
            Log.Debug("Page Size path")
            PathHelper.insert(libraries_path, system, architecture, '%s_%s' % (cpu_type, page_size))

    # UCS + CPU specific libraries
    if cpu_type and ucs:
        Log.Debug("CPU + UCS path")
        PathHelper.insert(libraries_path, system, architecture, cpu_type, ucs)

        if page_size:
            Log.Debug("And page size")
            PathHelper.insert(libraries_path, system, architecture, '%s_%s' % (cpu_type, page_size), ucs)


def insert_paths_windows(libraries_path, system, architecture):
    vcr = SystemHelper.vcr_version() or 'vc12'  # Assume "vc12" if call fails
    ucs = UNICODE_MAP.get(sys.maxunicode)

    Log.Debug('VCR: %r, UCS: %r', vcr, ucs)

    # VC++ libraries
    Log.Debug("Inserting vcr path")
    PathHelper.insert(libraries_path, system, architecture, vcr)

    # UCS libraries
    if ucs:
        Log.Debug("Inserting UCS path")
        PathHelper.insert(libraries_path, system, architecture, vcr, ucs)
