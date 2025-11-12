import asyncio
from aiohttp import web
import asyncpg
import os
from datetime import datetime, timezone
import re

def log(msg, level="INFO"):
    levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
    if level not in levels:
        level = "INFO"
    
    # Only log INFO or above (skip DEBUG if you want)
    if levels.index(level) >= 0:
        now_iso = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        print(f"[{now_iso}] [{level}] {msg}")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_CONN_POOL = None

LATEST_ANALYTICS = """(   
    SELECT DISTINCT ON (asset_id) *
    FROM analytics
    ORDER BY asset_id, server_time DESC)
"""
LATEST_BOOKS = """(
    SELECT DISTINCT ON (asset_id) *
    FROM books
    ORDER BY asset_id, server_time DESC
)
"""
LATEST_CHANGES = """(
    SELECT DISTINCT ON (asset_id) *
    FROM changes
    ORDER BY asset_id, server_time DESC
) 
"""
LATEST_TICK_CHANGES = """(
    SELECT DISTINCT ON (asset_id) *
    FROM tick_changes
    ORDER BY asset_id, server_time DESC
)
"""
ATTRIBUTE_LOOKUP = {
    "string": {
        "m.market_id",
        "m.condition_id",
        "m.negrisk_id",
        "m.asset_id",
        "b.asset_id",
        "c.asset_id",
        "tc.asset_id",
        "a.asset_id"
    },
    "numeric": {
        "m.row_index", "m.insert_time",
        "b.row_index", "b.resub_count", "b.insert_time", "b.server_time",
        "c.row_index", "c.resub_count", "c.insert_time", "c.server_time", "c.type",
        "tc.row_index", "tc.resub_count", "tc.insert_time", "tc.server_time",
        "a.row_index", "a.resub_count", "a.insert_time", "a.server_time", "a.ws_books_count",
        "a.ws_ambiguous_start_count", "a.ws_ambiguous_end_count", "a.bid_depth_diff",
        "a.ask_depth_diff", "a.bids_distance", "a.asks_distance",
        "a.ws_tick_changes_count", "a.ws_tick_changes", "a.tick_size_distance",
    },
}

def parse_attributes_by(attributes: str) -> (str, list):
    if attributes is None:
        return (None, None)
    if not attributes.strip():
        return ("", [])

    prefixes = set()
    terms = [t.strip() for t in attributes.split(",") if t.strip()]

    for term in terms:
        expr_part = term.split(" AS ")[0].strip()
        tokens = [t.strip() for t in re.split(r'(\+|\-)', expr_part) if t.strip()]
        for token in tokens:
            if re.fullmatch(r"[0-9]+(\.[0-9]+)?", token) or re.fullmatch(r"'.*'", token):
                continue
            if token not in ATTRIBUTE_LOOKUP["string"] and token not in ATTRIBUTE_LOOKUP["numeric"]:
                return (None, None)
            if token in ATTRIBUTE_LOOKUP["string"] and len(tokens) > 1:
                return (None, None)
            prefixes.add(token.split(".")[0])
    return (attributes, sorted(prefixes))


def parse_group_by(grouping: str) -> (str, list):
    if grouping is None:
        return (None, None)
    if not grouping.strip():
        return ("", [])
    terms = [t.strip() for t in grouping.split(",") if t.strip()]
    if not all(t in ATTRIBUTE_LOOKUP["string"] for t in terms):
        return (None, None)
    prefixes = list({t.split(".")[0] for t in terms})
    return (grouping, sorted(prefixes))


def parse_order_by(order: str) -> (str, list):
    if order is None:
        return (None, None)
    if not order.strip():
        return ("", [])
    tokens = [t.strip() for t in re.split(r'[\+\-]', order) if t.strip()]
    for t in tokens:
        if not (re.fullmatch(r"[0-9]+(\.[0-9]+)?", t) or t in ATTRIBUTE_LOOKUP["numeric"]):
            return (None, None)
    prefixes = list({t.split(".")[0] for t in tokens if "." in t})
    return (order, sorted(prefixes))


def parse_filter_by(filter_str: str) -> (str, list):
    if filter_str is None:
        return (None, None)
    if not filter_str.strip():
        return ("", [])

    tokens = re.findall(r"\(|\)|\bAND\b|\bOR\b|\bNOT\b|[^\s()]+", filter_str, re.IGNORECASE)
    stack = []
    expect_operand = True
    prefixes = set()
    i = 0

    while i < len(tokens):
        token = tokens[i].upper()
        if token == "(":
            stack.append("(")
            expect_operand = True
        elif token == ")":
            if not stack:
                return (None, None)
            stack.pop()
            expect_operand = False
        elif token in ("AND", "OR"):
            if expect_operand:
                return (None, None)
            expect_operand = True
        elif token == "NOT":
            if not expect_operand:
                return (None, None)
        else:
            # consume full condition
            cond_tokens = [tokens[i]]
            while i + 1 < len(tokens) and tokens[i + 1].upper() not in ("AND", "OR", "NOT", "(", ")"):
                i += 1
                cond_tokens.append(tokens[i])
            cond_str = " ".join(cond_tokens)

            string_match = re.fullmatch(r"([a-z]+\.[a-z_]+)\s*(=|!=)\s*'[^']*'", cond_str, re.IGNORECASE)
            null_match = re.fullmatch(r"([a-z]+\.[a-z_]+)\s+IS\s+NULL", cond_str, re.IGNORECASE)
            num_match = re.fullmatch(r"(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)", cond_str)

            if string_match:
                attr = string_match.group(1)
                if attr not in ATTRIBUTE_LOOKUP["string"]:
                    return (None, None)
                prefixes.add(attr.split(".")[0])
            elif null_match:
                attr = null_match.group(1)
                if attr not in ATTRIBUTE_LOOKUP["string"]:
                    return (None, None)
                prefixes.add(attr.split(".")[0])
            elif num_match:
                parts = [p.strip() for p in re.split(r'[\+\-]', num_match.group(1))] + \
                        [p.strip() for p in re.split(r'[\+\-]', num_match.group(3))]
                for p in parts:
                    if not (re.fullmatch(r"[0-9]+(\.[0-9]+)?", p) or p in ATTRIBUTE_LOOKUP["numeric"]):
                        return (None, None)
                    if "." in p:
                        prefixes.add(p.split(".")[0])
            else:
                return (None, None)
            expect_operand = False
        i += 1

    if expect_operand or stack:
        return (None, None)
    return (filter_str, sorted(prefixes))


def construct_query(
attributes : str, attribute_tables : list,
parsed_filter : str, filter_tables : list, 
parsed_order : str, order_tables : list,
parsed_group : str, group_tables : list,
ascending : bool, limit : int, offset : int,
include_count : bool
):  
    query = []
    if include_count:
        query.append("SELECT *, COUNT(*) OVER() AS total_count FROM (")

    query.append("SELECT")
    query.append(attributes)
    query.append("FROM")
    all_tables = set(filter_tables + order_tables + group_tables + attribute_tables)
    previous_table = ""
    if 'm' in all_tables:
        query.append("markets m")
        previous_table = 'm'
    if 'b' in all_tables:
        if previous_table:
            query.append(f"JOIN books b ON b.asset_id = {previous_table}.asset_id")
        else:
            query.append("books b")
        previous_table = 'b'
    if 'c' in all_tables:
        if previous_table:
            query.append(f"JOIN changes c ON c.asset_id = {previous_table}.asset_id")
        else:
            query.append("changes c")
        previous_table = 'c'
    if 'tc' in all_tables:
        if previous_table:
            query.append(f"JOIN tick_changes tc ON tc.asset_id = {previous_table}.asset_id")
        else:
            query.append("tick_changes tc")
        previous_table = 'tc'
    if 'a' in all_tables:
        if previous_table:
            query.append(f"JOIN analytics a ON a.asset_id = {previous_table}.asset_id")
        else:
            query.append("analytics a")
    
    if parsed_filter:
        query.append("WHERE")
        query.append(parsed_filter)
    
    if parsed_group:
        query.append("GROUP BY")
        query.append(parsed_group)
    
    if parsed_order:
        query.append("ORDER BY")
        query.append(parsed_order)
        if ascending:
            query.append("ASC")
        else:
            query.append("DESC")
    
    if include_count:
        query.append(") sub ")
    query.append(f"LIMIT {limit} OFFSET {offset}")
    query = " ".join(query)

    if __debug__:
        log(f"construct_query constructed query '{query}'", "DEBUG")
    return query
    
def parse_query(query : dict)->str:
    limit = query.get("limit", None)
    if not isinstance(limit, int) or limit <= 0:
        log(f"parse_query received invalid limit from query '{query}'", "WARNING")
        return None
    
    offset = query.get("offset", None)
    if not isinstance(offset, int) or offset < 0:
        log(f"parse_query received invalid offset from query '{query}'", "WARNING")
        return None
    
    ascending = query.get("ascending", None)
    if not isinstance(ascending, bool):
        log(f"parse_query received invalid ascending from query '{query}'", "WARNING")
        return None

    tables_involved = []

    attributes = query.get("attributes", None)
    (parsed_attributes, attribute_tables) = parse_attributes_by(attributes)
    if parsed_attributes is None:
        log(f"parse_query received invalid attributes from query '{query}'", "WARNING")
        return None
    if parsed_attributes == "":
        parsed_attributes = "m.asset_id"
        attribute_tables = ['m'] 
    
    filter_by = query.get("filter", None)
    (parsed_filter, filter_tables) = parse_filter_by(filter_by)
    if parsed_filter is None:
        log(f"parse_query received invalid filter from query '{query}'", "WARNING")
        return None

    order = query.get("order", None)
    (parsed_order, order_tables) = parse_order_by(order)
    if parsed_order is None:
        log(f"parse_query received invalid order from query '{query}'", "WARNING")
        return None
    
    grouping = query.get("grouping", None)
    (parsed_group, group_tables) = parse_group_by(grouping)
    if parsed_group is None:
        log(f"parse_query received invalid grouping from query '{query}'", "WARNING")
        return None
    
    return construct_query(
        parsed_attributes, attribute_tables,
        parsed_filter, filter_tables,
        parsed_order, order_tables,
        parsed_group, group_tables,
        ascending, limit, offset,
        True
    )

async def make_query(query : str)-> list:
    try:
        conn = await DB_CONN_POOL.acquire()
        return await conn.fetch(query)
    except Exception as e:
        log(f"make_query caught exception '{e}' with query '{query}'", "ERROR")
        raise
    finally:
        if conn is not None:
            await DB_CONN_POOL.release(conn)

async def handle_html(request: web.Request):
    path = os.path.join(BASE_DIR, "index.html")
    log(f"Serving {path} for html", "DEBUG")
    return web.FileResponse(path)

async def handle_script(request: web.Request):
    path = os.path.join(BASE_DIR, "main.js")
    log(f"Serving {path} for script", "DEBUG")
    return web.FileResponse(path)

async def handle_styles(request: web.Request):
    path = os.path.join(BASE_DIR, "styles.css")
    log(f"Serving {path} for styles", "DEBUG")
    return web.FileResponse(path)


async def handle_query(request: web.Request):
    try:
        json_body = await request.json()
        query = parse_query(json_body)
        if not query:
            log(f"handle_query failed to parse query '{json_body}'", "WARNING")
            return web.json_response({"error": "Invalid query"}, status=400)

        rows = await make_query(query)

        if __debug__:
            log(f"handle_query got rows '{rows}' from query", "DEBUG")
        if len(rows) == 0:
            all_rows_count = 0
        else:
            all_rows_count = rows[0]["total_count"]
        return web.json_response(
            {'all_rows_count': all_rows_count,
            'page_rows': [dict(r) for r in rows]})
    except Exception as e:
        log(f"handle_query caught exception '{e}' for request '{request}'", "ERROR")
        return web.json_response({"error": str(e)}, status=500)


async def main():
    global DB_CONN_POOL
    db_user = os.environ.get("POLY_DB_CLI")
    db_user_pw = os.environ.get("POLY_DB_CLI_PASS")
    socket_dir = os.environ.get("PG_SOCKET")
    DB_CONN_POOL = await asyncpg.create_pool(
        user=db_user,
        password=db_user_pw,
        database="polymarket",
        host=socket_dir,
        port=5432,
        min_size=1,
        max_size=3,
    )
    app = web.Application()
    app.add_routes([
        web.get("/", handle_html),
        web.get("/index", handle_html),
        web.get("/styles", handle_styles),
        web.get("/script", handle_script),
        web.post("/refresh", handle_query)
    ])
    await web._run_app(app, host="172.16.0.114", port=8080)

if __name__ == "__main__":
    asyncio.run(main())

