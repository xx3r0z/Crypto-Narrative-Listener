import asyncio
import json
import logging
import os
import random
import sys

from collections import OrderedDict
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

load_dotenv()

X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

X_STREAM_URL = "https://api.x.com/2/tweets/search/stream"
X_RULES_URL = "https://api.x.com/2/tweets/search/stream/rules"

ACCOUNTS_FILE = "accounts.txt"

QUEUE_SIZE = 5000
DEDUP_CACHE_SIZE = 20000

MIN_BACKOFF_SECONDS = 1
MAX_BACKOFF_SECONDS = 120


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("x-stream-listener")


# --------------------------------------------------
# VALIDATION
# --------------------------------------------------

if not X_BEARER_TOKEN:
    raise RuntimeError("X_BEARER_TOKEN is missing from .env")

if not N8N_WEBHOOK_URL:
    raise RuntimeError("N8N_WEBHOOK_URL is missing from .env")


# --------------------------------------------------
# X AUTH
# --------------------------------------------------

X_HEADERS = {
    "Authorization": f"Bearer {X_BEARER_TOKEN}",
    "Content-Type": "application/json"
}


# --------------------------------------------------
# LOAD ACCOUNTS
# --------------------------------------------------

def load_accounts():

    with open(ACCOUNTS_FILE, "r", encoding="utf-8") as file:
        accounts = [
            line.strip().lstrip("@")
            for line in file.readlines()
            if line.strip()
        ]

    # Remove duplicates while maintaining order
    accounts = list(dict.fromkeys(accounts))

    logger.info("Loaded %s monitored accounts", len(accounts))

    return accounts


# --------------------------------------------------
# LRU DEDUPLICATION CACHE
# --------------------------------------------------

class PostDeduplicator:

    def __init__(self, max_size=20000):
        self.max_size = max_size
        self.cache = OrderedDict()

    def seen(self, post_id):

        if post_id in self.cache:
            self.cache.move_to_end(post_id)
            return True

        self.cache[post_id] = True

        if len(self.cache) > self.max_size:
            self.cache.popitem(last=False)

        return False


deduper = PostDeduplicator(DEDUP_CACHE_SIZE)


# --------------------------------------------------
# GET EXISTING X RULES
# --------------------------------------------------

async def get_existing_rules(client):

    response = await client.get(
        X_RULES_URL,
        headers=X_HEADERS
    )

    response.raise_for_status()

    payload = response.json()

    return payload.get("data", [])


# --------------------------------------------------
# SYNC ACCOUNT RULES
# --------------------------------------------------

async def sync_rules(client, accounts):

    logger.info("Checking existing X stream rules...")

    existing_rules = await get_existing_rules(client)

    existing_by_tag = {
        rule.get("tag"): rule
        for rule in existing_rules
        if rule.get("tag")
    }

    desired_rules = {}

    for username in accounts:

        tag = f"account:{username.lower()}"

        rule = (
            f"from:{username} "
            "-is:retweet "
            "-is:reply"
        )

        desired_rules[tag] = rule

    # ---------------------------------------------
    # DELETE obsolete / changed rules
    # ---------------------------------------------

    delete_ids = []

    for tag, rule in existing_by_tag.items():

        # Only manage rules created by our application
        if not tag.startswith("account:"):
            continue

        desired_value = desired_rules.get(tag)

        if desired_value is None:
            delete_ids.append(rule["id"])

        elif desired_value != rule["value"]:
            delete_ids.append(rule["id"])

    if delete_ids:

        logger.info("Deleting %s obsolete rules", len(delete_ids))

        response = await client.post(
            X_RULES_URL,
            headers=X_HEADERS,
            json={
                "delete": {
                    "ids": delete_ids
                }
            }
        )

        response.raise_for_status()

    # ---------------------------------------------
    # ADD missing rules
    # ---------------------------------------------

    rules_to_add = []

    for tag, value in desired_rules.items():

        existing = existing_by_tag.get(tag)

        if existing and existing["value"] == value:
            continue

        rules_to_add.append({
            "value": value,
            "tag": tag
        })

    if not rules_to_add:

        logger.info("All X stream rules already synchronized.")
        return

    logger.info("Adding %s stream rules...", len(rules_to_add))

    # Small batches make failures easier to diagnose.
    batch_size = 20

    for index in range(0, len(rules_to_add), batch_size):

        batch = rules_to_add[index:index + batch_size]

        response = await client.post(
            X_RULES_URL,
            headers=X_HEADERS,
            json={
                "add": batch
            }
        )

        if response.status_code >= 400:

            logger.error(
                "Rule creation failed: %s",
                response.text
            )

            response.raise_for_status()

        logger.info(
            "Added rule batch %s-%s",
            index + 1,
            index + len(batch)
        )


# --------------------------------------------------
# NORMALIZE X EVENT
# --------------------------------------------------

def normalize_event(event):

    post = event.get("data")

    if not post:
        return None

    post_id = post.get("id")

    if not post_id:
        return None

    users = (
        event
        .get("includes", {})
        .get("users", [])
    )

    author = None

    for user in users:
        if user.get("id") == post.get("author_id"):
            author = user
            break

    username = None

    if author:
        username = author.get("username")

    post_url = None

    if username:
        post_url = (
            f"https://x.com/{username}/status/{post_id}"
        )

    matching_rules = event.get(
        "matching_rules",
        []
    )

    return {
        "source": "x_filtered_stream",

        "received_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "post": {
            "id": post_id,
            "text": post.get("text"),
            "author_id": post.get("author_id"),
            "created_at": post.get("created_at"),
            "lang": post.get("lang"),
            "public_metrics": post.get(
                "public_metrics"
            ),
            "entities": post.get("entities"),
            "referenced_tweets": post.get(
                "referenced_tweets"
            ),
            "url": post_url
        },

        "author": author,

        "matching_rules": matching_rules,

        # Keep original X payload.
        # Very useful while debugging.
        "raw": event
    }


# --------------------------------------------------
# FORWARD TO N8N
# --------------------------------------------------

async def forward_to_n8n(client, payload):

    attempts = 0

    while attempts < 5:

        attempts += 1

        try:

            response = await client.post(
                N8N_WEBHOOK_URL,
                json=payload,
                timeout=10
            )

            response.raise_for_status()

            return True

        except Exception as error:

            logger.warning(
                "n8n delivery failed attempt %s: %s",
                attempts,
                error
            )

            await asyncio.sleep(
                min(2 ** attempts, 30)
            )

    logger.error(
        "Could not forward post %s to n8n",
        payload["post"]["id"]
    )

    return False


# --------------------------------------------------
# QUEUE WORKER
# --------------------------------------------------

async def processing_worker(queue):

    async with httpx.AsyncClient() as client:

        while True:

            payload = await queue.get()

            try:

                await forward_to_n8n(
                    client,
                    payload
                )

            except Exception:

                logger.exception(
                    "Unexpected processing worker error"
                )

            finally:

                queue.task_done()


# --------------------------------------------------
# X STREAM
# --------------------------------------------------

async def consume_stream(queue):

    params = {

        "tweet.fields": (
            "id,text,author_id,created_at,"
            "lang,public_metrics,"
            "referenced_tweets,entities"
        ),

        "expansions": (
            "author_id,"
            "attachments.media_keys"
        ),

        "user.fields": (
            "id,name,username,"
            "public_metrics,verified"
        ),

        "media.fields": (
            "media_key,type,url,"
            "preview_image_url,alt_text"
        )
    }

    backoff = MIN_BACKOFF_SECONDS

    timeout = httpx.Timeout(
        connect=20,
        read=30,
        write=20,
        pool=20
    )

    while True:

        try:

            logger.info(
                "Connecting to X Filtered Stream..."
            )

            async with httpx.AsyncClient(
                timeout=timeout
            ) as client:

                async with client.stream(
                    "GET",
                    X_STREAM_URL,
                    headers=X_HEADERS,
                    params=params
                ) as response:

                    if response.status_code != 200:

                        body = await response.aread()

                        logger.error(
                            "X stream returned %s: %s",
                            response.status_code,
                            body.decode(
                                "utf-8",
                                errors="ignore"
                            )
                        )

                        raise RuntimeError(
                            f"Stream HTTP "
                            f"{response.status_code}"
                        )

                    logger.info(
                        "Connected successfully to X."
                    )

                    # Reset backoff after successful connection
                    backoff = MIN_BACKOFF_SECONDS

                    async for line in response.aiter_lines():

                        # X sends blank heartbeat lines
                        if not line:
                            continue

                        try:

                            event = json.loads(line)

                        except json.JSONDecodeError:

                            logger.warning(
                                "Invalid JSON received: %s",
                                line
                            )

                            continue

                        # X may send control/error messages
                        if "data" not in event:

                            logger.warning(
                                "Non-post stream event: %s",
                                event
                            )

                            continue

                        payload = normalize_event(
                            event
                        )

                        if not payload:
                            continue

                        post_id = payload["post"]["id"]

                        # De-duplicate
                        if deduper.seen(post_id):

                            logger.debug(
                                "Duplicate ignored: %s",
                                post_id
                            )

                            continue

                        username = (
                            payload
                            .get("author", {})
                            .get("username")
                            if payload.get("author")
                            else "unknown"
                        )

                        logger.info(
                            "NEW POST | @%s | %s",
                            username,
                            post_id
                        )

                        # Do not wait for n8n here.
                        # Keep reading X as quickly as possible.
                        try:

                            queue.put_nowait(
                                payload
                            )

                        except asyncio.QueueFull:

                            logger.critical(
                                "Processing queue FULL. "
                                "Post %s could not be queued.",
                                post_id
                            )

        except asyncio.CancelledError:
            raise

        except Exception as error:

            logger.error(
                "X stream disconnected: %s",
                error
            )

            jitter = random.uniform(
                0,
                backoff * 0.25
            )

            wait_time = backoff + jitter

            logger.info(
                "Reconnecting in %.1f seconds...",
                wait_time
            )

            await asyncio.sleep(wait_time)

            backoff = min(
                backoff * 2,
                MAX_BACKOFF_SECONDS
            )


# --------------------------------------------------
# MAIN
# --------------------------------------------------

async def main():

    accounts = load_accounts()

    if not accounts:
        logger.error(
            "No accounts found in accounts.txt"
        )
        sys.exit(1)

    async with httpx.AsyncClient() as client:

        await sync_rules(
            client,
            accounts
        )

    queue = asyncio.Queue(
        maxsize=QUEUE_SIZE
    )

    # Two workers sending to n8n
    workers = [
        asyncio.create_task(
            processing_worker(queue)
        )
        for _ in range(2)
    ]

    stream_task = asyncio.create_task(
        consume_stream(queue)
    )

    logger.info(
        "Crypto Narrative X Listener running."
    )

    try:

        await stream_task

    finally:

        for worker in workers:
            worker.cancel()


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Listener stopped.")