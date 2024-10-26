from collections import defaultdict

from atproto import models

from server.whitelist import generate_whitelist_ids
from server.logger import logger
from server.database import db, Post

import time

WHITELIST = generate_whitelist_ids()
logger.info(f"Whitelist: {WHITELIST}")
WHITELIST_UPDATE_INTERVAL = 60 * 5
PREV_WHITELIST_UPDATE = time.time()


def update_whitelist():
    global PREV_WHITELIST_UPDATE

    if time.time() - PREV_WHITELIST_UPDATE < WHITELIST_UPDATE_INTERVAL:
        return

    PREV_WHITELIST_UPDATE = time.time()

    global WHITELIST
    WHITELIST = generate_whitelist_ids()
    logger.info(f"Updated whitelist: {WHITELIST}")


def operations_callback(ops: defaultdict) -> None:
    # Here we can filter, process, run ML classification, etc.
    # After our feed alg we can save posts into our DB
    # Also, we should process deleted posts to remove them from our DB and keep it in sync

    posts_to_create = []
    for created_post in ops[models.ids.AppBskyFeedPost]["created"]:
        author = created_post["author"]
        record = created_post["record"]

        # print all texts just as demo that data stream works
        post_with_images = isinstance(record.embed, models.AppBskyEmbedImages.Main)
        inlined_text = record.text.replace("\n", " ")

        reply_root = reply_parent = None
        if record.reply:
            reply_root = record.reply.root.uri
            reply_parent = record.reply.parent.uri

        # only alf-related posts
        if author in WHITELIST and not record.reply:
            logger.info(
                f"NEW POST "
                f"[CREATED_AT={record.created_at}]"
                f"[AUTHOR={author}]"
                f"[WITH_IMAGE={post_with_images}]"
                f": {inlined_text}"
            )
            logger.info(f"{created_post}")

            post_dict = {
                "uri": created_post["uri"],
                "cid": created_post["cid"],
                "reply_parent": reply_parent,
                "reply_root": reply_root,
            }
            posts_to_create.append(post_dict)

    posts_to_delete = ops[models.ids.AppBskyFeedPost]["deleted"]
    if posts_to_delete:
        post_uris_to_delete = [post["uri"] for post in posts_to_delete]
        Post.delete().where(Post.uri.in_(post_uris_to_delete))
        # logger.info(f"Deleted from feed: {len(post_uris_to_delete)}")

    if posts_to_create:
        with db.atomic():
            for post_dict in posts_to_create:
                Post.create(**post_dict)
        logger.info(f"Added to feed: {len(posts_to_create)}")

    update_whitelist()
