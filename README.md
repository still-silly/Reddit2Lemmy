# Reddit2Lemmy

Imports JSON archives produced by BDFR into a local Lemmy database.

The importer processes files sequentially, upserts each archive object using a stable source ID, and wraps each file in a database transaction. A failed file is reported and does not prevent the remaining archive from being attempted.

## Requirements

- Rust 1.85 or newer
- A running Lemmy instance using PostgreSQL
- A BDFR archive containing JSON post files

The importer uses Lemmy’s database schema crate directly, so its lemmy_db_schema and lemmy_db_views versions should match the target Lemmy installation.

## Configuration

Set the same database environment variable used by Lemmy:

    export LEMMY_INITIALIZE_WITH_DEFAULT_SETTINGS=1
    export LEMMY_DATABASE_URL='postgres://lemmy:password@127.0.0.1/lemmy'

Imported identities receive randomly generated, discarded passwords. They are mirror identities, not intended login accounts.

## Usage

    cargo run --release -- import /path/to/bdfr/archive

Useful options:

    --gen-users-only       create mirror users without importing posts
    --dont-mark-nsfw       do not carry Reddit's NSFW flag into Lemmy
    --username-override    map authors to one configured username
    --skip-low-score       skip posts and comment subtrees scoring below 1
    --only-progress        show progress and errors without normal import logs
    --verbose              enable debug logging

Imports are always sequential. The legacy --no-tasks option is accepted for compatibility but has no effect.

## Behavior

Posts and comments use deterministic synthetic ActivityPub IDs derived from their Reddit IDs. Re-running an archive updates the existing imported objects instead of deleting and recreating them. Votes are represented with a shared, cached pool of synthetic local users and are upserted in batches.
