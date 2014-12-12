# Elasticsearch RethinkDB River

This is a plugin for [Elasticsearch][] that pulls in documents from [RethinkDB][], then indexes new/updated/deleted documents in real time.
Elasticsearch gives you the ability to do [full-text search][].

You might want this if you'd like to be able to search RethinkDB documents using queries like:
  - get all documents that contain the phrase X
  - retrieve the first 10 docs that roughly match the phrase X, ignoring common words like "the" and "a"

[Elasticsearch]: http://www.elasticsearch.org
[RethinkDB]: http://rethinkdb.com
[full-text search]: http://en.wikipedia.org/wiki/Full_text_search

## Installation

First off, you need [Elasticsearch 1.3][] running on [Java 8][] for this to work.
Once that's in place, you can install the plugin with:

[Elasticsearch 1.3]: http://www.elasticsearch.org/overview/elkdownloads/
[Java 8]: http://www.oracle.com/technetwork/java/javase/downloads/index.html

```
elasticsearch-plugin --install river-rethinkdb --url http://goo.gl/JmMwTf
```


__Note__: Depending on how you've installed Elasticsearch, you may need to become the elasticsearch user to install the plugin.

## Quickstart

If you want to index the table `posts` in the database `blog`, this is all you need to do:

```bash
$ curl -XPUT localhost:9200/_river/rethinkdb/_meta -d '{
   "type":"rethinkdb",
   "rethinkdb": {
     "databases": {"blog": {"posts": {"backfill": true}}},
     "host": "localhost",
     "port": 28015
   }}'
```

Now you'll have a new index called `blog` and a type called `posts` which you can query:

```bash
$ curl localhost:9200/blog/posts/_search?q=*:*
```

Elasticsearch's default port is 9200.
RethinkDB's default port is 28015.
You may want to brush up on [how to query Elasticsearch][].

[how to query Elasticsearch]: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-search.html

## Details

Rivers are a kind of plugin for Elasticsearch (ES) that sync external data sources with Elasticsearch's indexes.
ES indexes are similar to RethinkDB's databases, and ES types are similar to RethinkDB's tables.
Every index can have zero or more types, and every type can have zero or more documents.
To configure the river, you create a document in the `_river` index, which is a magical index ES watches for configuration info.

```bash
$ curl -XPUT localhost:9200/_river/rethinkdb/_meta
```

This creates a new document in the `rethinkdb` type with the id `_meta`.
At a minimum, the `_meta` document needs a key with the `type` field set to `"rethinkdb"`.
You'll also want to put a `rethinkdb` key with a document that contains these keys:

- `host`: RethinkDB server hostname, defaults to `"localhost"`
- `port`: RethinkDB server port, defaults to 28015,
- `auth_key`: RethinkDB server auth key, defaults to the empty string
- `databases`: A document containing one subdocument per database
  - `<dbName>`: The name of a database in RethinkDB. Must have a table specified as well.
    - `<tableName>`: The name of a RethinkDB table to watch
      - `backfill`: Whether to backfill existing documents or just watch for new ones, defaults to true.
      - `index`: What ES index to send documents from this table to, defaults to `<dbName>`
      - `type`: What ES type to send documents from this table to, defaults to `<tableName>`

You can specify as many databases and tables to watch as you'd like.

Here's a larger example that indexes `blog.posts` and `blog.comments` with the defaults plugged in:

```javascript
// localhost:9200/_river/rethinkdb/_meta
{
  "type": "rethinkdb",
  "rethinkdb": {
    "host": "localhost",
    "port": 28015,
    "auth_key": "",
    "databases": {
      "blog": {
        "posts": {
          "backfill": true,
          "index": "blog",
          "type": "posts",
        },
        "comments": {
          "backfill": true,
          "index": "blog",
          "type": "comments",
        }
      }
    }
  }
}
```

After the river backfills documents for a given table, it will change the `backfill` setting to `false`.
This way, the next time the Elasticsearch server restarts, it won't re-pull all documents from RethinkDB again.

## OK, I've queried Elasticsearch, what do I do now?

The documents are stored in Elasticsearch with the same id as the RethinkDB uses for it, so you can easily retrieve the original document.

For example, if you query your lorem ipsum blog posts for any that have the word "cupiditate" in the body:

```
$ curl localhost:9200/blog/posts/_search?q=body:cupiditate
```

You'll get results that look like:

```javascript
{
    "_shards": {
        "failed": 0,
        "successful": 1,
        "total": 1
    },
    "hits": {
        "hits": [
            {
                "_id": "261f4990-627b-4844-96ed-08b182121c5e",
                "_index": "blog",
                "_score": 1.0,
                "_source": {
                    "body": "cupiditate quo est a modi nesciunt soluta\nipsa voluptas",
                    "id": "261f4990-627b-4844-96ed-08b182121c5e",
                    "title": "at nam consequatur ea labore ea harum",
                    "userId": 10.0
                },
                "_type": "posts"
            }
        ],
        "max_score": 1.0,
        "total": 1
    },
    "timed_out": false,
    "took": 6
}
```

Now, you can fetch the original document from RethinkDB using:

```python
r.db('blog').table('posts').get('261f4990-627b-4844-96ed-08b182121c5e').run(conn)
```



## Caveats

Currently, there's no way to guarantee that no documents are lost if the river loses connection with the RethinkDB server.
The only way to be sure is to backfill every time, and this will still miss deleted documents.
In the future, RethinkDB will support changefeeds that accept a timestamp.
When that is implemented, this plugin will be able to ensure no documents are lost during disconnections.
