# `services.models` — pgvector pool-shared registry contract

## Why this document exists

`services.retrieval.pathways:pathway_b_semantic` (the cosine-similarity
retrieval pathway) chooses between two ways of binding the seed vector
parameter to its SQL query. The choice is made per call, by
inspecting whether the asyncpg connection has the pgvector codec
registered:

```python
# services/retrieval/pathways.py:763
if _conn_has_vector_codec(conn):
    vec_param = numpy.asarray(vec, dtype="float32")  # binary, fast
else:
    vec_param = "[" + ",".join(...) + "]"            # text, slow
```

The check goes through `services.models.repo.PGVECTOR_REGISTERED_POOL_IDS`
— a process-wide set of `id(conn)` values that have had
`pgvector.asyncpg.register_vector` called on them.

If a pool feeds retrieval reads but the codec was never registered on
its connections, Pathway B falls back to the text path. That path
*works*, but defeats the purpose of HNSW indexing — which is why
production code (gateway, Think worker) registers the codec at pool
init.

The much more painful failure mode: a pool *partially* registers the
codec (some connections have it, some don't). asyncpg connections
have `__slots__`, and asyncpg's pool reuses connections across
acquisitions, so once one code path calls `register_vector(conn)` on
a checked-out connection, that connection is permanently mutated for
the rest of its lifetime in the pool. The next code path that tries
to bind a string `'[…]'::vector` literal on the same reused connection
hits a `could not convert string to float` error — confusing because
it has nothing to do with the literal. See
`tests/synthesis_harness/REPORT.md` §8 for the original diagnosis.

## The contract

Any new pool that shares connections with `ModelsRepo` (or with any
code that reads from the `models` table via Pathway B) MUST do one of:

1. **Recommended.** Pass `pgvector_pool_init` as the `init` callback
   when creating the pool:

   ```python
   from services.models.repo import pgvector_pool_init

   pool = await asyncpg.create_pool(
       dsn, init=pgvector_pool_init, min_size=..., max_size=...,
   )
   ```

   asyncpg invokes the init callback on every connection the pool
   ever produces — both the initial `min_size` set and any later
   expansions up to `max_size`. This is the only way to guarantee
   uniform codec registration.

2. **Manual init.** If your pool already has an init callback you
   can't replace, compose it: call `register_vector(conn)` AND add
   both `id(conn)` and `id(conn._con)` (the inner Connection
   object, if `conn` is a `PoolConnectionProxy`) to
   `PGVECTOR_REGISTERED_POOL_IDS`. Both ids are needed because
   Pathway B's check walks both.

3. **Post-hoc retrofit.** `register_pgvector_on_pool(pool)` walks
   the pool's currently-idle connections and registers the codec on
   each. It does NOT install an init callback, so connections
   spawned later (e.g. when the pool grows under load) will not
   be registered until `_ensure_vector_codec` happens to lazily
   register them on first use. Prefer option 1 unless you cannot
   change the pool constructor call.

4. **Don't share.** If your pool only does writes via raw SQL and
   never goes through ModelsRepo or retrieval, you don't need any
   of the above. But if you ever read from `models` via Pathway
   B's query, you do.

`pgvector_pool_init` and `PGVECTOR_REGISTERED_POOL_IDS` are the
public surface. The latter is exposed only because Pathway B reads
it directly — new code should not write to it manually unless you
have a reason to opt out of the init pattern.

## Existing call sites (all must stay in sync)

| Site                                              | Method  |
|---------------------------------------------------|---------|
| `services/gateway/db_bootstrap.py`                | manual init body |
| `tests/synthesis_harness/__main__.py`             | `init=pgvector_pool_init` |
| `services/models/repo.py:_ensure_vector_codec`    | per-conn lazy fallback |
| `services/retrieval/pathways.py:_conn_has_vector_codec` | reader |

If you grep for `register_vector` outside this list, that's a new
call site and the contract above applies to it.

## Why it isn't just `await register_vector(conn)` everywhere

asyncpg's pool grows on demand. A connection registered at acquire
time gets recycled; a *new* connection spawned to grow the pool
won't have the codec unless the pool's `init` callback installs it.
The `init` callback is the only place where you can guarantee
"every connection this pool ever produces will have the codec."
The helper wraps that contract.

## Testing the contract

`tests/synthesis_harness/cases_retrieval.py` exercises Pathway B end
to end. If anyone breaks the registry plumbing, retrieval tests
fail with the `could not convert string to float` error, which is
distinctive enough to short-circuit the diagnosis.
