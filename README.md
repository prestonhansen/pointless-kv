# pointless-kv
`pointless-kv` is a for-fun key value store to help me learn Go, as well as implement some stuff myself that I normally take for granted in my day-to-day work.

## Features
- [x] Get and put values for keys
- [x] Persistent append-only storage backed with local files
- [x] Log compaction to help keep files small as many updates are made to keys
- [x] Indexing for fast lookups (uses a hash index - all keys need to fit in memory!)
- [ ] Transactions
- [ ] Alternate storage backends such as S3, GCS, ...
