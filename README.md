# ipfs-sqlite
 CLI utility to backup and restore a SQLite database from IPFS.  Requires local instance of GO-IPFS running

Arguments:
- [argName] = Optional
- (argName) = Required

- <h4>Encrypted Database Example</h4>
- node index.js backup [sqlite db to backup] [existing backup CID] [secret encryption Key] [secret encryption IV]
- node index.js restore (existing backup CID) [secret encryption Key] [secret encryption IV]
- <h4>Unencrypted Database Example</h4>
- node index.js backup --unencrypted [sqlite db to backup] [existing backup CID] [secret encryption Key] [secret encryption IV]
- node index.js restore --unencrypted (existing backup CID) [secret encryption Key] [secret encryption IV]
