# ipfs-sqlite

CLI utility to backup and restore a SQLite database from IPFS. Requires local instance of GO-IPFS running

Arguments:

- [argName] = Optional
- (argName) = Required

<h4>Encrypted Database Example</h4>

- node index.js backup (sqlite db to backup) [existing backup CID] [secret encryption Key] [secret encryption IV]
- node index.js restore (existing backup CID) [secret encryption Key] [secret encryption IV]

<h4>Unencrypted Database Example</h4>

- node index.js backup --unencrypted (sqlite db to
  backup) [existing backup CID] [secret encryption Key] [secret encryption IV]
- node index.js restore (existing backup CID) [secret encryption Key] [secret encryption IV]

<h4>Unencrypted Northwind_small.sqlite Database Example</h4>

- ipfs-sqlite restore bafybeidlb5h4waumukod6lqoyqtuolettlim5xxwlqtgrq66l65kwqsrzu
- ipfs-sqlite backup test_assets/Northwind_small.sqlite bafybeidlb5h4waumukod6lqoyqtuolettlim5xxwlqtgrq66l65kwqsrzu
  --unencrypted