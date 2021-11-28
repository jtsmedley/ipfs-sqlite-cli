## Project Name <!-- Add your project name here with format "Project Name"-->

IPFS-SQLite-CLI

## Category
infrastructure, developer tooling

## Project Description
IPFS-SQLite-CLI enables you to backup a SQLite database to a local IPFS GO instance.  
Once the initial backup is complete then each subsequent backup will be a much faster incremental backup of the database.

## Use of IPFS, Filecoin and Libp2p
This utility stores the database backup on IPFS and provides a single CID that you can pin with a pinning provider.

## Project Status
brainstorming: ideas to improve the CLI experience
under development: IPFS-SQLite-Browser (Utility to load SQLite databases into SQL.js and only load needed pages for queries)
shipped: Ability to backup and optionally encrypt a SQLite database to the IPFS network and restore it on any system connected to the public IPFS network.

## Target Audience
Developers: Ability to backup databases and use them for configuration.
Infrastructure: Ability to backup databases across IPFS securely and with incremental backup functionality.

## Github repo
https://github.com/jtsmedley/ipfs-sqlite-cli

## Docs
https://github.com/jtsmedley/ipfs-sqlite-cli/blob/main/README.md

## Team Info
Jason Smedley - Full Stack Embedded Development.  I work on this project in my spare time to help build a better environment for syncing SQLite databases.

## How the community can engage
* GitHub Discussion: https://github.com/jtsmedley/ipfs-sqlite-cli/discussions

## How to Contribute
Pull Requests are appreciated and any feedback on existing code is appreciated.  Rewriting this in GO is a current goal.
