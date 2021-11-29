#!/usr/bin/env node
const Database = require("better-sqlite3"),
  fs = require("fs/promises"),
  Bottleneck = require("bottleneck"),
  hasha = require("hasha"),
  _ = require("lodash"),
  customCrypto = require("./crypto"),
  { program } = require("@caporal/core"),
  path = require("path"),
  { CID } = require("multiformats/cid"),
  dagPB = require("@ipld/dag-pb"),
  { UnixFS } = require("ipfs-unixfs"),
  fse = require("fs-extra");

// Backup Each Page in Order
const dbBackupLimiter = new Bottleneck({
  maxConcurrent: 1,
  highWater: 1,
  strategy: Bottleneck.strategy.LEAK,
});
const savePageLimiter = new Bottleneck({
  maxConcurrent: 1000,
});
const restorePageLimiter = new Bottleneck({
  maxConcurrent: 1,
});
const backupDatabaseSectionLimiter = new Bottleneck({
  maxConcurrent: 10,
});

class IPFSSQLite {
  #encryptionEnabled = true;
  pageLinks = [];
  successfulPageSaves = 0;
  sectionHashes = [];

  constructor({
    customKey,
    customIV,
    embeddedIPFS = false,
    unencrypted = false,
  }) {
    if (unencrypted === false) {
      this.encryptionHelper = new customCrypto({
        customKey: customKey,
        customIV: customIV,
      });
    } else {
      this.#encryptionEnabled = false;
    }

    this.IPFS = embeddedIPFS ? require("ipfs") : require("ipfs-http-client");
  }

  async parseWriteAheadLog() {
    try {
      this.dbWALHandle = await fs.open(`${this.dbFilePath}-wal`, "r");
    } catch {
      console.log("No WAL Found");
      return false;
    }

    let pagesInWAL = {},
      pagesAffected = new Set();

    let currentWALBuffer = Buffer.from(
      (await this.dbWALHandle.readFile()).buffer
    );

    if (currentWALBuffer.byteLength === 0) {
      return false;
    }

    let WALHeader = currentWALBuffer.subarray(0, 32);

    let writeAheadLogHeader = {
      magicNumber: Number(WALHeader.readUIntBE(0, 4)).toString(16),
      formatVersion: WALHeader.readUIntBE(4, 4),
      pageSize: WALHeader.readUIntBE(8, 4),
      checkpointSequenceNumber: WALHeader.readUIntBE(12, 4),
      salt1: WALHeader.readUIntBE(16, 4),
      salt2: WALHeader.readUIntBE(20, 4),
      checksum1: WALHeader.readUIntBE(24, 4),
      checksum2: WALHeader.readUIntBE(28, 4),
    };

    //Get Page Size
    let WALFrameSize = writeAheadLogHeader.pageSize + 24;

    //Parse all frames
    let currentWALFrames = currentWALBuffer.subarray(32);
    let currentWALFrameCount = currentWALFrames.byteLength / WALFrameSize;

    for (
      let WALFrameNumber = 0;
      WALFrameNumber < currentWALFrameCount;
      WALFrameNumber++
    ) {
      let frameStart = WALFrameNumber * WALFrameSize,
        frameStop = frameStart + WALFrameSize;
      let currentFrame = currentWALFrames.subarray(frameStart, frameStop);

      let framePageNumber = currentFrame.readUIntBE(0, 4);
      let salt1 = currentFrame.readUIntBE(8, 4),
        salt2 = currentFrame.readUIntBE(12, 4);

      if (typeof pagesInWAL[`${salt1}-${salt2}`] === "undefined") {
        pagesInWAL[`${salt1}-${salt2}`] = new Set();
      }

      pagesInWAL[`${salt1}-${salt2}`].add(framePageNumber);
      pagesAffected.add(framePageNumber);
    }

    await this.dbWALHandle.close();

    return {
      header: writeAheadLogHeader,
      pageSets: pagesInWAL,
      pagesAffected: pagesAffected,
    };
  }

  async backupDatabase({ dbFilePath, backupStateCID, pagesChanged }) {
    try {
      console.log(`Connecting to IPFS`);
      // Wait for the IPFS Client to Initialize
      this.ipfsClient = await this.ipfsClient;
      console.log(`Starting Backup of File: ${dbFilePath}`);
      this.ipfsClient = await this.IPFS.create();

      this.dbFilePath = dbFilePath;
      this.dbName = path.basename(dbFilePath);

      this.backupState = {};
      //Get existing configuration if passed
      if (_.isString(backupStateCID)) {
        console.log(
          `Fetching Existing Backup State from CID: ${backupStateCID}`
        );
        let backupStateBlock = await this.ipfsClient.dag.get(
          CID.parse(backupStateCID)
        );
        this.backupFileInfo = UnixFS.unmarshal(backupStateBlock.value.Data);
        this.backupState = JSON.parse(
          new TextDecoder().decode(this.backupFileInfo.data)
        );
        this.sectionHashes = this.backupState.Hashes.Sections;

        let pageNumber = 0;
        for (let page of backupStateBlock.value.Links) {
          this.pageLinks[pageNumber] = page;
          pageNumber++;
        }

        //Add current version to history
        this.backupState.Versions.push({
          CreatedOn: this.backupState.CreatedOn,
          cid: backupStateCID,
        });
      }

      console.log(`Opening Database File: ${this.dbFilePath}`);
      // Open Database in WAL mode
      this.db = new Database(this.dbFilePath);
      this.db.pragma("journal_mode = WAL");
      // Wait for the ReadStream to Initialize
      this.dbFileHandle = await fs.open(this.dbFilePath, "r");
      // Parse DB Header
      this.dbHeader = await this.parseHeader();
      // Fetch Database File Stats
      this.dbStats = await this.dbFileHandle.stat();
      // Calculate Page Count
      this.dbStats.pageCount =
        this.dbStats.size / this.dbHeader["Page Size in Bytes"];
      // Ensure Lock Table Exists
      this.#createLockTable();
      // Lock Database for Backup
      this.#lockDatabase();

      console.log(`Generating Hash of Database File: ${dbFilePath}`);
      let fileHash = await hasha.fromFile(this.dbFilePath, {
        encoding: "hex",
        algorithm: "sha256",
      });

      // Return true immediately if values match
      if (
        typeof pagesChanged === "undefined" &&
        _.isString(this.backupState?.Hashes?.File) &&
        this.backupState.Hashes.File === fileHash
      ) {
        console.log(
          `No changes detected since last sync for database file: ${this.dbFilePath}`
        );
        return;
      }
      console.log(`File ${this.dbFilePath} has changed since last backup`);

      let maxSectionSize = 1000 * this.dbHeader["Page Size in Bytes"]; // 1000 Pages of Database
      let pagesInSection = Math.floor(
        maxSectionSize / this.dbHeader["Page Size in Bytes"]
      );
      let sectionSize = pagesInSection * this.dbHeader["Page Size in Bytes"];
      let sectionCount = Math.ceil(this.dbStats.size / sectionSize);

      let sectionsChanged = new Set();
      if (typeof pagesChanged !== "undefined" && pagesChanged.length > 0) {
        pagesChanged.forEach((page) => {
          sectionsChanged.add(page % sectionSize);
        });
      }

      let sectionsInProgress = [];
      for (
        let sectionNumber = 0;
        sectionNumber < sectionCount;
        sectionNumber++
      ) {
        //Skip sections that haven't been changed
        if (
          sectionsChanged.length > 0 &&
          sectionsChanged.has(sectionNumber) === false
        ) {
          continue;
        }

        const sectionWorker = await backupDatabaseSectionLimiter.schedule(() =>
          this.#backupDatabaseSection(
            sectionNumber,
            sectionSize,
            pagesInSection,
            pagesChanged
          )
        );

        sectionsInProgress.push(sectionWorker);
      }
      await Promise.all(sectionsInProgress);

      await this.dbFileHandle.close();
      try {
        let backupFileSettings = {
          type: "file",
          data: {
            Name: this.dbName,
            Hashes: {
              File: fileHash,
              Sections: this.sectionHashes,
            },
            Versions: [],
            CreatedOn: Date.now(),
          },
          blockSizes: this.pageLinks.map(() => {
            return this.dbHeader["Page Size in Bytes"];
          }),
        };

        //Merge existing versions with new reference version
        backupFileSettings.data.Versions = _.uniq(
          backupFileSettings.data.Versions.concat(
            this.backupState.Versions || []
          )
        );
        backupFileSettings.data = Buffer.from(
          JSON.stringify(backupFileSettings.data)
        );

        this.backupFile = new UnixFS(backupFileSettings);

        const cid = await this.ipfsClient.dag.put(
          dagPB.prepare({
            Data: this.backupFile.marshal(),
            Links: this.pageLinks,
          }),
          {
            format: "dag-pb",
            hashAlg: "sha2-256",
          }
        );

        if (_.isString(backupStateCID)) {
          if (backupStateCID !== cid.toString()) {
            await this.ipfsClient.pin.add(cid);
            try {
              await this.ipfsClient.pin.rm(CID.parse(backupStateCID));
            } catch (e) {
              console.error(e.message);
            }
          } else {
            console.log(`Backup Configuration did not change!`);
          }
        } else {
          await this.ipfsClient.pin.add(cid);
        }

        console.log(`Database Backed Up to CID [${cid.toV1().toString()}]`);
        return cid.toV1().toString();
      } catch (err) {
        console.error(err.message);
      }
    } catch (err) {
      console.error(err.message);
    } finally {
      await this.dbFileHandle.close();

      // Unlock Database after Backup
      this.#unlockDatabase();
      this.db.close();

      console.log(`Backup Completed of File: ${this.dbFilePath}`);
    }
  }

  async parseHeader() {
    let headerBuffer = (
      await this.dbFileHandle.read(Buffer.alloc(100), 0, 100, 0)
    ).buffer;
    return {
      "Header String": headerBuffer.toString("utf8", 0, 16),
      "Page Size in Bytes": headerBuffer.readUIntBE(16, 2),
      "File Format write Version": headerBuffer.at(18),
      "File Format read Version": headerBuffer.at(19),
      "Bytes Reserved at the end of Each Page": headerBuffer.at(20),
      "Max Embedded Payload Fraction": headerBuffer.at(21),
      "Min Embedded Payload Fraction": headerBuffer.at(22),
      "Min Leaf Payload Fraction": headerBuffer.at(23),
      "File Change Counter": headerBuffer.readUIntBE(24, 4),
      "Reserved for Future Use": headerBuffer.subarray(28, 32),
      "First Freelist Page": headerBuffer.readUIntBE(32, 4),
      "Number of Freelist Pages": headerBuffer.readUIntBE(36, 4),
      "154 Byte Meta Values": headerBuffer.subarray(40, 80),
    };
  }

  async #backupDatabaseSection(
    sectionNumber,
    sectionSize,
    pagesInSection,
    pagesChanged
  ) {
    try {
      console.log(`Parsing Section ${sectionNumber}`);
      let pagesInProgress = [];
      let sectionBuffer = (
        await this.dbFileHandle.read({
          buffer: Buffer.alloc(sectionSize),
          offset: 0,
          length: sectionSize,
          position: sectionNumber * sectionSize,
        })
      ).buffer;

      let sectionHash = await hasha.async(sectionBuffer, {
        encoding: "hex",
        algorithm: "sha256",
      });

      let existingHash = undefined;
      if (_.isArray(this.backupState?.Hashes?.Sections)) {
        existingHash = this.backupState.Hashes.Sections[sectionNumber];
      }

      // Return true immediately if values match
      if (_.isString(existingHash) && existingHash === sectionHash) {
        console.log(
          `Section ${sectionNumber} has not changed since last backup`
        );
        return;
      }

      for (
        let frameNumber = 0, pageNumber = sectionNumber * pagesInSection;
        frameNumber < pagesInSection && pageNumber < this.dbStats.pageCount;
        frameNumber++, pageNumber++
      ) {
        if (
          typeof pagesChanged === "object" &&
          pagesChanged.has(pageNumber + 1) === false
        ) {
          continue;
        }

        let pageBuffer = sectionBuffer.subarray(
          frameNumber * this.dbHeader["Page Size in Bytes"],
          (frameNumber + 1) * this.dbHeader["Page Size in Bytes"]
        );

        let page = {
          index: pageNumber,
          buffer: pageBuffer,
        };

        let savePageWorker = savePageLimiter.schedule(() => {
          return this.#savePage(page);
        });

        savePageWorker.then((savedPage) => {
          this.successfulPageSaves++;
        });

        pagesInProgress.push(savePageWorker);
      }
      await Promise.all(pagesInProgress);
      try {
        this.sectionHashes[sectionNumber] = sectionHash;
      } catch (err) {
        console.error(err.message);
      }
    } catch (err) {
      console.error(err.message);
    }
  }

  async #savePage(page) {
    let existingHash = this.pageLinks[page.index] || undefined;

    // Write Page to IPFS
    let contentToWrite = page.buffer;

    let encryptedContent;
    if (this.#encryptionEnabled === true) {
      encryptedContent = this.encryptionHelper.encrypt(page.buffer);
      contentToWrite = Buffer.from(encryptedContent.content);
    }

    // Return true immediately if values match
    if (_.isObject(existingHash)) {
      page.hash = await this.ipfsClient.add(
        {
          path: `${this.dbName}-${page.index}.page`,
          content: contentToWrite,
        },
        {
          cidVersion: 1,
          onlyHash: true,
        }
      );
      if (existingHash.Hash.toString() === page.hash.cid.toString()) {
        return true;
      } else {
        console.log(`Page ${page.index + 1} has changed since last backup`);
      }
    }

    let uploadedPage = await this.ipfsClient.add(
      {
        path: `${page.index}.page`,
        content: contentToWrite,
      },
      {
        cidVersion: 1,
        pin: false,
      }
    );

    try {
      // this.pageLinks[page.index] = new dagPB.createLink(`page-${page.index}`, contentToWrite.byteLength, uploadedPage.cid)
      this.pageLinks[page.index] = uploadedPage.cid;
    } catch (err) {
      console.error(err.message);
    }

    console.log(
      `Uploaded Page ${page.index + 1}/${this.dbStats.pageCount} (${(
        (100 / this.dbStats.pageCount) *
        (page.index + 1)
      ).toFixed(2)}%) to IPFS at CID [${uploadedPage.cid.toString()}]`
    );
    return page;
  }

  async restore(backupStateCID, restorePath) {
    console.log(`Connecting to IPFS`);
    // Wait for the IPFS Client to Initialize
    this.ipfsClient = await this.IPFS.create();

    this.backupState = {};
    //Get existing configuration if passed
    if (_.isString(backupStateCID) === false) {
      throw new Error(`Invalid CID`);
    }

    console.log(`Fetching Existing Backup State from CID: ${backupStateCID}`);
    let backupStateBlock = await this.ipfsClient.dag.get(
      CID.parse(backupStateCID)
    );
    this.backupFileInfo = UnixFS.unmarshal(backupStateBlock.value.Data);
    this.backupState = JSON.parse(
      new TextDecoder().decode(this.backupFileInfo.data)
    );
    this.sectionHashes = this.backupState.Hashes.Sections;

    //Open Restore File
    if (typeof restorePath === "undefined") {
      restorePath = `./restored-${this.backupState.Name}/${Date.now()}.db`;
    }

    await fse.ensureFile(restorePath);
    this.restoredDatabaseHandle = await fs.open(restorePath, "w");

    //Parse through each Link and download page
    let pageNumber = 0;
    for (let link of backupStateBlock.value.Links) {
      //Get Content
      let contentToWrite = await this.ipfsClient.block.get(link.Hash);

      //Check if Content needs Decrypted
      if (this.#encryptionEnabled === true) {
        //TODO: Get CustomIV
        contentToWrite = this.encryptionHelper.decrypt(contentToWrite);
      }

      //Write Content to Disk
      await this.restoredDatabaseHandle.write(
        contentToWrite,
        0,
        contentToWrite.byteLength,
        pageNumber * contentToWrite.length
      );

      //Next page
      pageNumber++;
    }
  }

  #createLockTable() {
    try {
      this.db.prepare(`SELECT * FROM _orbit_sqlite_seq WHERE id = @id`).get({
        id: "1",
      });
    } catch (err) {
      this.db
        .prepare(
          `CREATE TABLE IF NOT EXISTS _orbit_sqlite_seq (id INTEGER PRIMARY KEY, seq INTEGER)`
        )
        .run();
      this.db
        .prepare(
          `INSERT INTO _orbit_sqlite_seq VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1`
        )
        .run();
    }
  }

  #lockDatabase() {
    // Begin Transaction
    this.db.prepare(`BEGIN`).run();
    this.db.prepare(`SELECT COUNT(1) FROM _orbit_sqlite_seq`).run();
  }

  #unlockDatabase() {
    // Rollback Transaction
    return this.db.prepare(`ROLLBACK`).run();
  }
}

(async function () {
  program
    .command("restore")
    .argument(
      "<backupConfigurationDatabasePath>",
      "Local Path to Backup Configuration Database"
    )
    .argument("[customKey]", "Secret Key that Database was encrypted with")
    .argument("[customIv]", "Secret IV that Database was encrypted with")
    .action(async ({ logger, args, options }) => {
      try {
        console.log(
          `Restore Started from Config ${args.backupConfigurationDatabasePath}`
        );
        let db = new IPFSSQLite({
          embeddedIPFS: false,
          unencrypted: args.customIv ? false : true,
          customIV: args.customIv,
          customKey: args.customKey,
        });
        await db.restore(args.backupConfigurationDatabasePath);
        console.log(
          `Restore Completed from Config ${args.backupConfigurationDatabasePath}`
        );
      } catch (err) {
        console.error(err.message);
      }
    });

  program
    .command("backup")
    .argument("<databaseRelativePath>", "Relative Path to Database to Backup")
    .argument("[backupStateCID]", "CID to load backup state from")
    .argument(
      "[customKey]",
      "Secret Key to encrypt Database with.  Optional and if blank will be generated randomly"
    )
    .argument(
      "[customIV]",
      "Custom IV to encrypt Database with.  Optional and if blank will be generated randomly"
    )
    .option("--unencrypted", "Do not encrypt each page of database")
    .option("--watch", "Watch file for changes and run backup")
    .action(async ({ logger, args, options }) => {
      let backupSettings = {
        backupStateCID: args.backupStateCid,
        dbFilePath: args.databaseRelativePath,
      };

      let db = new IPFSSQLite({
        embeddedIPFS: false,
        unencrypted: options.unencrypted,
        customIV: args.customIv,
        customKey: args.customKey,
      });

      try {
        let backupJob = dbBackupLimiter.schedule(
          () =>
            (backupSettings.backupStateCID = db.backupDatabase(backupSettings))
        );
        await backupJob;

        if (options.watch === true) {
          console.log(`Watching for Changes`);

          let writeAheadLogData = await db.parseWriteAheadLog();

          let dbDirWatcher = (async () => {
            let databaseFileWatcher = await fs.watch(
              path.dirname(db.dbFilePath),
              {
                persistent: true,
              }
            );

            for await (const databaseEvent of databaseFileWatcher) {
              console.log(
                `Database Directory Event: ${databaseEvent.eventType}`
              );

              if (
                path.basename(db.dbFilePath) === databaseEvent.filename &&
                databaseEvent.eventType === "change"
              ) {
                console.log(`MAIN File Event: ${databaseEvent.eventType}`);
                backupSettings.pagesChanged = writeAheadLogData.pagesAffected;
                let eventBackupJob = dbBackupLimiter.schedule(() =>
                  db.backupDatabase(backupSettings)
                );
                await eventBackupJob;
              } else if (
                path.basename(db.dbFilePath) + "-wal" ===
                  databaseEvent.filename &&
                databaseEvent.eventType === "change"
              ) {
                console.log(`WAL File Event: ${databaseEvent.eventType}`);
                writeAheadLogData = await db.parseWriteAheadLog();
              }
            }
          })();
        }
      } catch (error) {
        console.error(error.message);
      }
    });

  program.run();
})();
