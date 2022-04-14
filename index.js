#!/usr/bin/env node
const Database = require("better-sqlite3"),
  fs = require("fs/promises"),
  Bottleneck = require("bottleneck"),
  hasha = require("hasha"),
  _ = require("lodash"),
  customCrypto = require("./crypto"),
  { program } = require("@caporal/core"),
  path = require("path"),
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
  maxConcurrent: 1000,
});
const backupDatabaseSectionLimiter = new Bottleneck({
  maxConcurrent: 10,
});

const adapters = {
  IPFS: require("./adapters/ipfsAdapter"),
};

let selectedAdapter = null;

class IPFSSQLite {
  #configuration = {
    encryptionEnabled: true,
  };
  #runningMetadata = {};
  #runningBackup = {
    pageLinks: [],
    sectionHashes: [],
  };

  constructor({ customKey, customIV, unencrypted = false }) {
    if (unencrypted === false) {
      this.encryptionHelper = new customCrypto({
        customKey: customKey,
        customIV: customIV,
      });
    } else {
      this.#configuration.encryptionEnabled = false;
    }

    this.selectedAdapter = new adapters.IPFS({
      databaseName: `Northwind_small.sqlite`,
    });

    this.dbPageCIDs = [];
    this.restoreCount = 0;
  }

  async restoreDatabase({ backupStateCID, restorePath, existingStateCID }) {
    //Check if CID/S3 URL is passed
    if (_.isString(backupStateCID) === false) {
      throw new Error(`Invalid CID`);
    }

    //Connect with Selected Adapter
    console.log(
      `Connecting to Selected Adapter: ${
        this.selectedAdapter.displayName
      } at Date: ${Date.now()}`
    );
    await this.selectedAdapter.connect({});
    console.log(
      `Connected to Selected Adapter: ${
        this.selectedAdapter.displayName
      } at Date: ${Date.now()}`
    );

    //Load Backup State
    this.backupState = await this.selectedAdapter.getJSON(backupStateCID);

    //Set Restore Path using name from backup state
    if (typeof restorePath === "undefined") {
      restorePath = `./restored-${this.backupState.Name}/${Date.now()}.db`;
    }

    //Ensure Restore File Exists
    await fse.ensureFile(restorePath);
    //Get Current DB Stats
    let restoreDatabaseStats = await fs.stat(restorePath);
    //Check if Existing Restore and if so Open and Lock the Database
    if (restoreDatabaseStats.size !== 0) {
      this.restoreDB = new Database(restorePath);
      this.#lockDatabase(this.restoreDB, "EXCLUSIVE");
    }
    this.restoredDatabaseHandle = await fs.open(restorePath, "r+");

    //Fetch New Backup Info
    const backupInfo = await this.selectedAdapter.getJSON(
      this.backupState.Versions.Current
    );
    //Fetch Existing Backup Info
    const existingBackupInfo =
      typeof existingStateCID === "string"
        ? await this.selectedAdapter.getJSON(existingStateCID)
        : null;

    //Parse through each Link and Download Pages
    let pageNumber = 0;
    let restoresInProgress = [];
    //Reused variables
    const totalPageCount = backupInfo.links.length;
    //Determine which sections can be skipped
    for (const link of backupInfo.links) {
      //Check current CID for page and see if page has changed
      if (
        existingBackupInfo !== null &&
        typeof existingBackupInfo.links[pageNumber] === "string" &&
        existingBackupInfo.links[pageNumber] === link.Hash.toString()
      ) {
        pageNumber++;
        continue;
      }

      let beforeActionVerb = `Creating`;
      if (
        existingBackupInfo !== null &&
        typeof existingBackupInfo.links[pageNumber] === "string" &&
        existingBackupInfo.links[pageNumber] !== link.Hash.toString()
      ) {
        beforeActionVerb = "Updating";
      }
      console.log(
        `${beforeActionVerb} Page: (${pageNumber + 1}/${totalPageCount}) [${(
          ((pageNumber + 1) / totalPageCount) *
          100
        ).toFixed(2)}%] from Page Link: ${link} at Date: ${Date.now()}`
      );

      //Write Content to Disk
      let contentToWrite = await this.selectedAdapter.getPage(pageNumber, link);
      await this.restoredDatabaseHandle.write(
        contentToWrite,
        0,
        contentToWrite.byteLength,
        pageNumber * contentToWrite.length
      );

      let actionVerb = `Created`;
      if (
        existingBackupInfo !== null &&
        typeof existingBackupInfo.links[pageNumber] === "string" &&
        existingBackupInfo.links[pageNumber] !== link.Hash.toString()
      ) {
        actionVerb = "Updated";
      }
      console.log(
        `${actionVerb} Page: (${pageNumber + 1}/${totalPageCount}) [${(
          ((pageNumber + 1) / totalPageCount) *
          100
        ).toFixed(2)}%] from Link: ${link} at Date: ${Date.now()}`
      );

      //Next page
      pageNumber++;
    }

    //Wait for all pages to be restored
    await Promise.all(restoresInProgress);

    //If Database is a Fresh Restore then Open and Lock Database now
    if (restoreDatabaseStats.size === 0) {
      this.restoreDB = new Database(restorePath);
      this.#lockDatabase(this.restoreDB, "EXCLUSIVE");
    }

    //Close File Handle
    await this.restoredDatabaseHandle.close();

    //Clear DB Cache
    this.restoreDB.pragma("cache_size = 0");
    this.restoreDB.pragma("cache_size = -2000");

    //Unlock and Close Database
    await this.#unlockDatabase(this.restoreDB);
    await this.restoreDB.close();
  }

  async backupDatabase({ dbFilePath, backupStateCID, pagesChanged }) {
    console.log(`Starting Backup of File: ${dbFilePath}`);

    //Connect with Selected Adapter
    console.log(`Connecting to Selected Adapter`);
    await this.selectedAdapter.connect({});
    this.dbFilePath = dbFilePath;
    this.dbName = path.basename(dbFilePath);

    this.backupState = {};

    //Get existing configuration if passed
    if (_.isString(backupStateCID)) {
      console.log(`Fetching Existing Backup State`);
      this.backupState = await this.selectedAdapter.getJSON(backupStateCID);

      //Load existing section hashes
      this.#runningBackup.sectionHashes = this.backupState.Hashes.Sections;

      //Load existing page references
      let pageNumber = 0;
      for (let page of this.backupState.value.Links) {
        this.#runningBackup.pageLinks[pageNumber] = page.Hash;
        pageNumber++;
      }
    }

    // Open Database in WAL mode
    console.log(`Opening Database File`);
    this.db = new Database(this.dbFilePath);
    // Ensure Lock Table Exists
    this.#createLockTable(this.db);
    // Lock Database for Backup
    this.#lockDatabase(this.db);

    // Wait for the ReadStream to Initialize
    this.dbFileHandle = await fs.open(this.dbFilePath, "r");
    // Parse DB Header
    this.dbHeader = await this.#parseDatabaseHeader();
    // Fetch Database File Stats
    this.dbStats = await this.dbFileHandle.stat();
    // Calculate Page Count
    this.dbStats.pageCount =
      this.dbStats.size / this.dbHeader["Page Size in Bytes"];

    //Generate Hash of Current Database File
    console.log(`Generating Hash of Database File: ${dbFilePath}`);
    let fileHash = await hasha.fromFile(this.dbFilePath, {
      encoding: "hex",
      algorithm: "sha256",
    });

    // Return true immediately if values match and no pages have been marked as changed
    if (
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
    let pagesInSection =
      maxSectionSize > this.dbStats.pageCount
        ? this.dbStats.pageCount
        : maxSectionSize;
    let sectionSize = pagesInSection * this.dbHeader["Page Size in Bytes"];
    let sectionCount = Math.ceil(this.dbStats.size / sectionSize);

    let sectionsChanged = new Set();
    let pagesChangedBySection = {};
    if (typeof pagesChanged !== "undefined") {
      for (let pageNumber of pagesChanged) {
        let sectionNumber = pageNumber % sectionSize;
        sectionsChanged.add(sectionNumber);
        pagesChangedBySection[sectionNumber] =
          pagesChangedBySection[sectionNumber] || new Set();
        pagesChangedBySection[sectionNumber].add(pageNumber);
      }
    }

    let sectionsInProgress = [];
    for (let sectionNumber = 0; sectionNumber < sectionCount; sectionNumber++) {
      //Skip sections that haven't been changed
      if (
        sectionsChanged.length > 0 &&
        sectionsChanged.has(sectionNumber) === false
      ) {
        continue;
      }

      //Get Buffer for Section
      let sectionBuffer = (
        await this.dbFileHandle.read({
          buffer: Buffer.alloc(sectionSize),
          offset: 0,
          length: sectionSize,
          position: sectionNumber * sectionSize,
        })
      ).buffer;

      let pagesChangedInSection = pagesChangedBySection[sectionNumber] || [];

      if (typeof pagesChangedBySection[sectionNumber] === "undefined") {
        let sectionInfo = await this.#checkSectionForChanges(
          sectionBuffer,
          sectionNumber,
          sectionSize,
          pagesInSection
        );

        pagesChangedInSection = sectionInfo.changedPages;
      }

      let pagesInProgress = [];
      // Check each page for changes
      for (let pageNumber of pagesChangedInSection) {
        let pageBuffer = sectionBuffer.subarray(
          pageNumber * this.dbHeader["Page Size in Bytes"],
          (pageNumber + 1) * this.dbHeader["Page Size in Bytes"]
        );

        //Set read and write mode to rollback journal instead of WAL
        if (pageNumber === 0) {
          pageBuffer[18] = 1;
          pageBuffer[19] = 1;
        }

        let savePageWorker = savePageLimiter.schedule(() => {
          return this.selectedAdapter.savePage(
            pageNumber,
            pageBuffer,
            this.#configuration.encryptionEnabled
          );
        });

        savePageWorker.then((saveResult) => {
          let savedPageID = saveResult.id;
          console.log(saveResult.message);
          this.#runningBackup.pageLinks[pageNumber] = savedPageID;
        });

        pagesInProgress.push(savePageWorker);
      }

      // Wait for All Pages to Finish Backing Up
      await Promise.all(pagesInProgress);
    }
    // Wait for All Sections to Finish Backing Up
    await Promise.all(sectionsInProgress);

    // Unlock and Close Database after Backup
    await this.#unlockDatabase();
    await this.db.close();

    // Close File Handle after Backup
    await this.dbFileHandle.close();

    let backupCID = await this.selectedAdapter.saveJSON({
      name: this.selectedAdapter.databaseName,
      pageSize: this.dbHeader["Page Size in Bytes"],
      links: this.#runningBackup.pageLinks,
    });

    let metadataResult = await this.selectedAdapter.saveJSON({
      Name: this.selectedAdapter.databaseName,
      CreatedOn: Date.now(),
      Hashes: {
        File: fileHash,
        Sections: this.#runningBackup.sectionHashes,
      },
      Versions: {
        Current: backupCID.toString(),
        [Date.now()]: backupCID.toString(),
      },
    });

    let publishRequest = await this.selectedAdapter.publishMetadata(
      `/ipfs/${metadataResult}`
    );
    console.log(publishRequest.message);
  }

  async watchPageChanges() {
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

  async #parseDatabaseHeader() {
    let headerBuffer = (
      await this.dbFileHandle.read(Buffer.alloc(100), 0, 100, 0)
    ).buffer;
    return {
      "Header String": headerBuffer.toString("utf8", 0, 16),
      "Page Size in Bytes": headerBuffer.readUIntBE(16, 2),
      "File Format write Version": headerBuffer[18],
      "File Format read Version": headerBuffer[19],
      "Bytes Reserved at the end of Each Page": headerBuffer[20],
      "Max Embedded Payload Fraction": headerBuffer[21],
      "Min Embedded Payload Fraction": headerBuffer[22],
      "Min Leaf Payload Fraction": headerBuffer[23],
      "File Change Counter": headerBuffer.readUIntBE(24, 4),
      "Reserved for Future Use": headerBuffer.subarray(28, 32),
      "First Freelist Page": headerBuffer.readUIntBE(32, 4),
      "Number of Freelist Pages": headerBuffer.readUIntBE(36, 4),
      "154 Byte Meta Values": headerBuffer.subarray(40, 80),
    };
  }

  async #checkSectionForChanges(
    sectionBuffer,
    sectionNumber,
    sectionSize,
    pagesInSection
  ) {
    try {
      //Get Hash for Section
      let sectionHash = await hasha.async(sectionBuffer, {
        encoding: "hex",
        algorithm: "sha256",
      });

      //Load Existing Section Hashes
      let existingHash = undefined;
      if (_.isArray(this.backupState?.Hashes?.Sections)) {
        existingHash = this.backupState.Hashes.Sections[sectionNumber];

        // Return true immediately if section has not changed
        if (_.isString(existingHash) && existingHash === sectionHash) {
          console.log(
            `Section ${sectionNumber} has not changed since last backup`
          );
          return sectionHash;
        }
      }

      //Check if we already know which pages to check for changes
      let startingPage = sectionNumber * pagesInSection;
      let pagesChanged = new Set();

      // Check each page for changes
      for (
        let pageNumber = startingPage;
        pageNumber < startingPage + pagesInSection;
        pageNumber++
      ) {
        pagesChanged.add(pageNumber);
      }

      this.#runningBackup.sectionHashes[sectionNumber] = sectionHash;

      return {
        sectionHash: sectionHash,
        changedPages: pagesChanged,
      };
    } catch (err) {
      console.error(err.message);
      throw err;
    }
  }

  #createLockTable(selectedDB = this.db) {
    try {
      selectedDB.prepare(`SELECT * FROM _ipfs_sqlite_seq WHERE id = @id`).get({
        id: "1",
      });
    } catch (err) {
      selectedDB
        .prepare(
          `CREATE TABLE IF NOT EXISTS _ipfs_sqlite_seq (id INTEGER PRIMARY KEY, seq INTEGER)`
        )
        .run();
      selectedDB
        .prepare(
          `INSERT INTO _ipfs_sqlite_seq VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1`
        )
        .run();
    }
  }

  #lockDatabase(selectedDB = this.db, lockType = null) {
    // Begin Transaction
    selectedDB.prepare(`BEGIN ${lockType || ""}`).run();
    selectedDB.prepare(`SELECT COUNT(1) FROM _ipfs_sqlite_seq`).run();
  }

  #unlockDatabase(selectedDB = this.db) {
    // Rollback Transaction
    return selectedDB.prepare(`ROLLBACK`).run();
  }
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

(async function () {
  program
    .command("replicate")
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
          unencrypted: !args.customIv,
          customIV: args.customIv,
          customKey: args.customKey,
        });

        let protocol = args.backupConfigurationDatabasePath.split("/")[0];

        let backupConfigurationCID;
        if (protocol === "ipns") {
          for await (const name of db.ipfsClient.name.resolve(
            args.backupConfigurationDatabasePath
          )) {
            backupConfigurationCID = name.split("/")[2];
          }
        } else if (protocol === "ipfs") {
          backupConfigurationCID =
            args.backupConfigurationDatabasePath.split("/")[1];
        } else {
          throw new Error(`Invalid Protocol: ${protocol}`);
        }

        if (options.watch !== true) {
          await db.restoreDatabase({
            backupStateCID: backupConfigurationCID,
          });

          console.log(
            `Restore Completed from Config ${args.backupConfigurationDatabasePath}`
          );
        }

        if (protocol !== "ipns") {
          throw new Error(`Must use an IPNS path for replication`);
        }

        let runningCID = null;
        while (options.watch === true) {
          for await (const name of db.ipfsClient.name.resolve(
            args.backupConfigurationDatabasePath.split("/")[1],
            {
              nocache: true,
            }
          )) {
            let currentCID = name.split("/")[2];

            if (runningCID === currentCID) {
              continue;
            }

            await db.restoreDatabase({
              backupStateCID: currentCID,
              restorePath: `replica.db`,
            });
            runningCID = currentCID;
          }

          await sleep(1000);
        }
      } catch (err) {
        console.error(err.message);
        throw err;
      }
    });

  program
    .command("restore")
    .argument("<backupStateCID>", "Local Path to Backup Configuration Database")
    .argument("[customKey]", "Secret Key that Database was encrypted with")
    .argument("[customIv]", "Secret IV that Database was encrypted with")
    .action(async ({ logger, args, options }) => {
      try {
        console.log(
          `Restore Started from Backup Metadata: ${
            args.backupStateCid
          } at Date: ${Date.now()}`
        );

        let db = new IPFSSQLite({
          unencrypted: !args.customIv,
          customIV: args.customIv,
          customKey: args.customKey,
        });

        await db.restoreDatabase({
          backupStateCID: args.backupStateCid,
        });

        console.log(
          `Restore Completed from Backup Metadata: ${
            args.backupStateCid
          } at Date: ${Date.now()}`
        );
      } catch (err) {
        console.error(err.message);
        throw err;
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
        unencrypted: options.unencrypted,
        customIV: args.customIv,
        customKey: args.customKey,
      });

      try {
        let backupJob = dbBackupLimiter.schedule(() => {
          let backupRequest = db.backupDatabase(backupSettings);
          backupSettings.backupStateCID = backupRequest.CID;
        });
        await backupJob;

        if (options.watch === true) {
          console.log(`Watching for Changes`);

          let writeAheadLogData = await db.watchPageChanges();

          let databaseFileWatcher = await fs.watch(
            path.dirname(db.dbFilePath),
            {
              persistent: true,
            }
          );

          for await (const databaseEvent of databaseFileWatcher) {
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
              writeAheadLogData = await db.watchPageChanges();
            }
          }
        }
      } catch (error) {
        console.error(error.message);
        throw error;
      }
    });

  program.run();
})();
