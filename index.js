#!/usr/bin/env node
const Database = require('better-sqlite3'),
  fs = require('fs/promises'),
  Bottleneck = require("bottleneck"),
  hasha = require("hasha"),
  _ = require('lodash'),
  customCrypto = require("./crypto"),
  {program} = require("@caporal/core"),
  path = require("path"),
	{ CID } = require('multiformats/cid'),
	dagPB = require('@ipld/dag-pb'),
	{ UnixFS } = require('ipfs-unixfs'),
	fse = require('fs-extra');

// Backup Each Page in Order
const dbBackupLimiter = new Bottleneck({
	maxConcurrent: 1,
	highWater: 1,
	strategy: Bottleneck.strategy.LEAK,
});
const savePageLimiter = new Bottleneck({
	maxConcurrent: 12
});
const restorePageLimiter = new Bottleneck({
	maxConcurrent: 1
});
const backupDatabaseSectionLimiter = new Bottleneck({
	maxConcurrent: 2
})

class OrbitSQLite {
	#encryptionEnabled = true;
	pageLinks = [];
	successfulPageSaves = 0;
	sectionHashes = [];

	constructor({customKey, customIV, embeddedIPFS = false, unencrypted = false}) {
		if (unencrypted === false) {
			this.encryptionHelper = new customCrypto({
				customKey: customKey,
				customIV: customIV
			});
		} else {
			this.#encryptionEnabled = false;
		}

		this.IPFS = embeddedIPFS ? require('ipfs') : require('ipfs-http-client');
	}

	async backupDatabase({dbFilePath, backupStateCID}) {
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
				console.log(`Fetching Existing Backup State from CID: ${backupStateCID}`);
				let backupStateBlock = await this.ipfsClient.dag.get(CID.parse(backupStateCID));
				this.backupFileInfo = UnixFS.unmarshal(backupStateBlock.value.Data);
				this.backupState = JSON.parse((new TextDecoder().decode(this.backupFileInfo.data)));
				this.sectionHashes = this.backupState.Hashes.Sections;

				let pageNumber = 0;
				for (let page of backupStateBlock.value.Links) {
					this.pageLinks[pageNumber] = page;
					pageNumber++;
				}

				//Add current version to history
				this.backupState.Versions.push({
					CreatedOn: this.backupState.CreatedOn,
					cid: backupStateCID
				});
			}

			console.log(`Opening Database File: ${this.dbFilePath}`);
			// Wait for the ReadStream to Initialize
			this.dbFileHandle = await fs.open(this.dbFilePath, 'r');
			// Parse DB Header
			this.dbHeader = await this.parseHeader();
			// Fetch Database File Stats
			this.dbStats = await this.dbFileHandle.stat();
			// Calculate Page Count
			this.dbStats.pageCount = this.dbStats.size / this.dbHeader['Page Size in Bytes'];

			// Open Database
			this.db = new Database(this.dbFilePath);
			// Ensure Lock Table Exists
			this.#createLockTable();
			// Lock Database for Backup
			this.#lockDatabase();


			console.log(`Generating Hash of Database File: ${dbFilePath}`);
			let fileHash = await hasha.fromFile(this.dbFilePath, {
				encoding: 'hex',
				algorithm: 'sha256'
			});

			// Return true immediately if values match
			if (_.isString(this.backupState?.Hashes?.File) && this.backupState.Hashes.File === fileHash) {
				console.log(`No changes detected since last sync for database file: ${this.dbFilePath}`);
				return;
			}
			console.log(`File ${this.dbFilePath} has changed since last backup`);

			let maxSectionSize = 250 * this.dbHeader['Page Size in Bytes'] // 250 Pages of Database
			let pagesInSection = Math.floor(maxSectionSize / this.dbHeader['Page Size in Bytes']);
			let sectionSize = pagesInSection * this.dbHeader['Page Size in Bytes'];
			let sectionCount = Math.ceil(this.dbStats.size / sectionSize);

			let sectionsInProgress = [];
			for (let sectionNumber = 0; sectionNumber < sectionCount; sectionNumber++) {
				const sectionWorker = await backupDatabaseSectionLimiter.schedule(() => this.#backupDatabaseSection(
					sectionNumber,
					sectionSize,
					pagesInSection
				));

				sectionsInProgress.push(sectionWorker);
			}
			await Promise.all(sectionsInProgress);

			await this.dbFileHandle.close();
			try {
				let backupFileSettings = {
					type: 'file',
					data: {
						Name: this.dbName,
						Hashes: {
							File: fileHash,
							Sections: this.sectionHashes
						},
						Versions: [],
						CreatedOn: Date.now()
					},
					blockSizes: this.pageLinks.map(() => {
						return this.dbHeader['Page Size in Bytes'];
					})
				}

				//Merge existing versions with new reference version
				backupFileSettings.data.Versions = _.uniq(backupFileSettings.data.Versions.concat(this.backupState.Versions || []));
				backupFileSettings.data = Buffer.from(JSON.stringify(backupFileSettings.data));

				this.backupFile = new UnixFS(backupFileSettings);

				const cid = await this.ipfsClient.dag.put(dagPB.prepare({
					Data: this.backupFile.marshal(),
					Links: this.pageLinks
				}), {
					format: 'dag-pb',
					hashAlg: 'sha2-256'
				})

				if (_.isString(backupStateCID)) {
					if (backupStateCID !== cid.toString()) {
						await this.ipfsClient.pin.add(cid);
						try {
							await this.ipfsClient.pin.rm(backupStateCID);
						} catch (e) {
							console.error(e.message);
						}
					} else {
						console.log(`Backup Configuration did not change!`);
					}
				} else {
					await this.ipfsClient.pin.add(cid);
				}

				console.log(`Recover Database using Backup Configuration at CID: ${cid.toV1().toString()}`);
			} catch (err) {
				console.error(err.message);
			}
		} catch (err) {
			console.error(err.message);
		} finally {
			// Unlock Database after Backup
			this.#unlockDatabase();
			this.db.close();

			console.log(`Backup Completed of File: ${this.dbFilePath}`);
		}
	}

	async parseHeader() {
		let headerBuffer = (await this.dbFileHandle.read(Buffer.alloc(100), 0, 100, 0)).buffer;
		return {
			"Header String": headerBuffer.toString('utf8', 0, 16),
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
			"154 Byte Meta Values": headerBuffer.subarray(40, 80)
		};
	}

	async #backupDatabaseSection(sectionNumber, sectionSize, pagesInSection) {
		try {
			console.log(`Checking Section ${sectionNumber}`);
			let pagesInProgress = [];
			let sectionBuffer = (await this.dbFileHandle.read({
				buffer: Buffer.alloc(sectionSize),
				offset: 0,
				length: sectionSize,
				position: sectionNumber * sectionSize
			})).buffer;

			let sectionHash = await hasha.async(sectionBuffer, {
				encoding: 'hex',
				algorithm: 'sha256'
			});

			let existingHash = undefined;
			if (_.isArray(this.backupState?.Hashes?.Sections)) {
				existingHash = this.backupState.Hashes.Sections[sectionNumber];
			}

			// Return true immediately if values match
			if (_.isString(existingHash) && existingHash === sectionHash) {
				console.log(`Section ${sectionNumber} has not changed since last backup`);
				return;
			} else {
				console.log(`Page ${sectionNumber} has changed since last backup`);
			}

			for (
				let frameNumber = 0, pageNumber = sectionNumber * pagesInSection;
				frameNumber < pagesInSection && pageNumber < this.dbStats.pageCount;
				frameNumber++, pageNumber++
			) {
				let pageBuffer = sectionBuffer.subarray(
					(frameNumber * this.dbHeader['Page Size in Bytes']),
					((frameNumber + 1) * this.dbHeader['Page Size in Bytes'])
				)

				let page = {
					index: pageNumber,
					buffer: pageBuffer
				}

				let savePageWorker = savePageLimiter.schedule(() => {
					return this.#savePage(page);
				});

				savePageWorker.then((savedPage) => {
					this.successfulPageSaves++;
				})

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
			page.hash = await this.ipfsClient.add({
				path: `${this.dbName}-${page.index}.page`,
				content: contentToWrite
			}, {
				cidVersion: 1,
				onlyHash: true
			})
			if (existingHash.Hash.toString() === page.hash.cid.toString()) {
				console.log(`Page ${page.index} has not changed since last backup`);
				return true;
			} else {
				console.log(`Page ${page.index} has changed since last backup`);
			}
		}

		let uploadedPage = await this.ipfsClient.add({
			path: `${page.index}.page`,
			content: contentToWrite
		}, {
			cidVersion: 1,
			pin: false
		});

		try {
			// this.pageLinks[page.index] = new dagPB.createLink(`page-${page.index}`, contentToWrite.byteLength, uploadedPage.cid)
			this.pageLinks[page.index] = uploadedPage.cid
		} catch (err) {
			console.error(err.message);
		}

		console.log(`Uploaded Page ${page.index + 1}/${this.dbStats.pageCount} (${((100 / this.dbStats.pageCount) * (page.index + 1)).toFixed(2)}%) to IPFS at CID [${uploadedPage.cid.toString()}]`);
		return page;
	}

	async restore(backupStateCID, restorePath) {
		console.log(`Connecting to IPFS`)
		// Wait for the IPFS Client to Initialize
		this.ipfsClient = await this.IPFS.create();

		this.backupState = {};
		//Get existing configuration if passed
		if (_.isString(backupStateCID) === false) {
			throw new Error(`Invalid CID`);
		}

		console.log(`Fetching Existing Backup State from CID: ${backupStateCID}`);
		let backupStateBlock = await this.ipfsClient.dag.get(CID.parse(backupStateCID));
		this.backupFileInfo = UnixFS.unmarshal(backupStateBlock.value.Data);
		this.backupState = JSON.parse((new TextDecoder().decode(this.backupFileInfo.data)));
		this.sectionHashes = this.backupState.Hashes.Sections;

		//Open Restore File
		if (typeof restorePath === 'undefined') {
			restorePath = `./restored-${this.backupState.Name}/${Date.now()}.db`;
		}

		await fse.ensureFile(restorePath);
		this.restoredDatabaseHandle = await fs.open(restorePath, 'w');

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
				(pageNumber * contentToWrite.length)
			);

			//Next page
			pageNumber++;
		}
	}

	#createLockTable() {
		try {
			this.db.prepare(`SELECT * FROM _orbit_sqlite_seq WHERE id = @id`).get({
				id: '1'
			});
		} catch (err) {
			this.db.prepare(`CREATE TABLE IF NOT EXISTS _orbit_sqlite_seq (id INTEGER PRIMARY KEY, seq INTEGER)`).run();
			this.db.prepare(`INSERT INTO _orbit_sqlite_seq VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1`).run();
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
		.argument("<backupConfigurationDatabasePath>", "Local Path to Backup Configuration Database")
		.argument("[customKey]", "Secret Key that Database was encrypted with")
		.argument("[customIv]", "Secret IV that Database was encrypted with")
		.option("--unencrypted", "Do not encrypt each page of database")
		.action(async ({logger, args, options}) => {
			try {
				console.log(`Restore Started from Config ${args.backupConfigurationDatabasePath}`);
				let db = new OrbitSQLite({
					embeddedIPFS: false,
					unencrypted: options.unencrypted,
					customIV: args.customIv,
					customKey: args.customKey
				});
				await db.restore(args.backupConfigurationDatabasePath);
				console.log(`Restore Completed from Config ${args.backupConfigurationDatabasePath}`);
			} catch (err) {
				console.error(err.message);
			}
		})

	program
		.command("backup")
		.argument("<databaseRelativePath>", "Relative Path to Database to Backup")
		.argument("[backupStateCID]", "CID to load backup state from")
		.argument("[customKey]", "Secret Key to encrypt Database with.  Optional and if blank will be generated randomly")
		.argument("[customIV]", "Custom IV to encrypt Database with.  Optional and if blank will be generated randomly")
		.option("--unencrypted", "Do not encrypt each page of database")
		.option("--watch <boolean>", "Watch file for changes and run backup", {
			default: true,
		})
		.action(async ({logger, args, options}) => {
			let backupSettings = {
				backupStateCID: args.backupStateCid,
				dbFilePath: args.databaseRelativePath
			}

			let db = new OrbitSQLite({
				embeddedIPFS: false,
				unencrypted: options.unencrypted,
				customIV: args.customIv,
				customKey: args.customKey
			});

			try {
				let backupJob = dbBackupLimiter.schedule(() => db.backupDatabase(backupSettings));
				await backupJob;

				let fileWatcher = await fs.watch(db.dbFilePath, {
					persistent: true
				});


				for await (const event of fileWatcher) {
					console.log(`File Event: ${event.type} - Starting Backup`)
					let eventBackupJob = dbBackupLimiter.schedule(() => db.backupDatabase(backupSettings));
					await eventBackupJob;
				}


			} catch (error) {
				console.error(error.message);
			}
		})

	program.run()
})();