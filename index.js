#!/usr/bin/env node
const Database = require('better-sqlite3'),
  fs = require('fs/promises'),
  Bottleneck = require("bottleneck"),
  hasha = require("hasha"),
  _ = require('lodash'),
  customCrypto = require("./crypto"),
  {program} = require("@caporal/core"),
  toBuffer = require('it-to-buffer'),
  path = require("path"),
	{ constants } = require("fs");

// Backup Each Page in Order
const dbBackupLimiter = new Bottleneck({
	maxConcurrent: 1,
	highWater: 1,
	strategy: Bottleneck.strategy.LEAK,
});
const savePageLimiter = new Bottleneck({
	maxConcurrent: 1200
});
const restorePageLimiter = new Bottleneck({
	maxConcurrent: 1200
});
const backupDatabaseSectionLimiter = new Bottleneck({
	maxConcurrent: 12
})

class OrbitSQLite {
	constructor({secretKey, embeddedIPFS = false}) {
		this.encryptionHelper = new customCrypto({
			secretKey: secretKey
		})

		const IPFS = embeddedIPFS ? require('ipfs') : require('ipfs-http-client');
		this.ipfsClient = IPFS.create();

		this.encryptionEnabled = true;

		this.successfulPageSaves = 0;

		this.preparedStatements = {};
	}

	async backupDatabase({dbFilePath}) {
		try {
			console.log(`Backup Started of File: ${dbFilePath}`);

			this.dbFilePath = dbFilePath;
			this.encryptedDbFilePath = `${this.dbFilePath}-encrypted`;
			this.dbName = path.basename(dbFilePath);

			//Open Frame Map DB
			this.backupConfigurationDatabasePath = `${this.dbFilePath}-orbit`;
			//Check for and if needed create configuration database
			try {
				await fs.access(this.backupConfigurationDatabasePath, constants.R_OK | constants.W_OK);
				console.log('Existing Backup Configuration Database Found');
			} catch {
				await fs.copyFile('./backupConfigurationTemplate.db-orbit', this.backupConfigurationDatabasePath);
			}
			//Open Configuration Database
			this.backupConfigurationDatabase = new Database(this.backupConfigurationDatabasePath);

			// Wait for the ReadStream to Initialize
			this.dbFileHandle = await fs.open(this.dbFilePath, 'r');
			this.encryptedDbFileHandle = await fs.open(this.encryptedDbFilePath, 'w');

			// Parse DB Header
			this.dbHeader = await this.parseHeader();

			// Wait for the IPFS Client to Initialize
			this.ipfsClient = await this.ipfsClient;

			// Fetch Database File Stats
			this.dbStats = await this.dbFileHandle.stat();

			// Calculate Page Count
			this.dbStats.pageCount = this.dbStats.size / this.dbHeader['Page Size in Bytes'];

			this.db = new Database(this.dbFilePath);

			// Ensure Lock Table Exists
			await this.#createLockTable();

			// Lock Database for Backup
			this.#lockDatabase();

			let fileHash = await hasha.fromFile(this.dbFilePath, {
				encoding: 'hex',
				algorithm: 'md5'
			});

			// Compare Hash in SQLite if the hash exists
			let getFileStatement = await this.backupConfigurationDatabase.prepare(`
				SELECT * 
				FROM file
				WHERE fileId = @fileId
				LIMIT 1
			`);
			let existingFileHash = await getFileStatement.get({
				fileId: 1
			});

			// Return true immediately if values match
			if (_.isObject(existingFileHash)) {
				if (existingFileHash.hash === fileHash) {
					console.log(`File ${this.dbFilePath} has not changed since last backup`);


				} else {
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
					await this.encryptedDbFileHandle.close();
					console.log(`Written ${this.successfulPageSaves} pages`);

					// Store Hash of File in SQLite
					let newHashInsert = await this.backupConfigurationDatabase.prepare(`
						UPDATE file
						SET hash = @hash, name = @name, updatedOn = @updatedOn
						WHERE fileId = @fileId
					`);
					try {
						await newHashInsert.run({
							name: this.dbName,
							fileId: 1,
							hash: fileHash,
							updatedOn: Date.now()
						});

						await this.backupConfigurationDatabase.close();

						//Get full buffer of backup configuration database
						let backupDatabaseBuffer = await fs.readFile(this.backupConfigurationDatabasePath);
						let backupDatabaseUpload = await this.ipfsClient.add(backupDatabaseBuffer, {
							cidVersion: 1
						});

						console.log(`Recover Database using Backup Configuration Database Uploaded to CID: ${backupDatabaseUpload.cid.toString()}`);
					} catch (err) {
						console.error(err.message);
					}
				}
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
			let pagesInProgress = [];
			let sectionBuffer = (await this.dbFileHandle.read({
				buffer: Buffer.alloc(sectionSize),
				offset: 0,
				length: sectionSize,
				position: sectionNumber * sectionSize
			})).buffer;

			let sectionHashPromise = hasha.async(sectionBuffer, {
				encoding: 'hex',
				algorithm: 'md5'
			})

			// Compare Hash in SQLite if the hash exists
			let existingHashGet = await this.backupConfigurationDatabase.prepare(`
					SELECT * 
					FROM sections 
					WHERE number = @number
					LIMIT 1
				`);
			let existingHash = await existingHashGet.get({
				number: sectionNumber
			});

			let sectionHash;
			// Return true immediately if values match
			if (_.isObject(existingHash)) {
				sectionHash = await sectionHashPromise;
				if (existingHash.hash === sectionHash) {
					console.log(`Section ${sectionNumber} has not changed since last backup`);
					return;
				} else {
					console.log(`Page ${sectionNumber} has changed since last backup`);
				}
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
					buffer: pageBuffer,
					hashPromise: hasha.async(pageBuffer, {
						encoding: 'hex',
						algorithm: 'md5'
					})
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

			// Store Hash of Segment in SQLite
			let newHashInsert = await this.backupConfigurationDatabase.prepare(`
					INSERT INTO sections (number, hash) 
					VALUES (@number, @hash)
					ON CONFLICT (number)
					DO UPDATE SET hash = @hash
				`);
			try {
				await newHashInsert.run({
					number: sectionNumber,
					hash: await sectionHashPromise
				});
			} catch (err) {
				console.error(err.message);
			}
		} catch (err) {
			console.error(err.message);
		}
	}

	async #savePage(page) {
		// Compare Hash in SQLite if the hash exists
		let existingHashGet = await this.backupConfigurationDatabase.prepare(`
			SELECT * 
			FROM pages 
			WHERE number = @number
			LIMIT 1
		`);
		let existingHash = await existingHashGet.get({
			number: page.index
		});
		// Return true immediately if values match
		if (_.isObject(existingHash)) {
			page.hash = await page.hashPromise;
			if (existingHash.hash === page.hash) {
				console.log(`Page ${page.index} has not changed since last backup`);
				return true;
			} else {
				console.log(`Page ${page.index} has changed since last backup`);
			}
		}

		// Write Page to IPFS
		let contentToWrite = page.buffer;

		let encryptedContent;
		if (this.encryptionEnabled === true) {
			encryptedContent = this.encryptionHelper.encrypt(page.buffer);
			contentToWrite = Buffer.from(encryptedContent.content);
		}

		let uploadedPage = await this.ipfsClient.add(contentToWrite, {
			cidVersion: 1
		});

		// Store Hash of Buffer in SQLite
		let newHashInsert = await this.backupConfigurationDatabase.prepare(`
			INSERT INTO pages (number, hash, iv, cid, updatedOn) 
			VALUES (@number, @hash, @iv, @cid, @updatedOn)
			ON CONFLICT (number) DO 
			UPDATE SET hash=@hash, iv=@iv, cid=@cid, updatedOn=@updatedOn 
		`);
		try {
			if (typeof page.hash === "undefined") {
				page.hash = await page.hashPromise;
			}
			await newHashInsert.run({
				number: page.index,
				hash: page.hash,
				iv: encryptedContent.iv,
				cid: uploadedPage.cid.toString(),
				updatedOn: Date.now()
			});
		} catch (err) {
			console.error(err.message);
		}

		console.log(`Uploaded Page ${page.index}/${this.dbStats.pageCount} (${((100 / this.dbStats.pageCount) * page.index).toFixed(2)}%) to IPFS at CID [${uploadedPage.cid.toString()}]`);
		return page;
	}

	async restore(backupConfigurationDatabasePath, restorePath) {
		//Open Backup Configuration DB
		this.backupConfigurationDatabase = new Database(backupConfigurationDatabasePath);

		let getFileInfo = await this.backupConfigurationDatabase.prepare(`
			SELECT *
			FROM file
			WHERE fileId = '1'
			LIMIT 1;
		`);

		this.preparedStatements.getPageInfo = await this.backupConfigurationDatabase.prepare(`
			SELECT *
			FROM pages 
			WHERE number = @number
			LIMIT 1;
		`);

		let getPageCount = await this.backupConfigurationDatabase.prepare(`
			SELECT COUNT(number) AS 'count'
			FROM pages;
		`);

		let fileInfo = await getFileInfo.get();
		let pageCount = await getPageCount.get();

		if (typeof restorePath === 'undefined') {
			restorePath = `./restored-${Date.now()}-${fileInfo.name}`
		}

		this.restoredDatabaseHandle = await fs.open(restorePath, 'w');

		let restorationsInProgress = [];

		//Worker Function to Restore Page to Decrypted Local DB File
		let restorePage = async (pageNumber) => {
			let pageInfo = await this.preparedStatements.getPageInfo.get({
				number: pageNumber
			});
			let encryptedPageBuffer = await toBuffer(this.ipfsClient.cat(pageInfo.cid));
			let unencryptedPageBuffer = this.encryptionHelper.decrypt({
				iv: pageInfo.iv,
				content: encryptedPageBuffer
			});
			await this.restoredDatabaseHandle.write(unencryptedPageBuffer, 0, unencryptedPageBuffer.byteLength, (pageNumber * unencryptedPageBuffer.length));
		}

		for (let pageNumber = 0; pageNumber < pageCount.count; pageNumber++) {
			let restorePageWorker = restorePageLimiter.schedule(() => restorePage(pageNumber));

			restorePageWorker.then((restoredPage) => {
				console.log(`Restored Page [${pageNumber}] (${((100 / pageCount.count) * pageNumber).toFixed(2)})%)`);
			});

			restorationsInProgress.push(restorePageWorker);
		}
		await Promise.all(restorationsInProgress);
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
		.argument("<secretKey>", "Secret Key that Database was encrypted with")
		.action(async ({logger, args, options}) => {
			try {
				console.log(`Restore Started from Config ${args.backupConfigurationDatabasePath}`);
				let db = new OrbitSQLite({
					secretKey: args.secretKey
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
		.argument("<secretKey>", "Secret Key to encrypt Database with")
		.option("--encrypt <boolean>", "Encrypt each page of database", {
			default: true,
		})
		.option("--watch <boolean>", "Watch file for changes and run backup", {
			default: true,
		})
		.action(async ({logger, args, options}) => {
			let backupSettings = {
				dbFilePath: args.databaseRelativePath
			}

			let db = new OrbitSQLite({
				secretKey: args.secretKey
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