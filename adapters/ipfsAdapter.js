const BaseAdapter = require("./baseAdapter");

const { UnixFS } = require("ipfs-unixfs");
const dagPB = require("@ipld/dag-pb");
const _ = require("lodash");
const IPFS = require("ipfs-http-client");
const CID = IPFS.CID;

class IPFSAdapter extends BaseAdapter {
  constructor(options) {
    super(options);

    this.displayName = "IPFS";
  }

  async getMetadata(path) {
    let backupStateBlock = await this.adapterClient.dag.get(CID.parse(path));
    let metadata = UnixFS.unmarshal(backupStateBlock.value.Data).data;
    return JSON.parse(new TextDecoder().decode(metadata));
  }

  async saveMetadata(fileHash, sectionHashes, path, versions) {
    let metadata = {
      Name: this.databaseName,
      Hashes: {
        File: fileHash,
        Sections: sectionHashes,
      },
      Versions: {
        Current: path,
        [Date.now()]: path,
      },
    };

    // Merge existing versions with new reference version
    metadata.Versions = _.merge(metadata.Versions, versions || {});

    const metadataCID = await this.adapterClient.dag.put(metadata, {
      format: "dag-cbor",
      hashAlg: "sha2-256",
    });
    await this.adapterClient.pin.add(metadataCID.toString());

    console.log(
      `Uploaded Metadata to ${
        this.displayName
      } at CID [${metadataCID.toString()}]`
    );

    return {
      Link: metadataCID.toString(),
    };
  }

  async publishMetadata(metadataPath) {
    //Get List of Existing Publish Keys
    let keys = await this.adapterClient.key.list();

    //Search for Existing Publish Key by Database Name
    let existingKey = _.find(keys, {
      name: `ipfs-sqlite-db-${this.databaseName}`,
    });

    //Create New Key if an Existing Key is NOT Found
    if (typeof existingKey === "undefined") {
      await this.adapterClient.key.gen(`ipfs-sqlite-db-${this.databaseName}`, {
        type: "rsa",
        size: 2048,
      });
    }

    //Publish Backup Path to IPNS
    let publishRequest = await this.adapterClient.name.publish(metadataPath, {
      key: `ipfs-sqlite-db-${this.databaseName}`,
    });

    return {
      message: `Published Metadata to IPNS at Name [${publishRequest.name}]`,
    };
  }

  async saveBackup(pageLinks, pageSize) {
    let backupFileSettings = {
      type: "file",
      blockSizes: pageLinks.map(() => {
        return pageSize;
      }),
    };
    this.backupFile = new UnixFS(backupFileSettings);
    const cid = await this.adapterClient.dag.put(
      dagPB.prepare({
        Data: this.backupFile.marshal(),
        Links: pageLinks.map((pageLink) => {
          return CID.parse(pageLink);
        }),
      }),
      {
        format: "dag-pb",
        hashAlg: "sha2-256",
      }
    );
    await this.adapterClient.pin.add(cid.toString());

    console.log(
      `Uploaded Backup to ${this.displayName} at CID [${cid.toString()}]`
    );

    return cid;
  }

  async connect(options) {
    try {
      console.log(`Connecting to IPFS`);
      // Wait for the IpfsAdapter Client to Initialize
      this.adapterClient = await IPFS.create();
      return true;
    } catch (err) {
      return false;
    }
  }

  async savePage(index, data, encrypted = false) {
    let uploadedPage = await this.adapterClient.add(
      {
        path: `${index}.page`,
        content: data,
      },
      {
        cidVersion: 1,
        pin: true,
      }
    );

    return {
      id: uploadedPage.cid.toString(),
      message: `Page [${index}] Saved to CID: [${uploadedPage.cid.toString()}]`,
    };
  }

  async restorePage(index, link, encrypted = false) {
    //Get Content
    let contentToRestore = await this.adapterClient.block.get(link.Hash);

    //Check if Content is encrypted
    if (encrypted === true) {
      return this.encryptionHelper.decrypt(contentToRestore);
    }

    return contentToRestore;
  }
}

module.exports = IPFSAdapter;
