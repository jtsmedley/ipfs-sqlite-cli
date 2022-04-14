const BaseAdapter = require("./baseAdapter");
const _ = require("lodash");
const IPFS = require("ipfs-http-client");

class IPFSAdapter extends BaseAdapter {
  constructor(options) {
    super(options);

    this.displayName = "IPFS";
  }

  async connect(options) {
    try {
      // Wait for the IpfsAdapter Client to Initialize
      this.adapterClient = await IPFS.create();
    } catch (err) {
      console.error(err.message);
    }
  }

  async getJSON(backupCID) {
    const chunks = [];
    for await (const chunk of this.adapterClient.cat(backupCID)) {
      chunks.push(chunk);
    }
    return JSON.parse(Buffer.concat(chunks).toString());
  }

  async saveJSON(dataToSave) {
    const dataCID = (
      await this.adapterClient.add(JSON.stringify(dataToSave), {
        cidVersion: 1,
      })
    ).cid;

    console.log(
      `Uploaded JSON to ${this.displayName} at CID [${dataCID.toString()}]`
    );

    return dataCID.toString();
  }

  async getPage(index, link, encrypted = false) {
    //Get Content
    let contentToRestore = await this.adapterClient.block.get(link);

    //Check if Content is encrypted and decrypt
    if (encrypted === true) {
      return this.encryptionHelper.decrypt(contentToRestore);
    }

    return contentToRestore;
  }

  async savePage(index, data, encrypted = false) {
    let uploadedPage = await this.adapterClient.add(
      {
        path: `${index}.page`,
        content: data,
      },
      {
        cidVersion: 1,
      }
    );

    return {
      id: uploadedPage.cid.toString(),
      message: `Page [${index}] Saved to CID: [${uploadedPage.cid.toString()}]`,
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
}

module.exports = IPFSAdapter;