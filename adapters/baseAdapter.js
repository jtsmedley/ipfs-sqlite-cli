class BaseAdapter {
  constructor(options) {
    if (typeof options.databaseName === "undefined") {
      throw new Error("options.databaseName is required.");
    }

    this.databaseName = options.databaseName;
    this.encryptionHelper = options.encryptionHelper;
  }

  async connect() {
    throw new Error(`Connect is not implemented.`);
  }

  async getJSON() {
    throw new Error(`Get JSON is not implemented.`);
  }

  async saveJSON() {
    throw new Error(`Save JSON is not implemented.`);
  }

  async getPage() {
    throw new Error("restorePage is not implemented");
  }

  async savePage() {
    throw new Error(`savePage is not implemented`);
  }

  async publishMetadata() {
    throw new Error(`Publish metadata is not implemented.`);
  }
}

module.exports = BaseAdapter;
