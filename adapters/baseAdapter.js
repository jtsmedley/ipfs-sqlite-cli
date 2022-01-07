class BaseAdapter {
  constructor(options) {
    if (typeof options.databaseName === "undefined") {
      throw new Error("options.databaseName is required.");
    }

    this.databaseName = options.databaseName;
  }

  async getMetadata() {
    throw new Error(`Get metadata is not implemented.`);
  }

  async saveMetadata() {
    throw new Error(`Save metadata is not implemented.`);
  }

  async publishMetadata() {
    throw new Error(`Publish metadata is not implemented.`);
  }

  async saveBackup() {
    throw new Error(`Save backup is not implemented.`);
  }

  async connect() {
    throw new Error(`Connect is not implemented.`);
  }

  async savePage(index, data) {
    throw new Error(`savePage is not implemented`);
  }

  async restorePage(index) {
    throw new Error("restorePage is not implemented");
  }
}

module.exports = BaseAdapter;
