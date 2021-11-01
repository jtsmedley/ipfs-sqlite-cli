const crypto = require('crypto');
const _ = require('lodash');

class customCrypto {
	#encryptionKey = undefined;
	#encryptionIV = undefined;

	constructor({algorithm, customKey, customIV}) {
		this.#encryptionKey = customKey ? Buffer.from(customKey, 'hex') : crypto.randomBytes(32);
		this.#encryptionIV = customIV ? Buffer.from(customIV, 'hex') : crypto.randomBytes(16);

		if (_.isString(customIV) === false || _.isString(customKey) === false) {
			console.log(`Encryption Key: ${this.#encryptionKey.toString('hex')}`);
			console.log(`Encryption IV: ${this.#encryptionIV.toString('hex')}`);
		}


		this.algorithm = algorithm || 'aes-256-cbc';
	}

	encrypt (text) {
		const cipher = crypto.createCipheriv(this.algorithm, this.#encryptionKey, this.#encryptionIV);
		const encrypted = Buffer.concat([cipher.update(text), cipher.final()]);

		return {
			iv: this.#encryptionIV.toString('hex'),
			content: encrypted
		};
	}

	decrypt (hash) {
		const decipher = crypto.createDecipheriv(this.algorithm, this.#encryptionKey, Buffer.from(hash.iv || this.#encryptionIV, 'hex'));
		const decrypted = Buffer.concat([decipher.update(Buffer.from(hash.iv ? hash.content : hash, 'hex')), decipher.final()]);

		return decrypted;
	}
}

module.exports = customCrypto;