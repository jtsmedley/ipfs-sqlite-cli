const crypto = require('crypto');

class customCrypto {
	constructor({algorithm, secretKey}) {
		if (typeof secretKey === 'undefined') {
			throw new Error(`secretKey was not defined in the crypto.js`);
		} else {
			this.secretKey = secretKey;
		}

		this.algorithm = algorithm || 'aes-256-ctr';
	}

	encrypt (text) {
		let iv = crypto.randomBytes(16);
		const cipher = crypto.createCipheriv(this.algorithm, this.secretKey, iv);
		const encrypted = Buffer.concat([cipher.update(text), cipher.final()]);

		return {
			iv: iv.toString('hex'),
			content: encrypted
		};
	}

	decrypt (hash) {
		const decipher = crypto.createDecipheriv(this.algorithm, this.secretKey, Buffer.from(hash.iv, 'hex'));
		const decrypted = Buffer.concat([decipher.update(Buffer.from(hash.content, 'hex')), decipher.final()]);

		return decrypted;
	}
}

module.exports = customCrypto;