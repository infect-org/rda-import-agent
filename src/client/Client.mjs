import crypto from 'crypto';


export default class Client {


    async getCurrentImportHash(file) {
        const info = await this.getInfo(file);

        return crypto.createHash('sha256')
            .update(`${info.siz}//${info.modificationDate.getTime()}`)
            .digest('hex');
    }
}
