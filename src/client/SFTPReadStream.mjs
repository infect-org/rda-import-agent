import { Readable } from 'stream';


/**
 * this class is required in order to get a stream that has the standard stream interface. the
 * invalidCrapSFTPStream passed to the class is a piece of shit received from the sftp library. it
 * needs to be wrapped. Please, don't rely on the crappy docs for this sftp client, its worthless.
 * yes, I'm angry are the authors of that library wasting my time.
 */


export default class SFTPReadStream extends Readable {

    constructor(invalidCrapSFTPStream) {
        super({
            read: (size) => {
                return this.privateRead(size);
            },
            destroy: (err, callback) => {
                return this.privateDestroy(err, callback);
            },
        });

        invalidCrapSFTPStream.resume();

        this.invalidCrapSFTPStream = invalidCrapSFTPStream;
    }



    privateRead() {
        //this.invalidCrapSFTPStream.resume();

        const chunk = this.invalidCrapSFTPStream.read();
        console.log(chunk);

        if (chunk) {
            console.log('got a chunk', chunk.length);
            const stop = !this.push(chunk);


            console.log('stop', stop);
            if (stop) {
                //this.invalidCrapSFTPStream.pause();
            } else {
                this.privateRead();
            }
        } else {
            console.log(`we're done`);
            // the crap stream is not able to indicate when its finished. so i'm assuming it's done
            // if no data is returned anymore.
            this.push(null);
        }
    }


    privateDestroy(err, callback) {
        return this.invalidCrapSFTPStream.destroy(err, callback);
    }
}
