class TimeSeries{
    //Initializing
    constructor(conn,namespace){ // convention: namespace:timeStampInSeconds:granularity
        this.conn=conn;
        this.namespace=namespace;
        this.units={
            second:1,
            minute:60,
            hour:3600,
            day:24*3600
        }
        this.granularities={
            '1sec':{
                name:'1sec',ttl:this.units.hour*2,duration:this.units.second
            },
            '1min':{
                name:'1min',ttl:this.units.day*7,duration:this.units.minute
            },
            '1hr':{
                name:'1hr',ttl:this.units.day*60,duration:this.units.day
            },
            '1day':{
                name:'1day',ttl:null,duration:this.units.day
            }
        }
    }

/*************************************************************************** */
// insert registers an event that happened at a given timestamp in
// multiple granularities:
    insert(timeStampInSeconds){
        for(let gran in this.granularities){
            const granularity=this.granularities[gran];
            const key=this._generateKey(granularity,timeStampInSeconds); // convention: namespace:timeStampInSeconds:granularity
            this.conn.incr(key);
            if(granularity.ttl!==null){
                this.conn.expire(key,granularity.ttl);
            }

        }
    }


    //Generates Key.(sec/min/hr/day 4keys at 4 granularities of given timestamp)
//     _generateKey, which receives a granularity object and
// a timestamp. It is a convention in JavaScript to use an underscore at the
// beginning of private method names.
    _generateKey(granularity,timeStampInSeconds){
        let roundedOff=this._getRoundedTimestamp(timeStampInSeconds,granularity.duration); 
        return [this.namespace,roundedOff,timeStampInSeconds].join(":");
    }



//     _getRoundedTimestamp. It returns a normalized timestamp
// by granularity duration. This new timestamp will be used to create the key
// name. For instance, all insertions that happen in the first minute of an hour
// are stored in a key like "namespace:1min:0", all inserts from the second minute
// are stored in "namespace:1min:60", and so on.
     _getRoundedTimestamp(timeStampInSeconds,precision){
        return Math.floor(timeStampInSeconds/precision)*precision;
    }
/*************************************************************************** */

/**
 * 
 * @param {String} granularityName 
 * @param {Number} startTimeInSeconds 
 * @param {Number} endTimeInSeconds 
 * @param {Function} callback 
 */
//Accepts granularityName,start timestamp,end timestamp,callback function
    fetch(granularityName,startTimeInSeconds,endTimeInSeconds,callback){//callback granularity as first arg and the array of timestamp results as second
        const granularity=this.granularities[granularityName];
        //we want time stamps at granularity.duration intervals between these timeStamps
        const begin=this._getRoundedTimestamp(startTimeInSeconds,granularity.duration);
        const end=this._getRoundedTimestamp(endTimeInSeconds,granularity.duration)
        let keys=[];

        for(let i=begin;i<=end;i+=granularity.duration){
            keys.push(this._generateKey(granularity,i));
        }

        //We have got the keys,now time to query the redis for these keys
        this.conn.mget(keys,(err,replies)=>{
            let results=[];
            for(let i=0;i<replies.length;i++){
                const timeStamp=begin+i*granularity.duration;
                // Convert a reply value to an integer value. If this is not possible, fall back to zero.
                const val=parseInt(replies[i],10) || 0;
                results.push({timeStamp:timeStamp,value:val});
            }
            callback(granularityName,results);
        })
    }
}

module.exports=TimeSeries;