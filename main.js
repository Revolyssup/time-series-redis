const { argv } = require('process');
//Make sure you have redis-server listening on default port
const redis=require('redis')
const conn=redis.createClient();

//Based on command line arguments different methods will be called. One using hash and other using string.
if(process.argv.length<3){
    console.log("ERROR: You need to specify a data type!");
    console.log("$ node main.js [string|hash]");
    process.exit(1);
}
const dataType=argv[2];
if(dataType!=='string' && dataType!=='hash'){
    console.log("ERROR: You need to specify correct data type!");
    console.log("$ node main.js [string|hash]");
    process.exit(1);
}

//Callback function to display all results for some granularity.
const display=(granularityName,results)=>{
    console.log("Results from " + granularityName + ":");
    console.log("Timestamp \t| Value");
    console.log("--------------- | ------");
    for(var i = 0 ; i < results.length; i++){
        console.log('\t' + results[i].timeStamp + '\t| ' + results[i].value);
    }
    console.log("Done ;)");
}
//clearing database
conn.flushall();

const TimeSeries=require('./src/time-series-'+dataType);

//example - time-series mapping of some item1Purchases(Say in a shopping application)
const item1Purchases=new TimeSeries(conn,'purchases:item1');

const beginTimestamp=0;
item1Purchases.insert(beginTimestamp);
item1Purchases.insert(beginTimestamp+1);
item1Purchases.insert(beginTimestamp+4);
item1Purchases.insert(beginTimestamp+320);
item1Purchases.insert(beginTimestamp+32);
item1Purchases.insert(beginTimestamp+69420);

item1Purchases.fetch('1sec',beginTimestamp,beginTimestamp+32,display);
