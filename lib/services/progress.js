const Rx = require('@reactivex/rxjs');


function LogEvents() {
}

let startTime = null;
let tick = 0;
let counters = {};
LogEvents.ticker = Rx.Observable.interval(1000);
LogEvents.blockImport = new Rx.Subject();
LogEvents.coinsMinted = new Rx.Subject();
LogEvents.coinsSpent = new Rx.Subject();
LogEvents.transactionsAdded = new Rx.Subject();


LogEvents.ticker.subscribe((c) => {
  if(startTime === null) {
    startTime = new Date();
  }
  tick = c;
});

LogEvents.blockImport
  .bufferTime(1000)
  .filter((updates) => updates.length > 0)
  .map((blockTimeUpdates) => {
    if (blockTimeUpdates.length > 0) {
      let blockTimeUpdate = blockTimeUpdates[0];
      blockTimeUpdate.count = blockTimeUpdates.length;
      let pluck = ({time, curHeight, totalHeight, count}) => ({
        time,
        curHeight,
        totalHeight,
        count
      });
      let update = pluck(blockTimeUpdate);
      return update;
    }
  })
  .subscribe((update) => {
    /*
     *console.log(update);
     */
    let curTick = tick;
    let nowDate = new Date();
    let msFromStart = Math.floor(nowDate - startTime).toFixed(0);
    let sFromStart = (msFromStart/1000).toFixed(0);
    let downTime =  (sFromStart - curTick).toFixed(0);
    let {time, curHeight, totalHeight, count} = update;

    let blockCount = counters['blocks'] || 0;
    blockCount += count;
    counters['blocks'] = blockCount;

    let avgBlockRate = (sFromStart) / (blockCount);

    /*
     *var bar = new ProgressBar('syncing [:bar] :percent, :blocks blocks, :time s, :blockRate s/block :blocksEta hrs :blockTime :current \n', {
     *  curr: curHeight,
     *  complete: '=',
     *  incomplete: ' ',
     *  width: 30,
     *  total: totalHeight,
     *  renderThrottle: 500
     *});
     */

    let blockTime = new Date(time * 1000).toISOString();
    let blockRate = avgBlockRate.toFixed(3);
    let blocksEta = ((((totalHeight - curHeight) - tick) * avgBlockRate) / 3600).toFixed(2);
    let blocks = blockCount;

    console.log(`Syncing: ${ blocks } blocks, ${ sFromStart } s, downtime ${ downTime }s, ${ blockRate } s/block ${ blocksEta } hrs ${ blockTime } `);

  });

LogEvents.coinsMinted
  .bufferTime(1000)
  .filter((x) => x.length > 0)
  .subscribe((mintOps) => {

    let coins = mintOps.reduce((prev, cur) => {
      return prev + cur;
    }, 0);

    let coinCount = counters['coins'] || 0;
    coinCount += coins;
    counters['coins'] = coinCount;

    let coinRate = coinCount / tick;

    let rate = coinRate.toFixed(2);

    console.log(`   Minting:  ${coins} Coins, ${mintOps.length} batches/s, Rate ${rate} /s, ${tick} s `);
  });

LogEvents.coinsSpent
  .bufferTime(1000)
  .filter((x) => x.length > 0)
  .subscribe((spendOps) => {

    let spends = spendOps.reduce((prev, cur) => {
      return prev + cur;
    }, 0);

    let coinCount = counters['spends'] || 0;
    coinCount += spends;
    counters['spends'] = coinCount;

    let coinRate = coinCount / tick;

    let rate = coinRate.toFixed(2);

    console.log(`   Spending:  ${spends} Coins, ${spendOps.length} batches/s, Rate ${rate} /s, ${tick} s `);
  });

LogEvents.transactionsAdded
  .bufferTime(1000)
  .filter((x) => x.length > 0)
  .subscribe((txOps) => {

    let txs = txOps.reduce((prev, cur) => {
      return prev + cur;
    }, 0);

    let coinCount = counters['txs'] || 0;
    coinCount += txs;
    counters['txs'] = coinCount;

    let coinRate = coinCount / tick;

    let rate = coinRate.toFixed(2);

    console.log(`   Adding Tx:  ${txs} tx, ${txOps.length} batches/s, Rate ${rate} /s, ${tick} s `);
  });



module.exports = LogEvents;
