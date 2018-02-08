const ProgressBar = require('progress');
const Rx = require('@reactivex/rxjs');


function LogEvents() {
}

let tick = 0;
let counters = {};
LogEvents.ticker = Rx.Observable.interval(1000);
LogEvents.blockImport = new Rx.Subject();


LogEvents.ticker.subscribe((c) => {
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
    let {time, curHeight, totalHeight, count} = update;

    let blockCount = counters['blocks'] || 0;
    blockCount += count;
    counters['blocks'] = blockCount;

    let avgBlockRate = tick/blockCount;

    var bar = new ProgressBar('syncing [:bar] :percent, :blocks blocks, :time s, :blockRate s/block :blocksEta hrs :blockTime :current', {
      curr: curHeight,
      complete: '=',
      incomplete: ' ',
      width: 30,
      total: totalHeight,
      renderThrottle: 500
    });

    bar.tick({
      blockTime: new Date(time * 1000).toISOString(),
      blockRate: avgBlockRate.toFixed(2),
      blocksEta: ((((totalHeight - curHeight) - tick) * avgBlockRate) / 3600).toFixed(2),
      blocks: blockCount,
      time: tick
    });
  });
module.exports = LogEvents;
