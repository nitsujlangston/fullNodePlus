const Rx = require('@reactivex/rxjs');
const mongoose = require('mongoose');
const Schema = mongoose.Schema;
const async = require('async');
const bitcore = require('bitcore-lib');

const Coin = mongoose.model('Coin');
const WalletAddress = mongoose.model('WalletAddress');

const TransactionSchema = new Schema({
  txid: String,
  network: String,
  chain: String,
  blockHeight: Number,
  blockHash: String,
  blockTime: Date,
  blockTimeNormalized: Date,
  coinbase: Boolean,
  fee: Number,
  size: Number,
  locktime: Number,
  wallets: { type: [Schema.Types.ObjectId] },
});

TransactionSchema.index({txid: 1});
TransactionSchema.index({blockHeight: 1});
TransactionSchema.index({blockHash: 1});
TransactionSchema.index({blockTimeNormalized: 1});
TransactionSchema.index({wallets: 1}, {sparse: true});

TransactionSchema.statics.batchImport = async function (params) {
  return new Promise(async (resolve) => {
    let partition = (array, n) => {
      return array.length ? [array.splice(0, n)].concat(partition(array, n)) : [];
    };

    Transaction.mintCoins(params)
    .bufferCount(10000)
    .flatMap((mintBatch) => Coin.collection.bulkWrite(mintBatch, {ordered: false}))
    .subscribe((mintOps) => {
      //console.log('Minted ', mintOps);
    });
  
    
    Transaction.spendCoins(params)
      .bufferCount(10000)
      .flatMap((spendBatch) => Coin.collection.bulkWrite(spendBatch, {ordered: false}))
      .subscribe((coinOps) => {
        //console.log('Spent', coinOps);
      });

    let txOps = await Transaction.addTransactions(params);
    txOps = partition(txOps, 100);
    txOps = txOps.map((txBatch) => Transaction.collection.bulkWrite(txBatch, {ordered: false}));
    await Promise.all(txOps);
    resolve();
  });
};

TransactionSchema.statics.addTransactions = function(params){
  let { blockHash, blockTime, blockTimeNormalized, height, network, txs } = params;
  return new Promise((resolve, reject) => {
    async.mapLimit(txs, 100, function(tx, cb){
      Coin.aggregate([
        {
          $match: {
            $or: [
              { spentTxid: tx.hash },
              { mintTxid: tx.hash }
            ]
          }
        },
        { $unwind: '$wallets' },
        { $group: { _id: null, wallets: { $addToSet: '$wallets' } } },
        { $project: { _id: false } }
      ]).exec(function(err, wallets){
        if (err){
          return cb(err);
        }
        if (wallets.length) {
          wallets = wallets[0].wallets;
        }
        let op = {
          updateOne: {
            filter: {
              txid: tx.hash
            },
            update: {
              $set: {
                network: network,
                blockHeight: height,
                blockHash: blockHash,
                blockTime: blockTime,
                blockTimeNormalized: blockTimeNormalized,
                coinbase: tx.isCoinbase(),
                size: tx.toBuffer().length,
                locktime: tx.nLockTime,
                wallets: wallets
              }
            },
            upsert: true,
            forceServerObjectId: true
          }
        };
        cb(null, op);
      });
    }, (err, txOps) => {
      if (err){
        reject(err);
      }
      resolve(txOps);
    });
  });
};

TransactionSchema.statics.mintCoins = function (params) {
  let { height, network, txs } = params;
  let txObs = Rx.Observable.from(txs);

  let findWallet = (address) => { 
    WalletAddress.collection.find({address})
      .map((wallet) => wallet.wallet)
      .toArray();
    
  };

  let updateWallet = (tx, index, address, satoshis, scriptBuffer, wallets ) => {
    
    return {
      updateOne: {
        filter: {
          mintTxid: tx.hash,
          mintIndex: index,
          spentHeight: { $lt: 0 }
        },
        update: {
          $set: {
            network: network,
            mintHeight: height,
            coinbase: tx.isCoinbase(),
            value: satoshis,
            address: address,
            script: scriptBuffer,
            spentHeight: -2,
            wallets: wallets
          }
        },
        upsert: true,
        forceServerObjectId: true
      }
    };
  };
  
  let updateData = txObs.mergeMap((tx) => {
      return tx.outputs.map((output, index) => {
        let satoshis = output.satoshis;
        let scriptBuffer = output.script && output.script.toBuffer();
        let address;
        try {
          address = output.script.toAddress(network).toString();
          if (address === 'false' && output.script.classify() === 'Pay to public key') {
            let hash = bitcore.crypto.Hash.sha256ripemd160(output.script.chunks[0].buf);
            address = bitcore.Address(hash, network).toString();
          }
        } catch (e) {
          address = 'noAddress';
        }
        return {tx, address, scriptBuffer, index, satoshis};
      });
  });

  let findStream = updateData.bufferCount(200)
  .map((updatesArr) => {
    return updatesArr.map((update) => {
      let wallet = findWallet(update.address);
      let walletData = Object.assign({}, update, {wallet});
      return walletData;
    });
  }); 

  let mintOps = [];
  let updateStream = findStream.mergeMap((bulkUpdates) => {
    return bulkUpdates.map((walletData)=> {
      let {tx, index, address, satoshis, scriptBuffer, wallets } = walletData;
      return updateWallet(tx, index, address, satoshis, scriptBuffer, wallets);
    });
  });
  return updateStream;
};

TransactionSchema.statics.spendCoins = function (params) {
  let { height, txs } = params;
  return Rx.Observable.from(txs)
  .filter((tx) => tx.isCoinbase())
  .mergeMap((tx) => {
    return tx.inputs.map((input) => {
      input = input.toObject();
      return {
        updateOne: {
          filter: {
            mintTxid: input.prevTxId,
            mintIndex: input.outputIndex,
            spentHeight: { $lt: 0 }
          },
          update: {
            $set: {
              spentTxid: tx.hash,
              spentHeight: height
            }
          }
        }
      }
    });
  });
};

TransactionSchema.statics.getTransactions = function (params) {
  let query = params.query;
  return this.collection.aggregate([
    { $match: query },
    {
      $lookup:
        {
          from: 'coins',
          localField: 'txid',
          foreignField: 'spentTxid',
          as: 'inputs'
        }
    },
    {
      $lookup:
        {
          from: 'coins',
          localField: 'txid',
          foreignField: 'mintTxid',
          as: 'outputs'
        }
    }
  ]);
};

TransactionSchema.statics._apiTransform = function(tx, options) {
  let transform = {
    txid: tx.txid,
    network: tx.network,
    blockHeight: tx.blockHeight,
    blockHash: tx.blockHash,
    blockTime: tx.blockTime,
    blockTimeNormalized: tx.blockTimeNormalized,
    coinbase: tx.coinbase,
    fee: tx.fee,
  };
  if(options && options.object) {
    return transform;
  }
  return JSON.stringify(transform);
};

var Transaction = module.exports = mongoose.model('Transaction', TransactionSchema);
