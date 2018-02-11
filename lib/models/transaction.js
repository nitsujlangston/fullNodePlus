const _ = require('underscore');
const Rx = require('@reactivex/rxjs');
const LogEvents = require('../services/progress');
const mongoose = require('mongoose');
const Schema = mongoose.Schema;
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

let partition = (array, n) => {
  return array.length ? [array.splice(0, n)].concat(partition(array, n)) : [];
};


TransactionSchema.mintAndWrite = async function (params) {
  let mintOps = await Transaction.mintCoins(params);
  if(mintOps.length > 0) {
    LogEvents.coinsMinted.next(mintOps.length);
    mintOps = partition(mintOps, 500);
    mintOps = mintOps.map((mintBatch) => Coin.collection.bulkWrite(mintBatch, {ordered: false}));
    let allPromise = Promise.all(mintOps);
    return allPromise;
  }
};

TransactionSchema.spendAndWrite = async function(params) {
 let spendOps = await Transaction.spendCoins(params);
  if (spendOps.length > 0){
    LogEvents.coinsSpent.next(spendOps.length);
    spendOps = partition(spendOps, 500);
    spendOps = spendOps.map((spendBatch) => Coin.collection.bulkWrite(spendBatch, {ordered: false}));
    let allPromise = Promise.all(spendOps);
    return allPromise;
  }
};


TransactionSchema.txAndWrite = async function(params) {
  let txOps = await Transaction.addTransactions(params);
  if (txOps.length > 0 ){
    LogEvents.transactionsAdded.next(txOps.length);
    txOps = partition(txOps, 500);
    txOps = txOps.map((txBatch) => Transaction.collection.bulkWrite(txBatch, {ordered: false}));
    let allPromise = Promise.all(txOps);
    return allPromise;
  }
};

TransactionSchema.mintStart = new Rx.Subject();
TransactionSchema.spendStart = new Rx.Subject();
TransactionSchema.txStart = new Rx.Subject();

TransactionSchema.mintStart
  .flatMap((params) => Rx.Observable.of({params, mints: Promise.resolve(TransactionSchema.mintAndWrite(params))}))
  .subscribe(({params}) => TransactionSchema.spendStart.next(params));

TransactionSchema.spendStart
  .flatMap((params) => Rx.Observable.of({params, mints: Promise.resolve(TransactionSchema.spendAndWrite(params))}))
  .subscribe(({params}) => TransactionSchema.txStart.next(params));

TransactionSchema.txStart
  .flatMap((params) => Rx.Observable.of({params, mints: Promise.resolve(TransactionSchema.txAndWrite(params))}))
  .subscribe(() => {});



TransactionSchema.statics.batchImport = async function (params) {
  return TransactionSchema.mintStart.next(params);
};

TransactionSchema.statics.addTransactions = async function(params){
  let { blockHash, blockTime, blockTimeNormalized, height, network, txs } = params;
  return new Promise(async (resolve) => {
    let txids = txs.map((tx) => tx._hash);
    let allWallets = [];
    let mintBatch = await Coin.collection.aggregate([
      {
        $match: { mintTxid: {$in: txids} }
      },
      { $unwind: '$wallets' },
      { $group: { _id: '$mintTxid', wallets: { $addToSet: '$wallets' } } }
    ]).toArray();
    let spentBatch = await Coin.collection.aggregate([
      {
        $match: { spentTxid: { $in: txids } }
      },
      { $unwind: '$wallets' },
      { $group: { _id: '$spentTxid', wallets: { $addToSet: '$wallets' } } }
    ]).toArray();

    allWallets.push(...mintBatch);
    allWallets.push(...spentBatch);
    let allTxWallets = _.groupBy(allWallets, (wallet) => wallet._id);
    let txOps = [];
    for (let tx of txs) {
      let txHash = tx._hash;
      let txWallets = allTxWallets[txHash] || [];
      let wallets = {};
      for (let wallet of txWallets ){
        for (let walletMatch of wallet.wallets){
          let walletHex = walletMatch.toHexString();
          if (!wallets[walletHex]) {
            wallets[walletHex] = walletMatch;
          }
        }
      }

      txOps.push({
        updateOne: {
          filter: {
            txid: txHash
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
              wallets: allWallets
            }
          },
          upsert: true,
          forceServerObjectId: true
        }
      });
    }
    resolve(txOps);
  });
};

TransactionSchema.statics.mintCoins = async function (params) {
  let {height, network, txs} = params;
  let mintOps = [];
  let mintOpPromises = [];
  for (var tx of txs) {
    tx._hash = tx.hash;
    let txHash = tx._hash;
    for (let [index, output] of tx.outputs.entries()) {
      let address;
      let mintOpPromise = new Promise((resolve) => {
        let scriptBuffer = output.script && output.script.toBuffer();
        try {
          address = output.script.toAddress(network).toString();
          if (address === 'false' && output.script.classify() === 'Pay to public key') {
            let hash = bitcore.crypto.Hash.sha256ripemd160(output.script.chunks[0].buf);
            address = bitcore.Address(hash, network).toString();
          }
        } catch (e) {
          address = 'noAddress';
        }
        let op = {
          updateOne: {
            filter: {
              mintTxid: txHash,
              mintIndex: index,
              spentHeight: {
                $lt: 0
              }
            },
            update: {
              $set: {
                network: network,
                mintHeight: height,
                coinbase: tx.isCoinbase(),
                value: output.satoshis,
                address: address,
                script: scriptBuffer,
                spentHeight: -2,
                wallets: []
              }
            },
            upsert: true,
            forceServerObjectId: true
          }
        };
        resolve(op);
      });
      mintOpPromises.push(mintOpPromise);
    }
  }
  mintOps = await Promise.all(mintOpPromises);
  let mintOpsAddresses = mintOps.map((mintOp) => mintOp.updateOne.update.$set.address);
  let wallets = await WalletAddress.collection.find({ address: { $in: mintOpsAddresses } }, { batchSize: 500}).toArray();
  if (wallets.length){
    mintOps = mintOps.map((mintOp) => {
      mintOp.updateOne.update.$set.wallets = wallets
        .filter((wallet) => wallet.address === mintOp.updateOne.update.$set.address)
        .map((wallet) => wallet.wallet);
      return mintOp;
    });
  }
  return mintOps;
};

TransactionSchema.statics.spendCoins = function(params) {
  let {height, txs} = params;
  let spendOps = [];
  for (let tx of txs) {
    let txHash = tx._hash;
    if (tx.isCoinbase()) {
      continue;
    }
    for (let input of tx.inputs) {
      input = input.toObject();
      spendOps.push({
        updateOne: {
          filter: {
            mintTxid: input.prevTxId,
            mintIndex: input.outputIndex,
            spentHeight: { $lt: 0 }
          },
          update: {
            $set: {
              spentTxid: txHash,
              spentHeight: height
            }
          }
        }
      });
    }
  }
  return spendOps;
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
